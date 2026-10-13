/*
 * Copyright 2024-2025, Ben Sherman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.boost.cleanup

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.transform.TupleConstructor
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Session
import nextflow.boost.BoostConfig
import nextflow.dag.DAG
import nextflow.file.FileHelper
import nextflow.processor.ErrorStrategy
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.TaskEvent
import nextflow.script.ProcessConfigV1
import nextflow.script.ProcessConfigV2
import nextflow.script.params.FileOutParam
import nextflow.script.params.OutParam
import nextflow.script.params.TupleOutParam
import nextflow.util.Threads
/**
 * Delete temporary files once they are no longer needed.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class CleanupObserver implements TraceObserverV2 {

    private Session session

    private Map<String,ProcessState> processes = [:]

    private Map<Path,PathState> paths = [:]

    // tracked separately from paths, since a consumer task can be created
    // before the task that produced its input files is marked as completed
    private Map<Path,Set<String>> consumerTasks = [:]

    private Set<Path> publishedOutputs = []

    private Set<Path> symlinkedOutputs = []

    private LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>()

    private long delayMillis = 5000

    private boolean cleanupFailedTasks

    private CountDownLatch done = new CountDownLatch(1)

    private Thread cleanupThread

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        final config = new BoostConfig(session.config.boost as Map ?: Collections.emptyMap())
        this.delayMillis = config.cleanupInterval.toMillis()
        this.cleanupFailedTasks = config.cleanupFailedTasks

        if( session.resumeMode )
            log.warn "This experimental version of automatic cleanup does not work with resume -- deleted tasks will be re-executed"
    }

    /**
     * When the workflow begins, determine the consumers of each process
     * in the DAG.
     */
    @Override
    void onFlowBegin() {
        final Map<String,List<ProcessOutput>> outputsByProcess = [:]
        for( def vertex : session.dag.vertices ) {
            final process = vertex.process
            if( process == null )
                continue

            outputsByProcess[process.name] = processOutputs(process).collect { param ->
                new ProcessOutput(param.getOutChannel(), hasForwardedInputs(param))
            }

            // add event listener for process close
            process.operator.addDataflowEventListener(new DataflowEventAdapter() {
                @Override
                void afterStop(final DataflowProcessor processor) {
                    onProcessClose(process)
                }
            })
        }

        this.processes = getProcessStates(session.dag, outputsByProcess, session.outputs.values())

        // launch thread to handle workflow events and cleanup
        this.cleanupThread = Threads.start('Task cleanup') {
            try {
                while( !done.await(delayMillis, TimeUnit.MILLISECONDS) )
                    handleEvents()
            }
            catch( Throwable e ) {
                log.debug "Unexpected error in cleanup observer", e
            }
        }
    }

    /**
     * Determine the consumers of each process output, and whether
     * it might be published, by traversing the DAG.
     *
     * @param dag
     * @param outputsByProcess
     * @param publishedChannels
     */
    @PackageScope
    static Map<String,ProcessState> getProcessStates(DAG dag, Map<String,List<ProcessOutput>> outputsByProcess, Collection publishedChannels) {
        final Map<String,ProcessState> result = [:]

        // prepare lookup tables
        final Map<DAG.Vertex,List<DAG.Vertex>> successors = [:]
        final Map<DataflowWriteChannel,List<DAG.Edge>> edgeLookup = [:]
        for( def edge : dag.edges ) {
            // lookup vertex -> successor vertices
            if( edge.from !in successors )
                successors[edge.from] = []
            successors[edge.from] << edge.to

            // lookup channel -> edges
            if( edge.channel !instanceof DataflowWriteChannel )
                continue
            final ch = (DataflowWriteChannel)edge.channel
            if( ch !in edgeLookup )
                edgeLookup[ch] = []
            edgeLookup[ch] << edge
        }

        // construct process lookup
        final Set<Tuple2<String,Integer>> withForwardedInputs = []

        for( def vertex : dag.vertices ) {
            // skip nodes that are not processes
            final process = vertex.process
            if( process == null )
                continue

            // get the set of consuming processes for each process output
            final outputs = outputsByProcess[process.name]
            final List<Set<String>> consumers = outputs.collect { [] as Set }
            final List<Boolean> publishable = outputs.collect { false }

            for( int i = 0; i < outputs.size(); i++ ) {
                final ch = outputs[i].channel

                // get the set of consuming processes
                final queue = edgeLookup.getOrDefault(ch, []).collect { edge -> edge.to }
                while( !queue.isEmpty() ) {
                    final w = queue.remove(0)
                    // skip terminal edges
                    if( !w )
                        continue
                    // add process nodes to the list of consumers
                    if( w.process != null )
                        consumers[i] << w.process.name
                    // add operator nodes to the queue to keep searching
                    else if( w in successors )
                        queue.addAll( successors[w] )
                }

                // determine whether the output channel might be published
                final publisherQueue = [ ch ]
                while( !publisherQueue.isEmpty() ) {
                    final ch0 = publisherQueue.remove(0)
                    if( ch0 in publishedChannels ) {
                        log.trace "Process output `${process.name}/${i+1}` might be published"
                        publishable[i] = true
                        break
                    }
                    // add operator output channels to the queue to keep searching
                    edgeLookup.getOrDefault(ch0, []).stream()
                        .map(edge -> edge.to)
                        .filter(w -> w != null && w.process == null)
                        .flatMap(w -> w.operators.stream())
                        .map(op -> op.getOutputs())
                        .forEach(publisherQueue::addAll)
                }

                // check if output may forward input files
                if( outputs[i].forwardsInputs )
                    withForwardedInputs << new Tuple2(process.name, i)
            }

            result[process.name] = new ProcessState(consumers, publishable)
        }

        // if a process B receives files from process A and forwards them
        // as outputs to process C, then process C must be marked as a
        // consumer of process A even if there is no direct dependency
        result.each { processName, processState ->
            // append indirect consumers (and publishable) for each process output
            for( int i = 0; i < processState.consumers.size(); i++ ) {
                final consumers = processState.consumers[i]
                for( final pair : withForwardedInputs ) {
                    final consumerName = pair.first
                    final j = pair.second
                    if( consumerName in consumers ) {
                        log.trace "Process output `${consumerName}/${j+1}` may forward output files, marking its consumers as indirect consumers of `${processName}/${i+1}`"
                        final consumerState = result[consumerName]
                        consumers.addAll(consumerState.consumers[j])
                        processState.publishable[i] |= consumerState.publishable[j]
                    }
                }

                log.trace "Process output `${processName}/${i+1}` is used by the following processes: ${processState.consumers[i]}"
            }
        }

        return result
    }

    private static List<OutParam> processOutputs(TaskProcessor process) {
        final config = process.config
        if( config instanceof ProcessConfigV2 )
            return config.getOutputs().getParams() as List<OutParam>
        if( config instanceof ProcessConfigV1 )
            return config.getOutputs()
        return []
    }

    /**
     * Determine whether a process output may forward input files
     * as outputs.
     *
     * TODO: check if a non-glob file output matches a file from this process output
     *
     * @param param
     */
    private static boolean hasForwardedInputs(OutParam param) {
        if( param instanceof FileOutParam && param.includeInputs )
            return true
        if( param instanceof TupleOutParam )
            return param.inner.any( p -> p instanceof FileOutParam && p.includeInputs )
        return false
    }

    /**
     * Process workflow events and cleanup.
     */
    @PackageScope
    void handleEvents() {
        // remove all events from the queue
        final List<Event> events = []
        eventQueue.drainTo(events)

        log.trace "Processing ${events.size()} workflow events"

        // process each event
        boolean cleanup = false
        for( final event : events ) {
            if( event instanceof Event.TaskPending ) {
                onTaskPending0(event.task)
            }
            else if( event instanceof Event.TaskCompleted ) {
                cleanup |= onTaskComplete0(event.task)
            }
            else if( event instanceof Event.FilePublished ) {
                onFilePublish0(event.path, event.target)
            }
            else if( event instanceof Event.ProcessClosed ) {
                onProcessClose0(event.process)
                cleanup = true
            }
        }

        // scan for cleanup if needed
        if( cleanup ) {
            log.trace "Scanning for cleanup"
            cleanup0()
        }
    }

    /**
     * When a task is created, mark it as a consumer of its input files.
     *
     * @param event
     */
    @Override
    void onTaskPending(TaskEvent event) {
        eventQueue.put(new Event.TaskPending(event.handler.task))
    }

    private void onTaskPending0(TaskRun task) {
        // mark task as consumer of each input file
        for( Path path : inputPaths(task) )
            consumerTasks.computeIfAbsent(path, k -> new HashSet<String>()).add(taskKey(task))
    }

    /**
     * When a task is completed, track the task and its output files
     * for automatic cleanup.
     *
     * @param event
     */
    @Override
    void onTaskComplete(TaskEvent event) {
        eventQueue.put(new Event.TaskCompleted(event.handler.task))
    }

    private boolean onTaskComplete0(TaskRun task) {
        // remove task as consumer of each input file, unless it will be
        // retried, in which case the retry will remove it when it completes
        if( task.errorAction != ErrorStrategy.RETRY ) {
            for( Path path : inputPaths(task) ) {
                final tasks = consumerTasks[path]
                if( tasks != null && tasks.remove(taskKey(task)) && tasks.isEmpty() )
                    consumerTasks.remove(path)
            }
        }

        // skip failed task without scanning for cleanup
        if( task.failed || !task.isSuccess() ) {
            // the retry runs in a new work directory, so the failed attempt is no longer needed
            if( cleanupFailedTasks && task.errorAction == ErrorStrategy.RETRY && task.workDir != null )
                deleteWorkDir(task)
            return false
        }

        // query task output files
        final outputs = task
            .getOutputsByType(FileOutParam)
            .values()
            .flatten() as Set<Path>

        // get process consumers for each file
        final processConsumersMap = getProcessConsumers(task, outputs)

        // get publishable outputs
        final publishableOutputs = getPublishableOutputs(task, outputs)

        log.trace "[${task.name}] the following files might be published: ${publishableOutputs*.toUriString()}"

        // remove any outputs that have already been published
        publishableOutputs.removeIf(path -> publishedOutputs.remove(path))

        // add each output file to the path state map
        for( Path path : outputs ) {
            final pathState = new PathState(task.name, processConsumersMap[path])
            if( path !in publishableOutputs )
                pathState.published = true

            log.trace "File ${path} might be used by the following processes: ${processConsumersMap[path]}"
            paths[path] = pathState
        }

        return true
    }

    /**
     * Get the input files of a task, including both the original path,
     * which matches the output path of the upstream task, and the real
     * path (symlinks resolved) that is staged into the task directory.
     *
     * @param task
     */
    private static Set<Path> inputPaths(TaskRun task) {
        final Set<Path> result = []
        for( final holder : task.inputFiles ) {
            result << holder.storePath
            if( holder.sourcePath != null )
                result << holder.sourcePath
        }
        return result
    }

    /**
     * Get a key that identifies a task across retries.
     *
     * @param task
     */
    private static String taskKey(TaskRun task) {
        "${task.processor.name}/${task.index}"
    }

    /**
     * Determine the set of process consumers for each output file
     * of a task based on the output channels that emitted the file.
     *
     * @param task
     * @param outputs
     */
    private Map<Path,Set<String>> getProcessConsumers(TaskRun task, Set<Path> outputs) {
        final Map<Path,Set<String>> result = outputs.inject([:]) { acc, path ->
            acc[path] = [] as Set
            acc
        }
        final processState = processes[task.processor.name]

        for( final entry : task.getOutputsByType(FileOutParam) ) {
            final param = entry.key
            final consumers = processState.consumers[param.index]
            final value = entry.value
            if( value instanceof Path )
                result[value].addAll(consumers)
            else if( value instanceof Collection<Path> )
                value.each { el -> result[el].addAll(consumers) }
            else
                throw new IllegalArgumentException("Unknown output file object [${value.class.name}]: ${value}")
        }

        return result
    }

    /**
     * Determine the set of task outputs that might be published
     * based on the output channels that emitted each file.
     *
     * @param task
     * @param outputs
     */
    protected Set<Path> getPublishableOutputs(TaskRun task, Set<Path> outputs) {
        final Set<Path> result = []
        final processState = processes[task.processor.name]

        for( final entry : task.getOutputsByType(FileOutParam) ) {
            final param = entry.key
            if( !processState.publishable[param.index] )
                continue
            final value = entry.value
            if( value instanceof Path )
                result.add(value)
            else if( value instanceof Collection<Path> )
                result.addAll(value)
            else
                throw new IllegalArgumentException("Unknown output file object [${value.class.name}]: ${value}")
        }

        return result
    }

    /**
     * When a file is published, mark it as published and delete
     * it if it is no longer needed.
     *
     * If the file is published before the corresponding task is
     * marked as completed, save it for later.
     *
     * @param event
     */
    @Override
    void onFilePublish(FilePublishEvent event) {
        eventQueue.put(new Event.FilePublished(event.source, event.target))
    }

    private void onFilePublish0(Path path, Path target) {
        // deleting a file that was published as a symlink would break the published file
        if( Files.isSymbolicLink(target) ) {
            log.warn1("Some files were published as symlinks and will not be deleted by automatic cleanup -- use publish mode 'copy' or 'link' instead", firstOnly: true)
            log.trace "File ${path?.toUriString()} was published as a symlink, it will not be deleted"
            symlinkedOutputs << path
            return
        }

        // get the corresponding task
        final pathState = paths[path]
        if( pathState != null ) {
            log.trace "File ${path.toUriString()} was published by task <${pathState.taskName}>"

            // mark file as published
            pathState.published = true

            // delete file if it can be deleted
            if( canDeleteFile(path) )
                deleteFile(path)
        }
        else {
            log.trace "File ${path.toUriString()} was published before task was marked as completed"

            // save file to be processed when task completes
            publishedOutputs << path
        }
    }

    /**
     * When a process is closed (all tasks of the process have been created),
     * mark the process as closed and scan files for cleanup.
     *
     * NOTE: a process may submit additional tasks after it is closed, if a
     * task fails and is retried. A task that will be retried remains a consumer
     * of its input files until the retry completes.
     *
     * @param process
     */
    void onProcessClose(TaskProcessor process) {
        eventQueue.put(new Event.ProcessClosed(process))
    }

    private void onProcessClose0(TaskProcessor process) {
        processes[process.name].closed = true
    }

    /**
     * Process any remaining events and cleanup when the workflow is done.
     */
    @Override
    void onFlowComplete() {
        // stop the cleanup thread before the final pass, since the cleanup state is not thread-safe
        done.countDown()
        cleanupThread?.join()
        handleEvents()
    }

    /**
     * Delete any files that can be deleted.
     */
    private void cleanup0() {
        // iterate over a copy since deleted files are removed from the map
        for( Path path : paths.keySet().toList() ) {
            if( canDeleteFile(path) )
                deleteFile(path)
        }
    }

    /**
     * Determine whether a file can be deleted.
     *
     * A file can be deleted if:
     * - the file has been published (or doesn't need to be published)
     * - all of its process consumers are closed
     * - all of its task consumers are completed
     * - it was not published as a symlink
     *
     * @param path
     */
    private boolean canDeleteFile(Path path) {
        final pathState = paths[path]

        pathState.published
            && pathState.consumerProcesses.every( p -> processes[p].closed )
            && path !in consumerTasks
            && path !in symlinkedOutputs
    }

    /**
     * Delete a file and stop tracking it.
     *
     * @param path
     */
    private void deleteFile(Path path) {
        final pathState = paths.remove(path)

        log.trace "[${pathState.taskName}] Deleting file: ${path.toUriString()}"
        try {
            FileHelper.deletePath(path)
        }
        catch( Exception e ) {
            log.warn "[${pathState.taskName}] Unable to delete file: ${path.toUriString()} -- cause: ${e.message ?: e}"
        }
    }

    /**
     * Delete the work directory of a failed task attempt.
     *
     * @param task
     */
    private void deleteWorkDir(TaskRun task) {
        log.trace "[${task.name}] Deleting work directory of failed attempt: ${task.workDir.toUriString()}"
        try {
            FileHelper.deletePath(task.workDir)
        }
        catch( Exception e ) {
            log.warn "[${task.name}] Unable to delete work directory of failed attempt: ${task.workDir.toUriString()} -- cause: ${e.message ?: e}"
        }
    }

    @TupleConstructor
    @PackageScope
    static class ProcessOutput {
        DataflowWriteChannel channel
        boolean forwardsInputs
    }

    @PackageScope
    static class ProcessState {
        List<Set<String>> consumers
        List<Boolean> publishable
        boolean closed = false

        ProcessState(List<Set<String>> consumers, List<Boolean> publishable) {
            this.consumers = consumers
            this.publishable = publishable
        }
    }

    @PackageScope
    static class PathState {
        String taskName
        Set<String> consumerProcesses
        boolean published = false

        PathState(String taskName, Set<String> consumerProcesses) {
            this.taskName = taskName
            this.consumerProcesses = consumerProcesses
        }
    }

    static private interface Event {
        @TupleConstructor
        static class TaskPending implements Event {
            TaskRun task
        }
        @TupleConstructor
        static class TaskCompleted implements Event {
            TaskRun task
        }
        @TupleConstructor
        static class FilePublished implements Event {
            Path path
            Path target
        }
        @TupleConstructor
        static class ProcessClosed implements Event {
            TaskProcessor process
        }
    }

}
