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

import java.nio.file.Path
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Session
import nextflow.dag.DAG
import nextflow.file.FileHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.script.params.FileOutParam
import nextflow.script.params.OutParam
import nextflow.script.params.TupleOutParam
import nextflow.util.Duration
import nextflow.util.Threads
/**
 * Delete temporary files once they are no longer needed.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class CleanupObserver implements TraceObserver {

    static private final Duration DEF_CLEANUP_INTERVAL = Duration.of('60s')

    private Session session

    private Map<String,ProcessState> processes = [:]

    private Map<Path,PathState> paths = [:]

    private Set<TaskRun> completedTasks = []

    private Set<Path> publishedOutputs = []

    private LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>()

    private long delayMillis = 5000

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.delayMillis = (session.config.navigate('boost.cleanupInterval', DEF_CLEANUP_INTERVAL) as Duration).toMillis()

        if( session.resumeMode )
            log.warn "This experimental version of automatic cleanup does not work with resume -- deleted tasks will be re-executed"
    }

    /**
     * When the workflow begins, determine the consumers of each process
     * in the DAG.
     */
    @Override
    void onFlowBegin() {

        // prpeare lookup tables
        final Map<DAG.Vertex,List<DAG.Vertex>> successors = [:]
        final Map<DataflowWriteChannel,List<DAG.Edge>> edgeLookup = [:]
        for( def edge : session.dag.edges ) {
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

        for( def vertex : session.dag.vertices ) {
            // skip nodes that are not processes
            final process = vertex.process
            if( process == null )
                continue

            // get the set of consuming processes for each process output
            final outputs = process.config.getOutputs()
            final List<Set<String>> consumers = outputs.collect { [] as Set }
            final List<Boolean> publishable = outputs.collect { false }

            for( int i = 0; i < outputs.size(); i++ ) {
                final param = outputs[i]
                final ch = param.getOutChannel()

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
                    if( ch0 in session.publishTargets ) {
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
                if( hasForwardedInputs(param) )
                    withForwardedInputs << new Tuple2(process.name, i)
            }

            processes[process.name] = new ProcessState(consumers, publishable)

            // add event listener for process close
            process.operator.addDataflowEventListener(new DataflowEventAdapter() {
                @Override
                void afterStop(final DataflowProcessor processor) {
                    onProcessClose(process)
                }
            })
        }

        // if a process B receives files from process A and forwards them
        // as outputs to process C, then process C must be marked as a
        // consumer of process A even if there is no direct dependency
        processes.each { processName, processState ->
            // append indirect consumers (and publishable) for each process output
            for( int i = 0; i < processState.consumers.size(); i++ ) {
                final consumers = processState.consumers[i]
                for( final pair : withForwardedInputs ) {
                    final consumerName = pair.first
                    final j = pair.second
                    if( consumerName in consumers ) {
                        log.trace "Process output `${consumerName}/${j+1}` may forward output files, marking its consumers as indirect consumers of `${processName}/${i+1}`"
                        final consumerState = processes[consumerName]
                        consumers.addAll(consumerState.consumers[j])
                        processState.publishable[i] |= consumerState.publishable[j]
                    }
                }

                log.trace "Process output `${processName}/${i+1}` is used by the following processes: ${processState.consumers[i]}"
            }
        }

        // launch thread to handle workflow events and cleanup
        Threads.start('Task cleanup') {
            try {
                while( true ) {
                    handleEvents()
                    Thread.sleep(delayMillis)
                }
            }
            catch( Throwable e ) {
                log.debug "Unexpected error in cleanup observer", e
            }
        }
    }

    /**
     * Determine whether a process output may forward input files
     * as outputs.
     *
     * TODO: check if a non-glob file output matches a file from this process output
     *
     * @param param
     */
    private boolean hasForwardedInputs(OutParam param) {
        if( param instanceof FileOutParam && param.includeInputs )
            return true
        if( param instanceof TupleOutParam )
            return param.inner.any( p -> p instanceof FileOutParam && p.includeInputs )
        return false
    }

    /**
     * Process workflow events and cleanup.
     */
    private void handleEvents() {
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
                onFilePublish0(event.path)
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
     * @param handler
     * @param trace
     */
    @Override
    void onProcessPending(TaskHandler handler, TraceRecord trace) {
        eventQueue.put(new Event.TaskPending(handler.task))
    }

    private void onTaskPending0(TaskRun task) {
        // mark task as consumer of each input file
        final inputs = task.getInputFilesMap().values()
        for( Path path : inputs ) {
            if( path in paths )
                paths[path].consumerTasks << task
        }
    }

    /**
     * When a task is completed, track the task and its output files
     * for automatic cleanup.
     *
     * @param handler
     * @param trace
     */
    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        eventQueue.put(new Event.TaskCompleted(handler.task))
    }

    private boolean onTaskComplete0(TaskRun task) {
        // mark failed task as completed without scanning for cleanup
        // TODO: wait for retried task to be pending first
        if( !task.isSuccess() ) {
            completedTasks << task
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

        // mark task as completed
        completedTasks << task

        // remove any outputs that have already been published
        final alreadyPublished = publishedOutputs.intersect(publishableOutputs)
        publishedOutputs.removeAll(alreadyPublished)
        publishableOutputs.removeAll(alreadyPublished)

        // add each output file to the path state map
        for( Path path : outputs ) {
            final pathState = new PathState(task, processConsumersMap[path])
            if( path !in publishableOutputs )
                pathState.published = true

            log.trace "File ${path} might be used by the following processes: ${processConsumersMap[path]}"
            paths[path] = pathState
        }

        return true
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
     * @param destination
     * @param source
     */
    @Override
    void onFilePublish(Path destination, Path source) {
        eventQueue.put(new Event.FilePublished(source))
    }

    private void onFilePublish0(Path path) {
        // get the corresponding task
        final pathState = paths[path]
        if( pathState != null ) {
            final task = pathState.task

            log.trace "File ${path.toUriString()} was published by task <${task.name}>"

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
     * task fails and is retried. The retried task should be marked as pending
     * before the failed task is marked as completed.
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
        handleEvents()
    }

    /**
     * Delete any files that can be deleted.
     */
    private void cleanup0() {
        for( Path path : paths.keySet() ) {
            if( canDeleteFile(path) )
                deleteFile(path)
        }
    }

    /**
     * Determine whether a file can be deleted.
     *
     * A file can be deleted if:
     * - the file has been published (or doesn't need to be published)
     * - the file hasn't already been deleted
     * - all of its process consumers are closed
     * - all of its task consumers are completed
     *
     * @param path
     */
    private boolean canDeleteFile(Path path) {
        final pathState = paths[path]

        pathState.published
            && !pathState.deleted
            && pathState.consumerProcesses.every( p -> processes[p].closed )
            && pathState.consumerTasks.every( t -> t in completedTasks )
    }

    /**
     * Delete a file.
     *
     * @param path
     */
    private void deleteFile(Path path) {
        final pathState = paths[path]
        final task = pathState.task

        log.trace "[${task.name}] Deleting file: ${path.toUriString()}"
        FileHelper.deletePath(path)
        pathState.deleted = true
    }

    static private class ProcessState {
        List<Set<String>> consumers
        List<Boolean> publishable
        boolean closed = false

        ProcessState(List<Set<String>> consumers, List<Boolean> publishable) {
            this.consumers = consumers
            this.publishable = publishable
        }
    }

    static private class PathState {
        TaskRun task
        Set<String> consumerProcesses
        Set<TaskRun> consumerTasks = []
        boolean deleted = false
        boolean published = false

        PathState(TaskRun task, Set<String> consumerProcesses) {
            this.task = task
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
        }
        @TupleConstructor
        static class ProcessClosed implements Event {
            TaskProcessor process
        }
    }

}
