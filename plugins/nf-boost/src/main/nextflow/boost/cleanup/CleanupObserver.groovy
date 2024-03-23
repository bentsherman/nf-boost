/*
 * Copyright 2024, Ben Sherman
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

import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Session
import nextflow.dag.DAG
import nextflow.file.FileHelper
import nextflow.processor.PublishDir
import nextflow.processor.PublishDir.Mode
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.script.params.FileOutParam
import nextflow.util.ThreadPoolManager
/**
 * Delete temporary files once they are no longer needed.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class CleanupObserver implements TraceObserver {

    private Session session

    private Map<String,ProcessState> processes = [:]

    private Map<TaskRun,TaskState> tasks = [:]

    private Map<Path,PathState> paths = [:]

    private Set<Path> publishedOutputs = []

    private Lock sync = new ReentrantLock()

    private ExecutorService threadPool

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.threadPool = new ThreadPoolManager('TaskCleanup')
            .withConfig(session.config)
            .create()

        if( session.resumeMode )
            log.warn "This experimental version of automatic cleanup does not work with resume -- deleted tasks will be re-executed"
    }

    /**
     * When the workflow begins, determine the consumers of each process
     * in the DAG.
     */
    @Override
    void onFlowBegin() {

        // construct process lookup
        final dag = session.dag
        final withIncludeInputs = [] as Set

        for( def processNode : dag.vertices ) {
            // skip nodes that are not processes
            final process = processNode.process
            if( !process )
                continue

            // find all downstream processes in the abstract dag
            def processName = process.name
            def consumers = [] as Set
            def queue = [ processNode ]

            while( !queue.isEmpty() ) {
                // remove a node from the search queue
                final sourceNode = queue.remove(0)

                // search each outgoing edge from the source node
                for( def edge : dag.edges ) {
                    if( edge.from != sourceNode )
                        continue

                    def node = edge.to

                    // skip if process is terminal
                    if( !node )
                        continue

                    // add process nodes to the list of consumers
                    if( node.process != null )
                        consumers << node.process.name
                    // add operator nodes to the queue to keep searching
                    else
                        queue << node
                }
            }

            // add event listener for process close
            process.operator.addDataflowEventListener(new DataflowEventAdapter() {
                @Override
                void afterStop(final DataflowProcessor processor) {
                    onProcessClose(process)
                }
            })

            processes[processName] = new ProcessState(consumers ?: [processName] as Set)

            // check if process uses includeInputs
            final hasIncludeInputs = process
                .config.getOutputs()
                .any( p -> p instanceof FileOutParam && p.includeInputs )

            if( hasIncludeInputs )
                withIncludeInputs << processName
        }

        // update producers of processes that use includeInputs
        processes.each { processName, processState ->
            final consumers = processState.consumers
            for( def consumer : consumers.intersect(withIncludeInputs) ) {
                log.trace "Process `${consumer}` uses includeInputs, adding its consumers to `${processName}`"
                final consumerState = processes[consumer]
                consumers.addAll(consumerState.consumers)
            }

            log.trace "Process `${processName}` is consumed by the following processes: ${consumers}"
        }
    }

    static private final Set<Mode> INVALID_PUBLISH_MODES = [Mode.COPY_NO_FOLLOW, Mode.RELLINK, Mode.SYMLINK]

    /**
     * Log warning for any process that uses any incompatible features.
     *
     * @param process
     */
    void onProcessCreate( TaskProcessor process ) {
        // check for incompatible publish modes
        final task = process.createTaskPreview()
        final publishers = task.config.getPublishDir()

        if( publishers.any( p -> p.mode in INVALID_PUBLISH_MODES ) )
            log.warn "Process `${process.name}` is publishing files as symlinks, which may be invalidated by automatic cleanup -- consider using 'copy' or 'link' instead"
    }

    /**
     * When a task is created, add it to the state map and add it as a consumer
     * of any upstream tasks and output files.
     *
     * @param handler
     * @param trace
     */
    @Override
    void onProcessPending(TaskHandler handler, TraceRecord trace) {
        // query task input files
        final task = handler.task
        final inputs = task.getInputFilesMap().values()

        sync.withLock {
            // add task to the task state map
            tasks[task] = new TaskState()

            // add task as consumer of each upstream task and output file
            for( Path path : inputs ) {
                if( path in paths ) {
                    final pathState = paths[path]
                    final taskState = tasks[pathState.task]
                    taskState.consumers << task
                    pathState.consumers << task
                }
            }
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
        final task = handler.task

        // handle failed tasks separately
        if( !task.isSuccess() ) {
            handleTaskFailure(task)
            return
        }

        // query task output files
        final outputs = task
            .getOutputsByType(FileOutParam)
            .values()
            .flatten() as List<Path>

        // get publish outputs
        final publishers = task.config.getPublishDir()
        final publishOutputs = outputs.findAll( output ->
            publishers.any( publisher -> canPublish(publisher, task, output) )
        )

        log.trace "[${task.name}] will publish the following files: ${publishOutputs*.toUriString()}"

        sync.withLock {
            // mark task as completed
            tasks[task].completed = true

            // remove any outputs have already been published
            final alreadyPublished = publishedOutputs.intersect(publishOutputs)
            publishedOutputs.removeAll(alreadyPublished)
            publishOutputs.removeAll(alreadyPublished)

            // add publish outputs to wait on
            tasks[task].publishOutputs = publishOutputs as Set<Path>

            // scan tasks for cleanup
            cleanup0()

            // add each output file to the path state map
            for( Path path : outputs ) {
                final pathState = new PathState(task)
                if( path !in publishOutputs )
                    pathState.published = true

                paths[path] = pathState
            }
        }
    }

    private boolean canPublish(PublishDir publisher, TaskRun task, Path source) {
        def target = task.targetDir.relativize(source)

        if( publisher.pattern ) {
            final matcher = getPathMatcherFor(publisher.pattern, source.fileSystem)
            if( !matcher.matches(target) )
                return false
        }

        if( publisher.saveAs )
            target = publisher.saveAs.call(target.toString())

        return target != null
    }

    @Memoized
    private PathMatcher getPathMatcherFor(String pattern, FileSystem fileSystem) {
        FileHelper.getPathMatcherFor("glob:${pattern}", fileSystem)
    }

    /**
     * When a task fails, mark it as completed without tracking its
     * output files or triggering a cleanup.
     *
     * @param task
     */
    void handleTaskFailure(TaskRun task) {
        sync.withLock {
            // mark task as completed
            tasks[task].completed = true
        }
    }

    /**
     * When a file is published, mark it as published and check
     * the corresponding task for cleanup.
     *
     * If the file is published before the corresponding task is
     * marked as completed, save it for later.
     *
     * @param destination
     * @param source
     */
    @Override
    void onFilePublish(Path destination, Path source) {
        sync.withLock {
            // get the corresponding task
            final pathState = paths[source]
            if( pathState ) {
                final task = pathState.task

                log.trace "File ${source.toUriString()} was published by task <${task.name}>"

                // mark file as published
                tasks[task].publishOutputs.remove(source)
                pathState.published = true

                // delete task if it can be deleted
                if( canDeleteTask(task) )
                    deleteTask(task)
                else if( canDeleteFile(source) )
                    deleteFile(source)
            }
            else {
                log.trace "File ${source.toUriString()} was published before task was marked as completed"

                // save file to be processed when task completes
                publishedOutputs << source
            }
        }
    }

    /**
     * When a process is closed (all tasks of the process have been created),
     * mark the process as closed and scan tasks for cleanup.
     *
     * @param process
     */
    void onProcessClose(TaskProcessor process) {
        sync.withLock {
            processes[process.name].closed = true
            cleanup0()
        }
    }

    /**
     * When the workflow completes, delete all task directories (only
     * when using the 'lazy' strategy).
     */
    @Override
    void onFlowComplete() {
        threadPool.shutdown()
    }

    /**
     * Delete any task directories and output files that can be deleted.
     */
    private void cleanup0() {
        for( TaskRun task : tasks.keySet() )
            if( canDeleteTask(task) )
                deleteTask(task)

        for( Path path : paths.keySet() )
            if( canDeleteFile(path) )
                deleteFile(path)
    }

    /**
     * Determine whether a task directory can be deleted.
     *
     * A task directory can be deleted if:
     * - the task has completed
     * - the task directory hasn't already been deleted
     * - all of its publish outputs have been published
     * - all of its process consumers are closed
     * - all of its task consumers are completed
     *
     * @param task
     */
    private boolean canDeleteTask(TaskRun task) {
        final taskState = tasks[task]
        final processState = processes[task.processor.name]

        taskState.completed
            && !taskState.deleted
            && taskState.publishOutputs.isEmpty()
            && processState.consumers.every( p -> processes[p].closed )
            && taskState.consumers.every( t -> tasks[t].completed )
    }

    /**
     * Delete a task directory.
     *
     * @param task
     */
    private void deleteTask(TaskRun task) {
        log.trace "[${task.name}] Deleting task directory: ${task.workDir.toUriString()}"

        // delete task
        threadPool.submit({
            try {
                FileHelper.deletePath(task.workDir)
            }
            catch( Exception e ) {}
        } as Runnable)

        // mark task as deleted
        final taskState = tasks[task]
        taskState.deleted = true
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
        final processState = processes[pathState.task.processor.name]

        pathState.published
            && !pathState.deleted
            && processState.consumers.every( p -> processes[p].closed )
            && pathState.consumers.every( t -> tasks[t].completed )
    }

    /**
     * Delete a file.
     *
     * @param path
     */
    private void deleteFile(Path path) {
        final pathState = paths[path]
        final task = pathState.task

        if( !tasks[task].deleted ) {
            log.trace "[${task.name}] Deleting file: ${path.toUriString()}"
            FileHelper.deletePath(path)
        }
        pathState.deleted = true
    }

    static private class ProcessState {
        Set<String> consumers
        boolean closed = false

        ProcessState(Set<String> consumers) {
            this.consumers = consumers
        }
    }

    static private class TaskState {
        Set<TaskRun> consumers = []
        Set<Path> publishOutputs = []
        boolean completed = false
        boolean deleted = false
    }

    static private class PathState {
        TaskRun task
        Set<TaskRun> consumers = []
        boolean deleted = false
        boolean published = false

        PathState(TaskRun task) {
            this.task = task
        }
    }

}
