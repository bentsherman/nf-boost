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

import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.processor.PublishDir
import nextflow.processor.TaskRun
import nextflow.trace.event.WorkflowOutputEvent
/**
 * Delete temporary files once they are no longer needed.
 *
 * This implementation uses publishDir directives instead
 * of the workflow publish definition.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class CleanupObserverV1 extends CleanupObserver {

    /**
     * Determine the set of task outputs that might be published
     * based on the output channels that emitted each file.
     *
     * @param task
     * @param outputs
     */
    @Override
    protected Set<Path> getPublishableOutputs(TaskRun task, Set<Path> outputs) {
        final publishers = task.config.getPublishDir()
        return outputs.findAll( output ->
            publishers.any( publisher -> isPublishable(publisher, task, output) )
        )
    }

    /**
     * Ignore workflow outputs, since files are published by
     * publishDir directives.
     *
     * @param event
     */
    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {}

    private boolean isPublishable(PublishDir publisher, TaskRun task, Path source) {
        if( !publisher.enabled )
            return false

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

}
