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

package nextflow.boost.ops

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Channel
import nextflow.extension.CH
import nextflow.extension.DataflowHelper

@CompileStatic
class ThenOp {

    private static final List<String> EVENT_NAMES = List.of('onNext', 'onComplete', 'onError')

    private List<DataflowReadChannel> sources

    private Map<String,Closure> handlers = [:]

    private boolean singleton

    private EventDsl dsl

    private Lock sync = new ReentrantLock()

    ThenOp(DataflowReadChannel source, Map opts) {
        this(List.of(source), opts)
    }

    ThenOp(List<DataflowReadChannel> sources, Map opts) {
        this.sources = sources

        this.singleton = opts.singleton != null
            ? opts.singleton as boolean
            : sources.size() == 1 && CH.isValue(sources.first())

        this.dsl = new EventDsl(singleton)
        for( final key : EVENT_NAMES ) {
            if( !opts.containsKey(key) )
                continue
            if( opts[key] !instanceof Closure )
                throw new IllegalArgumentException("In `then` operator -- option `${key}` must be a closure")

            final closure = (Closure)opts[key]
            final cl = (Closure)closure.clone()
            cl.setResolveStrategy(Closure.DELEGATE_FIRST)
            cl.setDelegate(dsl)

            handlers[key] = cl
        }
    }

    ThenOp apply() {
        for( int i = 0; i < sources.size(); i++ )
            DataflowHelper.subscribeImpl(CH.getReadChannel(sources[i]), eventsMap(handlers, i))
        return this
    }

    private Map<String,Closure> eventsMap(Map<String,Closure> handlers, int i) {
        if( sources.size() == 1 ) {
            // call done() automatically as convenience when there is only one source
            final result = new LinkedHashMap<String,Closure>(handlers)

            final onComplete = result.onComplete
            result.onComplete = { DataflowProcessor proc ->
                if( onComplete )
                    onComplete.call()
                dsl.done()
            }

            return result
        }
        else {
            // synchronize events when there are multiple sources
            final result = new LinkedHashMap<String,Closure>(handlers)

            final onNext = result.onNext
            result.onNext = { value ->
                if( onNext )
                    sync.withLock { onNext.call(value, i) }
            }

            final onComplete = result.onComplete
            result.onComplete = { DataflowProcessor proc ->
                if( onComplete )
                    sync.withLock { onComplete.call(i) }
            }

            return result
        }
    }

    DataflowWriteChannel getOutput() {
        return dsl.target
    }

    private static class EventDsl {

        private DataflowWriteChannel target

        private boolean emitted = false

        private boolean stopped = false

        EventDsl(boolean singleton) {
            target = CH.create(singleton)
        }

        void emit(value) {
            target << value
            emitted = true
        }

        void done() {
            if( stopped )
                return
            if( !CH.isValue(target) || !emitted ) {
                target << Channel.STOP
                stopped = true
            }
        }

        DataflowWriteChannel getTarget() {
            return target
        }
    }

}
