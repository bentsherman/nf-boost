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

package nextflow.boost.ops

import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.expression.DataflowExpression
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.Channel
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.script.ChannelOut

@CompileStatic
class ThenOp {

    private static final List<String> EVENT_NAMES = List.of('onNext', 'onComplete', 'onError')

    private DataflowReadChannel source

    private Map<String,Closure> handlers = [:]

    private boolean singleton

    private EventDsl dsl

    ThenOp(DataflowReadChannel source, Map opts) {
        this.source = source

        this.singleton = opts.singleton != null
            ? opts.singleton as boolean
            : CH.isValue(source)

        final emits = opts.emits != null
            ? opts.emits as List<String>
            : List.of(EventDsl.DEFAULT_EMIT_NAME)

        this.dsl = new EventDsl(emits, singleton)
        for( final key : EVENT_NAMES ) {
            if( !opts.containsKey(key) )
                continue
            if( opts[key] !instanceof Closure ) {
                final opName = opts.emits ? 'thenMany' : 'then'
                throw new IllegalArgumentException("In `${opName}` operator -- option `${key}` must be a closure")
            }

            final closure = (Closure)opts[key]
            final cl = (Closure)closure.clone()
            cl.setResolveStrategy(Closure.DELEGATE_FIRST)
            cl.setDelegate(dsl)

            handlers[key] = cl
        }

        final onComplete = handlers.onComplete
        handlers.onComplete = { DataflowProcessor proc ->
            if( onComplete )
                onComplete.call(proc)
            dsl.done()
        }
    }

    ThenOp apply() {
        DataflowHelper.subscribeImpl(source, handlers)
        return this
    }

    DataflowWriteChannel getOutput() {
        return dsl.targets.values().first()
    }

    ChannelOut getMultiOutput() {
        return new ChannelOut(dsl.targets)
    }

    private static class EventDsl {

        private static final String DEFAULT_EMIT_NAME = '_'

        private Map<String,DataflowWriteChannel> targets = [:]

        private Map<String,Boolean> emitted = [:]

        private boolean stopped = false

        EventDsl(List<String> emits, boolean singleton) {
            for( def emit : emits ) {
                targets.put(emit, CH.create(singleton))
                emitted.put(emit, false)
            }
        }

        void emit(value) {
            if( isMultiOutput() )
                throw new IllegalArgumentException("In `thenMany` operator -- single-channel emit() is not allowed, use `then` instead")
            emit0(DEFAULT_EMIT_NAME, value)
        }

        void emit(String name, value) {
            if( !isMultiOutput() )
                throw new IllegalArgumentException("In `then` operator -- multi-channel emit() is not allowed, use `thenMany` instead")
            if( !targets.containsKey(name) )
                throw new IllegalArgumentException("In `then` operator -- emit '${name}' is not defined")
            emit0(name, value)
        }

        private void emit0(String name, value) {
            targets[name] << value
            emitted[name] = true
        }

        void done() {
            if( stopped )
                return
            for( def name : targets.keySet() ) {
                final target = targets[name]
                if( !CH.isValue(target) || !emitted[name] ) {
                    target << Channel.STOP
                    stopped = true
                }
            }
        }

        Map<String,DataflowWriteChannel> getTargets() {
            return targets
        }

        private boolean isMultiOutput() {
            targets.size() != 1 || !targets.containsKey(DEFAULT_EMIT_NAME)
        }
    }

}
