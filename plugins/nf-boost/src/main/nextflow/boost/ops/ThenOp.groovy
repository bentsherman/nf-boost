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

@CompileStatic
class ThenOp {

    private DataflowReadChannel source

    private Map<String,Closure> events

    private boolean singleton

    private EventDsl dsl

    ThenOp(DataflowReadChannel source, Map<String,Closure> events, Map opts) {
        this.source = source
        this.events = new HashMap(events)

        this.singleton = opts.singleton != null
            ? opts.singleton as boolean
            : CH.isValue(source)

        this.dsl = new EventDsl(source, singleton)
        wrapEvents()
    }

    private void wrapEvents() {
        for( final key : events.keySet() ) {
            final closure = events[key]
            final copy = (Closure)closure.clone()
            copy.setResolveStrategy(Closure.DELEGATE_FIRST)
            copy.setDelegate(dsl)

            events[key] = copy
        }

        final onComplete = events.onComplete
        events.onComplete = { DataflowProcessor proc ->
            if( onComplete )
                onComplete.call(proc)
            dsl.done()
        }
    }

    DataflowWriteChannel apply() {
        DataflowHelper.subscribeImpl(source, events)
        return dsl.getTarget()
    }

    static class EventDsl {

        private DataflowWriteChannel target

        private boolean emitted = false

        private boolean stopped = false

        EventDsl(DataflowReadChannel source, boolean singleton) {
            this.target = CH.create(singleton)
        }

        void emit(value) {
            target << value
            emitted = true
        }

        void done() {
            if( !stopped && !CH.isValue(target) || !emitted ) {
                target << Channel.STOP
                stopped = true
            }
        }

        DataflowWriteChannel getTarget() {
            target
        }
    }

}
