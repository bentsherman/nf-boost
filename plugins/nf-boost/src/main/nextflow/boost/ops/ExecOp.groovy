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
import nextflow.Const
import nextflow.Session
import nextflow.extension.CH
import nextflow.processor.TaskProcessor
import nextflow.script.params.BaseInParam
import nextflow.script.params.BaseOutParam
import nextflow.script.BodyDef
import nextflow.script.ExecutionStack
import nextflow.script.ProcessConfig
import nextflow.script.TokenVar

@CompileStatic
class ExecOp {

    private DataflowReadChannel source

    private Session session

    private String name

    private Closure body

    ExecOp(DataflowReadChannel source, Session session, String name, Closure body) {
        if( name.contains(':') )
            throw new IllegalArgumentException("In `exec` operator -- invalid process name: '${name}' (should match /[A-Za-z][A-Za-z_]/)")

        if( body.parameterTypes.size() != 1 )
            throw new IllegalArgumentException("In `exec` operator -- inline exec body with more than one parameter is not currently supported")

        this.source = source
        this.session = session
        this.name = name
        this.body = body
    }

    DataflowWriteChannel apply() {
        final script = session.script

        // determine fully-qualified process name
        final prefix = ExecutionStack.workflow()?.getName()
        final simpleName = name
        final processName = prefix ? prefix + Const.SCOPE_SEP + name : name

        // build process
        final config = new ProcessConfig(script, processName)

        config._in_val(new TokenVar('__val'))
        config._out_val(new TokenVar('__result'))

        final bodyDef = new BodyDef(
            new ClosureWithBody(body),
            '<inline exec process>',
            'exec'
        )

        // apply process config
        config.applyConfig((Map)session.config.process, simpleName, simpleName, processName)

        // set input channels
        final declaredInputs = config.getInputs()
        final inputs = List.of(source)
        for( int i = 0; i < inputs.size(); i++ ) {
            final param = (declaredInputs[i] as BaseInParam)
            param.setFrom(inputs[i])
            param.init()
        }

        // set output channels
        final declaredOutputs = config.getOutputs()
        final singleton = declaredInputs.allScalarInputs()

        for( int i = 0; i < declaredOutputs.size(); i++ ) {
            final param = (declaredOutputs[i] as BaseOutParam)
            param.setInto( CH.create(singleton) )
        }

        // make a copy of the output list because execution can change it
        final declaredOutputs0 = declaredOutputs.clone()

        // create the executor
        final executor = session
            .executorFactory
            .getExecutor(processName, config, bodyDef, session)

        // invoke process
        new TaskProcessor(processName, executor, session, script, config, bodyDef).run()

        // emit output
        declaredOutputs0.getChannels().first()
    }

}

class ClosureWithBody extends Closure {

    private Closure body

    ClosureWithBody(Closure body) {
        super(null, null)
        this.body = body
    }

    @Override
    Object call(final Object... args) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object call(final Object arguments) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object call() {
        final cl = (Closure)body.clone()
        cl.setDelegate(delegate)
        cl.setResolveStrategy(Closure.DELEGATE_ONLY)

        delegate.__result = cl.call(delegate.__val)
    }
}
