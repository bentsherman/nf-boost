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

import groovyx.gpars.dataflow.Dataflow
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.operator.DataflowOperator
import nextflow.boost.cleanup.CleanupObserver.ProcessOutput
import nextflow.boost.cleanup.CleanupObserver.ProcessState
import nextflow.dag.DAG
import nextflow.file.FileHolder
import nextflow.processor.ErrorStrategy
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfigV1
import nextflow.script.params.EachInParam
import nextflow.script.params.FileOutParam
import nextflow.script.params.InputsList
import nextflow.script.params.ValueInParam
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.TaskEvent
import spock.lang.Specification

class CleanupObserverTest extends Specification {

    DAG dag = new DAG()

    DAG.Vertex process(String name) {
        final vertex = new DAG.Vertex(dag, DAG.Type.PROCESS, name)
        vertex.process = Stub(TaskProcessor) { getName() >> name }
        dag.vertices << vertex
        return vertex
    }

    DAG.Vertex operator(List outputs, String label='map') {
        final vertex = new DAG.Vertex(dag, DAG.Type.OPERATOR, label)
        vertex.operators = [ new DataflowOperator(Dataflow.DATA_FLOW_GROUP, [inputs: [new DataflowQueue()], outputs: outputs], { v -> }) ]
        dag.vertices << vertex
        return vertex
    }

    void edge(DAG.Vertex from, DAG.Vertex to, channel) {
        dag.edges << new DAG.Edge(dag, [from: from, to: to, channel: channel])
    }

    def 'should find direct and indirect consumers' () {
        given:
        def a1 = new DataflowQueue()
        def a2 = new DataflowQueue()
        def m1 = new DataflowQueue()
        def m2 = new DataflowQueue()
        def b1 = new DataflowQueue()
        def A = process('A')
        def B = process('B')
        def C = process('C')
        def op1 = operator([m1])
        def op2 = operator([m2])
        // A/1 -> map -> B
        edge(A, op1, a1)
        edge(op1, B, m1)
        // A/2 -> C, A/2 -> map -> (published)
        edge(A, C, a2)
        edge(A, op2, a2)
        edge(op2, null, m2)
        // B/1 -> (terminal)
        edge(B, null, b1)
        def outputs = [
            A: [ new ProcessOutput(a1, false), new ProcessOutput(a2, false) ],
            B: [ new ProcessOutput(b1, false) ],
            C: [],
        ]

        when:
        def states = CleanupObserver.getProcessStates(dag, outputs, [m2])

        then:
        states.A.consumers == [ ['B'] as Set, ['C'] as Set ]
        states.A.publishable == [ false, true ]
        states.B.consumers == [ [] as Set ]
        states.B.publishable == [ false ]
        states.C.consumers == []
    }

    def 'should mark consumers of forwarded inputs as indirect consumers' () {
        given:
        def a1 = new DataflowQueue()
        def b1 = new DataflowQueue()
        def A = process('A')
        def B = process('B')
        def C = process('C')
        // A/1 -> B, A/1 -> C, B/1 (forwards inputs) -> C, B/1 is published
        edge(A, B, a1)
        edge(A, C, a1)
        edge(B, C, b1)
        def outputs = [
            A: [ new ProcessOutput(a1, false) ],
            B: [ new ProcessOutput(b1, true) ],
            C: [],
        ]

        when:
        def states = CleanupObserver.getProcessStates(dag, outputs, [b1])

        then:
        states.A.consumers == [ ['B', 'C'] as Set ]
        states.A.publishable == [ true ]
        states.B.consumers == [ ['C'] as Set ]
        states.B.publishable == [ true ]
        states.A.singleDelivery == [ ['B'] as Set ]
        states.B.singleDelivery == [ ['C'] as Set ]
    }

    def 'should find single-delivery consumers' () {
        given:
        def a1 = new DataflowQueue()
        def a2 = new DataflowQueue()
        def a3 = new DataflowVariable()
        def m1 = new DataflowQueue()
        def m2 = new DataflowQueue()
        def m3 = new DataflowQueue()
        def A = process('A')
        def B = process('B')
        def C = process('C')
        def D = process('D')
        def E = process('E')
        def F = process('F')
        def G = process('G')
        def H = process('H')
        def I = process('I')
        def J = process('J')
        // A/1 -> B
        edge(A, B, a1)
        // A/1 -> map -> C
        def map = operator([m1], 'map')
        edge(A, map, a1)
        edge(map, C, m1)
        // A/1 -> combine -> D
        def combine = operator([m2], 'combine')
        edge(A, combine, a1)
        edge(combine, D, m2)
        // A/1 -> E, A/1 -> filter -> E
        def filter = operator([m3], 'filter')
        edge(A, E, a1)
        edge(A, filter, a1)
        edge(filter, E, m3)
        // A/2 -> F (each input)
        edge(A, F, a2)
        // A/3 (value) -> G
        edge(A, G, a3)
        // A/1 -> join -> H
        def m4 = new DataflowQueue()
        def join = operator([m4], 'join')
        edge(A, join, a1)
        edge(join, H, m4)
        // A/1 -> map -> filter -> I
        def m5 = new DataflowQueue()
        def m6 = new DataflowQueue()
        def map2 = operator([m5], 'map')
        def filter2 = operator([m6], 'filter')
        edge(A, map2, a1)
        edge(map2, filter2, m5)
        edge(filter2, I, m6)
        // A/1 -> map -> mix -> J, A/1 -> filter -> mix -> J
        def m7 = new DataflowQueue()
        def m8 = new DataflowQueue()
        def m9 = new DataflowQueue()
        def map3 = operator([m7], 'map')
        def filter3 = operator([m8], 'filter')
        def mix = operator([m9], 'mix')
        edge(A, map3, a1)
        edge(A, filter3, a1)
        edge(map3, mix, m7)
        edge(filter3, mix, m8)
        edge(mix, J, m9)
        def outputs = [
            A: [ new ProcessOutput(a1, false), new ProcessOutput(a2, false), new ProcessOutput(a3, false) ],
            B: [], C: [], D: [], E: [], F: [], G: [], H: [], I: [], J: [],
        ]

        when:
        def states = CleanupObserver.getProcessStates(dag, outputs, [], ['F'] as Set)

        then:
        states.A.consumers == [ ['B', 'C', 'D', 'E', 'H', 'I', 'J'] as Set, ['F'] as Set, ['G'] as Set ]
        states.A.singleDelivery == [ ['B', 'C', 'I'] as Set, [] as Set, [] as Set ]
    }

    def 'should detect each inputs' () {
        given:
        def inputs = new InputsList()
        inputs.addAll(params)
        def process = Stub(TaskProcessor) {
            getConfig() >> Stub(ProcessConfigV1) { getInputs() >> inputs }
        }

        expect:
        CleanupObserver.hasEachInputs(process) == expected

        where:
        params                                  | expected
        []                                      | false
        [ Stub(ValueInParam) ]                  | false
        [ Stub(ValueInParam), Stub(EachInParam) ] | true
    }

    // -- event handling

    Path workDir

    def setup() {
        workDir = Files.createTempDirectory('cleanup')
    }

    def cleanup() {
        workDir.deleteDir()
    }

    Path file(String name) {
        final path = workDir.resolve(name)
        path.text = name
        return path
    }

    TaskRun task(String processName, List<Path> outputs, List<Path> inputs=[], boolean success=true, ErrorStrategy errorAction=null, Path workDir=null) {
        final processor = Stub(TaskProcessor) { getName() >> processName }
        final param = Stub(FileOutParam) { getIndex() >> (short)0 }
        final inputFiles = inputs.collect { p -> new FileHolder(p) }
        Stub(TaskRun) {
            getName() >> processName
            getProcessor() >> processor
            getIndex() >> 1
            isSuccess() >> success
            isFailed() >> (errorAction != null)
            getErrorAction() >> errorAction
            getWorkDir() >> workDir
            getOutputsByType(_) >> (outputs ? [(param): outputs] : [:])
            getInputFiles() >> inputFiles
            getInputFilesMap() >> inputFiles.collectEntries { h -> [h.stageName, h.storePath] }
        }
    }

    TaskEvent event(TaskRun task) {
        new TaskEvent(Stub(TaskHandler) { getTask() >> task })
    }

    CleanupObserver observer(Map<String,ProcessState> processes) {
        final observer = new CleanupObserver()
        observer.processes = processes
        return observer
    }

    def 'should delete a file once its consumers are done' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when: 'producer completes'
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'consumer is pending'
        observer.onTaskPending(event(B))
        observer.onProcessClose(B.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'consumer completes'
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
        observer.paths.isEmpty()
        observer.consumerTasks.isEmpty()
    }

    def 'should keep deleting files when a file cannot be deleted' () {
        given:
        def readOnlyDir = Files.createDirectories(workDir.resolve('read-only'))
        def bam = readOnlyDir.resolve('a.bam')
        bam.text = 'a.bam'
        readOnlyDir.toFile().setWritable(false)
        def bai = file('a.bai')
        def A = task('A', [bam, bai])
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ false ]),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.handleEvents()
        then:
        noExceptionThrown()
        Files.exists(bam)
        !Files.exists(bai)
        observer.paths.isEmpty()

        cleanup:
        readOnlyDir.toFile().setWritable(true)
    }

    def 'should not delete a file whose consumer was created before the producer was marked as completed' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when: 'consumer is created and both processes are closed before the producer is marked as completed'
        observer.onTaskPending(event(B))
        observer.onProcessClose(A.processor)
        observer.onProcessClose(B.processor)
        observer.onTaskComplete(event(A))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'consumer completes'
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
        observer.consumerTasks.isEmpty()
    }

    def 'should not delete a file when the work directory path contains a symlink' () {
        given:
        def link = Files.createSymbolicLink(Files.createTempDirectory('cleanup-link').resolve('work'), workDir)
        def bam = link.resolve('a.bam')
        bam.text = 'a.bam'
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when: 'consumer is pending'
        observer.onTaskComplete(event(A))
        observer.onTaskPending(event(B))
        observer.onProcessClose(A.processor)
        observer.onProcessClose(B.processor)
        observer.handleEvents()
        then:
        B.inputFiles.first().storePath != bam
        Files.exists(bam)

        when: 'consumer completes'
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
        observer.consumerTasks.isEmpty()
    }

    def 'should delete a file once its single-delivery consumer task is done' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ], [ ['B'] as Set ]),
            B: new ProcessState([], []),
        ])

        when: 'producer completes'
        observer.onTaskComplete(event(A))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'consumer is pending'
        observer.onTaskPending(event(B))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'consumer completes while its process is open'
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should not delete a file that is never received by its single-delivery consumer until the consumer is closed' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ], [ ['B'] as Set ]),
            B: new ProcessState([], []),
        ])

        when: 'consumer task completes without receiving the file (e.g. it was filtered out)'
        observer.onTaskComplete(event(A))
        observer.onTaskPending(event(B))
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when:
        observer.onProcessClose(B.processor)
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should count a single-delivery consumer task created before the producer was marked as completed' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ], [ ['B'] as Set ]),
            B: new ProcessState([], []),
        ])

        when:
        observer.onTaskPending(event(B))
        observer.onTaskComplete(event(A))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when:
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should not treat a consumer as single-delivery for a file emitted by multiple outputs' () {
        given:
        def bam = file('a.bam')
        def param1 = Stub(FileOutParam) { getIndex() >> (short)0 }
        def param2 = Stub(FileOutParam) { getIndex() >> (short)1 }
        def A = Stub(TaskRun) {
            getName() >> 'A'
            getProcessor() >> Stub(TaskProcessor) { getName() >> 'A' }
            isSuccess() >> true
            getOutputsByType(_) >> [(param1): bam, (param2): [bam]]
            getInputFiles() >> []
        }
        def B = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set, ['B'] as Set ], [ false, false ], [ ['B'] as Set, ['B'] as Set ]),
            B: new ProcessState([], []),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onTaskPending(event(B))
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when:
        observer.onProcessClose(B.processor)
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should keep a file for a retried single-delivery consumer task' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B1 = task('B', [], [bam], false, ErrorStrategy.RETRY)
        def B2 = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ], [ ['B'] as Set ]),
            B: new ProcessState([], []),
        ])

        when: 'consumer fails and will be retried'
        observer.onTaskComplete(event(A))
        observer.onTaskPending(event(B1))
        observer.onTaskComplete(event(B1))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'retry completes while the consumer is open'
        observer.onTaskPending(event(B2))
        observer.onTaskComplete(event(B2))
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should wait for a non-single-delivery consumer to close' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam])
        def C = task('C', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B', 'C'] as Set ], [ false ], [ ['B'] as Set ]),
            B: new ProcessState([], []),
            C: new ProcessState([], []),
        ])

        when: 'both consumers complete while open'
        observer.onTaskComplete(event(A))
        observer.onTaskPending(event(B))
        observer.onTaskPending(event(C))
        observer.onTaskComplete(event(B))
        observer.onTaskComplete(event(C))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'non-single-delivery consumer is closed'
        observer.onProcessClose(C.processor)
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should not delete a file while its consumer process is open' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)

        when:
        observer.onProcessClose(Stub(TaskProcessor) { getName() >> 'B' })
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should delete a publishable file only after it is published' () {
        given:
        def bam = file('a.bam')
        def target = workDir.resolve('published.bam')
        Files.createLink(target, bam)
        def A = task('A', [bam])
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ true ]),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)

        when:
        observer.onFilePublish(new FilePublishEvent(bam, target))
        observer.handleEvents()
        then:
        !Files.exists(bam)
        Files.exists(target)
    }

    def 'should handle a file published before its task completes' () {
        given:
        def bam = file('a.bam')
        def target = workDir.resolve('published.bam')
        Files.createLink(target, bam)
        def other = file('collected.txt')
        def otherTarget = workDir.resolve('published.txt')
        Files.createLink(otherTarget, other)
        def A = task('A', [bam])
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ true ]),
        ])

        when:
        observer.onFilePublish(new FilePublishEvent(bam, target))
        observer.onFilePublish(new FilePublishEvent(other, otherTarget))
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        !Files.exists(bam)
        observer.publishedOutputs == [other] as Set
    }

    def 'should not delete a file that was published as a symlink' () {
        given:
        def bam = file('a.bam')
        def target = workDir.resolve('published.bam')
        Files.createSymbolicLink(target, bam)
        def A = task('A', [bam])
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ true ]),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.onFilePublish(new FilePublishEvent(bam, target))
        observer.handleEvents()
        then:
        Files.exists(bam)
        target.text == 'a.bam'
    }

    def 'should not delete the outputs of a failed task' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam], [], false)
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ false ]),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)
    }

    def 'should not delete the inputs of a failed task until it is retried' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B1 = task('B', [], [bam], false, ErrorStrategy.RETRY)
        def B2 = task('B', [], [bam])
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when: 'consumer fails and will be retried'
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.onTaskPending(event(B1))
        observer.onProcessClose(B1.processor)
        observer.onTaskComplete(event(B1))
        observer.handleEvents()
        then:
        Files.exists(bam)

        when: 'retry completes'
        observer.onTaskPending(event(B2))
        observer.onTaskComplete(event(B2))
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should delete the inputs of a failed task that will not be retried' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam])
        def B = task('B', [], [bam], false, ErrorStrategy.IGNORE)
        def observer = observer([
            A: new ProcessState([ ['B'] as Set ], [ false ]),
            B: new ProcessState([], []),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.onTaskPending(event(B))
        observer.onProcessClose(B.processor)
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        !Files.exists(bam)
    }

    def 'should not track the outputs of a task that failed after a successful exit' () {
        given:
        def bam = file('a.bam')
        def A = task('A', [bam], [], true, ErrorStrategy.TERMINATE)
        def observer = observer([
            A: new ProcessState([ [] as Set ], [ false ]),
        ])

        when:
        observer.onTaskComplete(event(A))
        observer.onProcessClose(A.processor)
        observer.handleEvents()
        then:
        Files.exists(bam)
    }

    def 'should delete the work directory of a failed attempt that is retried if enabled' () {
        given:
        def bam = file('a.bam')
        def taskDir = Files.createDirectories(workDir.resolve('task'))
        Files.createSymbolicLink(taskDir.resolve('a.bam'), bam)
        taskDir.resolve('.command.err').text = 'error'
        def B = task('B', [], [bam], false, ACTION, taskDir)
        def observer = observer([
            B: new ProcessState([], []),
        ])
        observer.cleanupFailedTasks = ENABLED

        when:
        observer.onTaskPending(event(B))
        observer.onTaskComplete(event(B))
        observer.handleEvents()
        then:
        Files.exists(taskDir) == !DELETED
        Files.exists(bam)

        where:
        ENABLED | ACTION                    | DELETED
        true    | ErrorStrategy.RETRY       | true
        false   | ErrorStrategy.RETRY       | false
        true    | ErrorStrategy.TERMINATE   | false
        true    | ErrorStrategy.IGNORE      | false
    }

}
