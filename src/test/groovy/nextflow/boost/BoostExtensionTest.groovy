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

package nextflow.boost

import groovyx.gpars.dataflow.DataflowQueue
import nextflow.Channel
import spock.lang.Specification

class BoostExtensionTest extends Specification {

    def 'should scan values' () {
        given:
        def source = new DataflowQueue()
        VALUES.each { v -> source << v }
        source << Channel.STOP

        when:
        def target = SEED != null
            ? new BoostExtension().scan(source, SEED, ACCUMULATOR)
            : new BoostExtension().scan(source, ACCUMULATOR)
        def result = []
        def v
        while( (v = target.getVal()) != Channel.STOP )
            result << v

        then:
        result == EXPECTED

        where:
        VALUES      | SEED  | ACCUMULATOR                          | EXPECTED
        [1, 2, 3]   | null  | { acc, x -> acc + x }                | [1, 3, 6]
        [1, 2, 3]   | 10    | { acc, x -> acc + x }                | [11, 13, 16]
        [1, 2, 3]   | null  | { acc, x -> x == 2 ? null : 'x' }    | [1, null, 'x']
    }

}
