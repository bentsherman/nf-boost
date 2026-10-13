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

import nextflow.util.Duration
import spock.lang.Specification

class BoostConfigTest extends Specification {

    def 'should parse cleanup option #VALUE' () {
        expect:
        new BoostConfig([cleanup: VALUE]).cleanup == EXPECTED

        where:
        VALUE   | EXPECTED
        null    | null
        false   | null
        true    | 'v1'
        'v1'    | 'v1'
        'v2'    | 'v2'
    }

    def 'should reject invalid cleanup option #VALUE' () {
        when:
        new BoostConfig([cleanup: VALUE])

        then:
        thrown(IllegalArgumentException)

        where:
        VALUE << ['v3', 'true', 1]
    }

    def 'should parse cleanup failed tasks option' () {
        expect:
        !new BoostConfig([:]).cleanupFailedTasks
        new BoostConfig([cleanupFailedTasks: true]).cleanupFailedTasks
    }

    def 'should parse cleanup interval' () {
        expect:
        new BoostConfig([:]).cleanupInterval == Duration.of('60s')
        new BoostConfig([cleanupInterval: '5s']).cleanupInterval == Duration.of('5s')
    }

}
