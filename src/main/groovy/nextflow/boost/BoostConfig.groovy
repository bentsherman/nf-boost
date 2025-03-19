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

import nextflow.config.schema.ConfigOption
import nextflow.config.schema.ConfigScope
import nextflow.config.schema.ScopeName
import nextflow.script.dsl.Description

@ScopeName('boost')
@Description('''
    The `boost` scope allows you to configure the `nf-boost` plugin.
''')
class BoostConfig implements ConfigScope {

    @ConfigOption
    @Description('''
        Set to `true` to enable automatic cleanup (default: `false`). Temporary files will be automatically deleted as soon as they are no longer needed.

        Can also be `'v1'` or `'v2'` to use an implementation that works with publishDir or the workflow output definition, respectively. Setting to `true` is equivalent to `'v1'`.
        ''')
    String cleanup

    @ConfigOption
    @Description('''
        Specify how often to scan for cleanup (default: `'60s'`).
        ''')
    Duration cleanupInterval
}
