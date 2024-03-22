/*
 * Copyright 2022, Seqera Labs
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

package nextflow.cleanup

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.plugin.Scoped
import org.pf4j.PluginWrapper

/**
 * Implements the cleanup plugin entry point
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@CompileStatic
class CleanupPlugin extends BasePlugin {

    CleanupPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
