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

package nextflow.boost

import java.nio.file.Path

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.boost.writers.CsvWriter
import nextflow.boost.writers.TextWriter
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint

@CompileStatic
class BoostExtension extends PluginExtensionPoint {

    @Override
    void init(Session session) {}

    /**
     * Save a list of records to a CSV file.
     *
     * @param opts
     * @param records
     * @param path
     */
    @Function
    void mergeCsv(Map opts=[:], List records, Path path) {
        if( records.size() == 0 )
            throw new IllegalArgumentException('At least one record must be provided')

        new CsvWriter(opts).apply(records, path)
    }

    /**
     * Save a list of items to a text file.
     *
     * @param opts
     * @param items
     * @param path
     */
    @Function
    void mergeText(Map opts=[:], List items, Path path) {
        if( items.size() == 0 )
            throw new IllegalArgumentException('At least one item must be provided')

        new TextWriter(opts).apply(items, path)
    }

}