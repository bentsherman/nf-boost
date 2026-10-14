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

import java.nio.file.Path

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.text.GStringTemplateEngine
import groovy.yaml.YamlSlurper
import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.boost.writers.TextWriter
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.script.dsl.Description
import nextflow.util.CsvWriter
import org.yaml.snakeyaml.Yaml

@CompileStatic
class BoostExtension extends PluginExtensionPoint {

    private Session session

    @Override
    void init(Session session) {
        this.session = session
    }

    /// FUNCTIONS

    @Function
    @Description('''
        Load a value from a JSON file.
    ''')
    Object fromJson(Path source) {
        return new JsonSlurper().parse(source)
    }

    @Function
    @Description('''
        Load a value from a JSON string.
    ''')
    Object fromJson(String text) {
        return new JsonSlurper().parseText(text)
    }

    @Function
    @Description('''
        Convert a value to JSON. Set `pretty` to `true` to pretty-print the output.
    ''')
    String toJson(Object value, boolean pretty=false) {
        final json = JsonOutput.toJson(value)
        return pretty
            ? JsonOutput.prettyPrint(json)
            : json
    }

    @Function
    @Description('''
        Load a value from a YAML file.
    ''')
    Object fromYaml(Path source) {
        return new YamlSlurper().parse(source)
    }

    @Function
    @Description('''
        Load a value from a YAML string.
    ''')
    Object fromYaml(String text) {
        return new YamlSlurper().parseText(text)
    }

    @Function
    @Description('''
        Convert a value to YAML.
    ''')
    String toYaml(Object value) {
        return new Yaml().dump(value)
    }

    @Function
    @Description('''
        Save a list of records (i.e. tuples or maps) to a CSV file.

        Available options: `header`, `sep`.
    ''')
    void mergeCsv(Map opts=[:], List records, Path path) {
        if( records.size() == 0 )
            throw new IllegalArgumentException('In `mergeCsv` function -- at least one record must be provided')

        new CsvWriter(opts).apply(records, path)
    }

    @Function
    @Description('''
        Save a list of items (i.e. files or strings) to a text file.

        Available options: `keepHeader`, `newLine`, `skip`.
    ''')
    void mergeText(Map opts=[:], List items, Path path) {
        if( items.size() == 0 )
            throw new IllegalArgumentException('In `mergeText` function -- at least one item must be provided')

        new TextWriter(opts).apply(items, path)
    }

    @Function
    @Description('''
        Make an HTTP request. Returns the underlying `HttpURLConnection`.

        Available options: `body`, `headers`, `method`.
    ''')
    HttpURLConnection request(Map opts=[:], String url) {
        final method = opts.method as String ?: 'GET'
        final headers = opts.headers as Map<String,String> ?: [:]
        final body = opts.body as String ?: null
        final req = (HttpURLConnection) new URL(url).openConnection()
        req.setRequestMethod(method.toUpperCase())
        for( final entry : headers ) {
            req.setRequestProperty(entry.key, entry.value)
        }
        if( body ) {
            req.setDoOutput(true)
            req.getOutputStream().write(body.getBytes('UTF-8'))
        }
        return req
    }

    @Function
    @Description('''
        Render a template file with the given binding.
    ''')
    String template(Path file, Map binding) {
        return new GStringTemplateEngine()
            .createTemplate(file.toFile())
            .make(binding)
            .toString()
    }

    @Function
    @Description('''
        Render a template string with the given binding.
    ''')
    String template(String templateText, Map binding) {
        return new GStringTemplateEngine()
            .createTemplate(templateText)
            .make(binding)
            .toString()
    }

    /// OPERATORS

    @Operator
    @Description('''
        Apply an accumulator function sequentially to each value in a channel, and emit each partially accumulated value.
    ''')
    DataflowWriteChannel scan(DataflowReadChannel source, seed=null, Closure accumulator) {
        final target = CH.createBy(source)
        def result = seed
        def seeded = seed != null

        final onNext = { val ->
            result = seeded ? accumulator(result, val) : val
            seeded = true
            target << result
        }
        final onComplete = {
            target << Channel.STOP
        }

        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])
        return target
    }

}
