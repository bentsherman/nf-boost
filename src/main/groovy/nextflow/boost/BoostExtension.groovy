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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.text.GStringTemplateEngine
import groovy.yaml.YamlSlurper
import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.boost.ops.ThenOp
import nextflow.boost.writers.TextWriter
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint
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
    Object fromJson(Path source) {
        return new JsonSlurper().parse(source)
    }

    @Function
    Object fromJson(String text) {
        return new JsonSlurper().parseText(text)
    }

    @Function
    String toJson(Object value, boolean pretty=false) {
        final json = JsonOutput.toJson(value)
        return pretty
            ? JsonOutput.prettyPrint(json)
            : json
    }

    @Function
    Object fromYaml(Path source) {
        return new YamlSlurper().parse(source)
    }

    @Function
    Object fromYaml(String text) {
        return new YamlSlurper().parseText(text)
    }

    @Function
    String toYaml(Object value) {
        return new Yaml().dump(value)
    }

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
            throw new IllegalArgumentException('In `mergeCsv` function -- at least one record must be provided')

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
            throw new IllegalArgumentException('In `mergeText` function -- at least one item must be provided')

        new TextWriter(opts).apply(items, path)
    }

    /**
     * Make an HTTP request.
     */
    @Function
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

    /**
     * Render a template from a file with the given binding.
     *
     * @param file
     * @param binding
     */
    @Function
    String template(Path file, Map binding) {
        return new GStringTemplateEngine()
            .createTemplate(file.toFile())
            .make(binding)
            .toString()
    }

    /**
     * Render a template with the given binding.
     *
     * @param templateText
     * @param binding
     */
    @Function
    String template(String templateText, Map binding) {
        return new GStringTemplateEngine()
            .createTemplate(templateText)
            .make(binding)
            .toString()
    }

    /// OPERATORS

    @Operator
    DataflowWriteChannel scan(DataflowReadChannel source, seed=null, Closure accumulator) {
        final target = CH.createBy(source)
        def result = seed

        final onNext = { val ->
            result = result == null ? val : accumulator(result, val)
            target << result
        }
        final onComplete = {
            target << Channel.STOP
        }

        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])
        return target
    }

    @Operator
    DataflowWriteChannel then(DataflowReadChannel source, Map opts=[:], Closure closure) {
        then(source, opts + [onNext: closure])
    }

    @Operator
    DataflowWriteChannel then(DataflowReadChannel source, Map opts=[:]) {
        if( opts.emits )
            throw new IllegalArgumentException('In `then` operator -- emit names are not allowed, use `thenMany` instead')
        new ThenOp(source, opts).apply().getOutput()
    }

    @Operator
    DataflowWriteChannel then(DataflowReadChannel source, Map opts=[:], DataflowReadChannel other) {
        then(source, opts, [other])
    }

    @Operator
    DataflowWriteChannel then(DataflowReadChannel source, Map opts=[:], List<DataflowReadChannel> others) {
        List<DataflowReadChannel> sources = []
        sources.add(source)
        sources.addAll(others)
        new ThenOp(sources, opts).apply().getOutput()
    }

}
