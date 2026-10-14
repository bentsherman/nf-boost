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

import java.nio.file.Files

import com.sun.net.httpserver.HttpServer
import groovyx.gpars.dataflow.DataflowQueue
import nextflow.Channel
import spock.lang.Specification

class BoostExtensionTest extends Specification {

    def 'should convert values to and from JSON' () {
        given:
        def ext = new BoostExtension()
        def value = [[id: '1', files: []], [id: '2', count: 2, ok: true]]
        def path = Files.createTempFile('value', '.json')

        expect:
        ext.toJson(value) == '[{"id":"1","files":[]},{"id":"2","count":2,"ok":true}]'
        ext.toJson(value, true).readLines().size() > 1
        ext.fromJson(ext.toJson(value)) == value
        ext.fromJson(ext.toJson(value, true)) == value

        when:
        path.text = ext.toJson(value)
        then:
        ext.fromJson(path) == value

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should convert values to and from YAML' () {
        given:
        def ext = new BoostExtension()
        def value = [[id: '1', files: []], [id: '2', count: 2, ok: true]]
        def path = Files.createTempFile('value', '.yaml')

        expect:
        ext.fromYaml(ext.toYaml(value)) == value
        ext.fromYaml('id: 1\nnames: [a, b]\n') == [id: 1, names: ['a', 'b']]

        when:
        path.text = ext.toYaml(value)
        then:
        ext.fromYaml(path) == value

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should save records to a CSV file' () {
        given:
        def ext = new BoostExtension()
        def records = [[id: 1, name: 'a'], [id: 2, name: 'b']]
        def path = Files.createTempFile('records', '.csv')
        Files.delete(path)

        when:
        ext.mergeCsv(records, path, sep: '\t', header: true)
        then:
        path.readLines() == ['"id"\t"name"', '"1"\t"a"', '"2"\t"b"']

        when:
        ext.mergeCsv([], path)
        then:
        thrown(IllegalArgumentException)

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should make an HTTP request' () {
        given:
        def server = HttpServer.create(new InetSocketAddress('localhost', 0), 0)
        server.createContext('/') { exchange ->
            def response = "${exchange.requestMethod} ${exchange.requestHeaders.getFirst('X-Test')} ${exchange.requestBody.text}".getBytes('UTF-8')
            exchange.sendResponseHeaders(200, response.length)
            exchange.responseBody.withCloseable { out -> out.write(response) }
        }
        server.start()
        def url = "http://localhost:${server.address.port}/"

        when:
        def req = new BoostExtension().request(OPTS, url)
        then:
        req.responseCode == 200
        req.inputStream.text == EXPECTED

        cleanup:
        server.stop(0)

        where:
        OPTS                                                    | EXPECTED
        [:]                                                     | 'GET null '
        [method: 'post', headers: ['X-Test': 'yes'], body: 'hi'] | 'POST yes hi'
        [method: 'put', body: 'data']                           | 'PUT null data'
    }

    def 'should render a template' () {
        given:
        def ext = new BoostExtension()
        def text = 'Dear $name, your paper was ${ accepted ? "accepted" : "rejected" }.'
        def path = Files.createTempFile('letter', '.txt')
        path.text = text

        expect:
        ext.template(text, [name: 'Grace', accepted: true]) == 'Dear Grace, your paper was accepted.'
        ext.template(path, [name: 'Ada', accepted: false]) == 'Dear Ada, your paper was rejected.'

        cleanup:
        Files.deleteIfExists(path)
    }

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
