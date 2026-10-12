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

package nextflow.boost.writers

import java.nio.file.Files

import spock.lang.Specification

class TextWriterTest extends Specification {

    def 'should merge items with options #OPTS' () {
        given:
        def path = Files.createTempFile('merge', '.txt')
        Files.delete(path)
        def items = ['h1\nh2\na\n', 'h1\nh2\nb\n']

        when:
        new TextWriter(OPTS).apply(items, path)

        then:
        path.text == EXPECTED

        cleanup:
        Files.deleteIfExists(path)

        where:
        OPTS                          | EXPECTED
        [:]                           | 'h1\nh2\na\nh1\nh2\nb\n'
        [skip: 2]                     | 'a\nb\n'
        [keepHeader: true]            | 'h1\nh2\na\nh2\nb\n'
        [keepHeader: true, skip: 2]   | 'h1\nh2\na\nb\n'
        [skip: '2']                   | 'a\nb\n'
        [skip: 5]                     | ''
    }

    def 'should merge items of any supported type' () {
        given:
        def dir = Files.createTempDirectory('merge')
        def path = dir.resolve('merged.txt')
        def file = dir.resolve('file.txt')
        file.text = 'path\n'
        def name = 'gstring'

        when:
        new TextWriter([:]).apply([file, file.toFile(), 'string\n', "${name}\n", 'bytes\n'.bytes], path)

        then:
        path.text == 'path\npath\nstring\ngstring\nbytes\n'

        cleanup:
        dir.deleteDir()
    }

    def 'should append a newline after each item' () {
        given:
        def path = Files.createTempFile('merge', '.txt')
        Files.delete(path)

        when:
        new TextWriter([newLine: true]).apply(['a', 'b'], path)

        then:
        path.text == "a${System.lineSeparator()}b${System.lineSeparator()}"

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should not append a newline after the header' () {
        given:
        def path = Files.createTempFile('merge', '.txt')
        Files.delete(path)

        when:
        new TextWriter([newLine: true, keepHeader: true]).apply(['h\na', 'h\nb'], path)

        then:
        path.text == "h\na${System.lineSeparator()}b${System.lineSeparator()}"

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should reject an unsupported item' () {
        given:
        def path = Files.createTempFile('merge', '.txt')

        when:
        new TextWriter([:]).apply([42], path)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('invalid item')

        cleanup:
        Files.deleteIfExists(path)
    }

    def 'should reject skip of zero with keepHeader' () {
        when:
        new TextWriter([keepHeader: true, skip: 0])

        then:
        thrown(IllegalArgumentException)
    }

}
