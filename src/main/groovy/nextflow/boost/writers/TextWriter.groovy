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

import java.nio.file.Path

import groovy.transform.CompileStatic
import nextflow.io.SkipLinesInputStream

@CompileStatic
class TextWriter {

    private boolean keepHeader = false

    private boolean newLine = false

    private int skipLines

    TextWriter(Map opts) {
        // set params
        if( opts.keepHeader )
            this.keepHeader = true

        if( opts.newLine )
            this.newLine = true

        this.skipLines = opts.skip != null
            ? opts.skip as int
            : (keepHeader ? 1 : 0)

        // validate params
        if( keepHeader && skipLines < 1 )
            throw new IllegalArgumentException("In function `mergeText` -- `skip` must be greater than zero when `keepHeader` is specified")
    }

    void apply(List items, Path path) {
        def wroteHeader = false

        for( final value : items ) {
            final stream = toStream(value)
            try {
                def data = stream
                if( skipLines ) {
                    data = new SkipLinesInputStream(data, skipLines)
                    data.consumeHeader()
                    if( keepHeader && !wroteHeader ) {
                        path << data.getHeader()
                        wroteHeader = true
                    }
                }

                append(path, data)
            }
            finally {
                stream.close()
            }
        }
    }

    private InputStream toStream( value ) {
        if( value instanceof Path )
            return value.newInputStream()

        if( value instanceof File )
            return value.newInputStream()

        if( value instanceof CharSequence )
            return new ByteArrayInputStream(value.toString().getBytes())

        if( value instanceof byte[] )
            return new ByteArrayInputStream(value as byte[])

        throw new IllegalArgumentException("In function `mergeText` -- invalid item [${value.class.name}]: $value")
    }

    private void append(Path path, InputStream data) {
        path << data
        if( newLine )
            path << System.lineSeparator()
    }

}