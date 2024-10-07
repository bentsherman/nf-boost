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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.boost.cleanup.CleanupObserver
import nextflow.boost.cleanup.CleanupObserverV1
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/**
 * Factory for the plugin observer
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class BoostObserverFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        List<TraceObserver> result = []

        final cleanup = session.config.navigate('boost.cleanup', false)
        if( cleanup == true || cleanup == 'v1' )
            result << new CleanupObserverV1()
        else if( cleanup == 'v2' )
            result << new CleanupObserver()
        else if( cleanup != false ) {
            throw new IllegalArgumentException("Invalid `boost.cleanup` value -- ${cleanup}")
        }

        return result
    }

}
