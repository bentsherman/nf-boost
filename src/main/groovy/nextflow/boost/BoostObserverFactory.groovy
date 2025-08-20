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
        final observer = createCleanupObserver(session)
        return observer ? [ observer ] : []
    }

    protected TraceObserver createCleanupObserver(Session session) {
        final opts = session.config.boost as Map ?: Collections.emptyMap()
        final config = new BoostConfig(opts)

        if( config.cleanup == 'v1' )
            return new CleanupObserverV1()
        if( config.cleanup == 'v2' )
            return new CleanupObserver()
        return null
    }

}
