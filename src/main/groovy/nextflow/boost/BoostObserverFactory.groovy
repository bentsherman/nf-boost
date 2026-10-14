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
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2

/**
 * Factory for the plugin observer
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 */
@Slf4j
@CompileStatic
class BoostObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        final observer = createCleanupObserver(session)
        return observer ? [ observer ] : []
    }

    protected TraceObserverV2 createCleanupObserver(Session session) {
        final opts = session.config.boost as Map ?: Collections.emptyMap()
        final config = new BoostConfig(opts)

        if( config.cleanup && isGlobalCache(session) ) {
            log.warn "Automatic cleanup is not supported with the global cache, since other runs may use the deleted files -- cleanup will be disabled"
            return null
        }

        if( config.cleanup == 'v1' )
            return new CleanupObserverV1()
        if( config.cleanup == 'v2' )
            return new CleanupObserver()
        return null
    }

    /**
     * Determine whether the session uses the global cache (nf-xpack),
     * which is shared by other runs.
     *
     * @param session
     */
    static boolean isGlobalCache(Session session) {
        // matched by name since the global cache is provided by a private plugin
        return session.cache?.getClass()?.getName() == 'nextflow.cache.global.GlobalCacheDB'
    }

}
