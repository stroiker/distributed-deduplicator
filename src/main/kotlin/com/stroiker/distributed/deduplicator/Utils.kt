package com.stroiker.distributed.deduplicator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import java.time.Duration

object Utils {

    internal fun CqlSession.getRequestTimeout(profileName: String): Duration =
        context.configLoader.initialConfig.getProfile(profileName).getDuration(DefaultDriverOption.REQUEST_TIMEOUT)
}
