/*
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
package com.facebook.presto.plugin.jdbc.cache;


import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by bianzexin on 16/12/23.
 */
public class JdbcCacheConfig {
    public static final String DEFAULT_VALUE = "NA";
    private String cacheTableConfig = DEFAULT_VALUE;
    private String cacheTableClause;
    private Duration cacheRefreshInterval = new Duration(5, TimeUnit.MINUTES);
    private Duration cacheExpireInterval = new Duration(5, TimeUnit.MINUTES);
    private boolean jdbcCacheEnable = false;

    public String getCacheTableConfig() {
        return cacheTableConfig;
    }

    @Config("jdbc-cache-table-config")
    public JdbcCacheConfig setCacheTableConfig(String cacheTableConfig) {
        this.cacheTableConfig = cacheTableConfig;
        return this;
    }

    public String getCacheTableClause() {
        return cacheTableClause;
    }

    @Config("jdbc-cache-table-clause")
    public JdbcCacheConfig setCacheTableClause(String cacheTableClause) {
        this.cacheTableClause = cacheTableClause;
        return this;
    }

    public Duration getCacheRefreshInterval()
    {
        return cacheRefreshInterval;
    }

    @Config("jdbc-cache-refresh-interval")
    public JdbcCacheConfig setCacheRefreshInterval(Duration cacheRefreshInterval)
    {
        this.cacheRefreshInterval = cacheRefreshInterval;
        return this;
    }

    public Duration getCacheExpireInterval()
    {
        return cacheExpireInterval;
    }

    @Config("jdbc-cache-expire-interval")
    public JdbcCacheConfig setCacheExpireInterval(Duration cacheExpireInterval)
    {
        this.cacheExpireInterval = cacheExpireInterval;
        return this;
    }

    public boolean getJdbcCacheEnable()
    {
        return jdbcCacheEnable;
    }

    @Config("jdbc-cache-enable")
    public JdbcCacheConfig setJdbcCacheEnable(boolean jdbcCacheEnable)
    {
        this.jdbcCacheEnable = jdbcCacheEnable;
        return this;
    }
}
