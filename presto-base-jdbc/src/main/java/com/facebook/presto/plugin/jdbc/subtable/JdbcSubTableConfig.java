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
package com.facebook.presto.plugin.jdbc.subtable;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by bianzexin on 16/12/24.
 */
public class JdbcSubTableConfig {

    public static final String DEFAULT_VALUE = "NA";

    private String jdbcSubTableConnectionURL;
    private String jdbcSubTableConnectionUser;
    private String jdbcSubTableConnectionPassword;
    private Duration jdbcReloadSubtableInterval = new Duration(5, TimeUnit.MINUTES);
    private boolean jdbcSubTableEnable = false;

    public String getJdbcSubTableConnectionURL() {
        return jdbcSubTableConnectionURL;
    }

    @Config("jdbc-sub-table-connection-url")
    public JdbcSubTableConfig setJdbcSubTableConnectionURL(String jdbcSubTableConnectionURL) {
         this.jdbcSubTableConnectionURL = jdbcSubTableConnectionURL;
        return this;
    }

    public String getJdbcSubTableConnectionUser()
    {
        return jdbcSubTableConnectionUser;
    }

    @Config("jdbc-sub-table-connection-user")
    public JdbcSubTableConfig setJdbcSubTableConnectionUser(String jdbcSubTableConnectionUser)
    {
        this.jdbcSubTableConnectionUser = jdbcSubTableConnectionUser;
        return this;
    }

    public String getJdbcSubTableConnectionPassword()
    {
        return jdbcSubTableConnectionPassword;
    }

    @Config("jdbc-sub-table-connection-password")
    public JdbcSubTableConfig setJdbcSubTableConnectionPassword(String connectionPassword)
    {
        this.jdbcSubTableConnectionPassword = connectionPassword;
        return this;
    }

    public Duration getJdbcReloadSubtableInterval()
    {
        return jdbcReloadSubtableInterval;
    }

    @Config("jdbc-reload-subtable-interval")
    public JdbcSubTableConfig setJdbcReloadSubtableInterval(Duration jdbcReloadSubtableInterval)
    {
        this.jdbcReloadSubtableInterval = jdbcReloadSubtableInterval;
        return this;
    }

    public boolean getJdbcSubTableEnable()
    {
        return jdbcSubTableEnable;
    }

    @Config("jdbc-sub-table-enable")
    public JdbcSubTableConfig setJdbcSubTableEnable(boolean jdbcSubTableEnable)
    {
        this.jdbcSubTableEnable = jdbcSubTableEnable;
        return this;
    }

}
