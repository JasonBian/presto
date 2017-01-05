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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by bianzexin on 16/12/23.
 */
public class JdbcCacheSplit {
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String baseTableName;
    private final String connectionUrl;

    public JdbcCacheSplit(String connectorId, String catalogName, String schemaName, String tableName, String connectionUrl, String baseTableName) {
        this.connectorId = checkNotNull(connectorId, "connector id in null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = checkNotNull(tableName, "table name is null");
        this.connectionUrl = checkNotNull(connectionUrl, "connectionUrl is null");
        this.baseTableName = checkNotNull(baseTableName, "base table name is null");
    }

    public String getConnectorId() {
        return connectorId;
    }

    public String getCatalogName() {
        return catalogName == null ? "null" : catalogName;
    }

    public String getSchemaName() {
        return schemaName == null ? "null" : schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getBaseTableName()
    {
        return baseTableName;
    }

    public String getConnectionUrl()
    {
        return connectionUrl;
    }
    @Override
    public int hashCode()
    {
        return Objects.hash(getConnectorId(), getConnectionUrl(), getCatalogName(), getSchemaName(), getTableName());
    }
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof JdbcCacheSplit) {
            JdbcCacheSplit other = (JdbcCacheSplit) obj;
            return this.getConnectorId().equals(other.getConnectorId())
                    && this.getConnectionUrl().equals(other.getConnectionUrl())
                    && this.getCatalogName().equals(other.getCatalogName())
                    && this.getSchemaName().equals(other.getSchemaName())
                    && this.getTableName().equals(other.getTableName());
        }
        else {
            return this.hashCode() == obj.hashCode();
        }
    }
    @Override
    public String toString()
    {
        return getConnectorId() + ","
                + getConnectionUrl() + ","
                + getCatalogName() + ","
                + getSchemaName() + ","
                + getTableName();
    }
}
