
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

/**
 * Created by bianzexin on 16/12/23.
 */

import com.facebook.presto.plugin.jdbc.util.JdbcUtil;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
public class JdbcResultCache {
    private final LoadingCache<JdbcCacheSplit, List<JdbcJavaBean>> jdbcResultCache;
    private final String identifierQuote;
    private final Driver driver;
    private final Properties connectionProperties;
    private static final Logger log = Logger.get(JdbcResultCache.class);
    private List<String> tableList = new ArrayList<String>();
    private HashMap<String, List<String>> fieldList = new HashMap<String, List<String>>();
    private LinkedHashMap<String, String> cacheTableClauseMap;

    public JdbcResultCache(String identifierQuote,
                           Driver driver,
                           Properties connectionProperties,
                           JdbcCacheConfig cacheConfig)
    {
        this.identifierQuote = identifierQuote;
        this.driver = driver;
        this.connectionProperties = connectionProperties;
        long expiresAfterWrite = checkNotNull(cacheConfig.getCacheExpireInterval(), "cacheExpireInterval is null").toMillis();
        long refreshAfterWrite = checkNotNull(cacheConfig.getCacheRefreshInterval(), "cacheRefreshInterval is null").toMillis();
        analyseCacheTableAndField(cacheConfig.getCacheTableConfig(), cacheConfig.getCacheTableClause());
        jdbcResultCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(expiresAfterWrite, TimeUnit.MILLISECONDS)
                .refreshAfterWrite(refreshAfterWrite, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<JdbcCacheSplit, List<JdbcJavaBean>>(){
                    @Override
                    public List<JdbcJavaBean> load(JdbcCacheSplit key) throws Exception
                    {
                        return loadTableDataSet(key);
                    }
                });
    }
    private List<JdbcJavaBean> loadTableDataSet(JdbcCacheSplit key)
    {
        log.debug("loadTableDataSet key : " + key);
        List<JdbcJavaBean> list = new ArrayList<JdbcJavaBean>();
        try {
            Connection connection = getConnection(key.getConnectionUrl());
            HashMap<String, Type> types = getColumnTypes(key);
            String tableName = key.getBaseTableName();
            List<String> columns = fieldList.get(tableName);
            String columnPart = Joiner.on(",").join(columns);
            String sql = "SELECT " + columnPart + " FROM " +
                    JdbcUtil.getTableName(identifierQuote, key.getCatalogName(), key.getSchemaName(), key.getTableName());
            if (cacheTableClauseMap != null && !isNullOrEmpty(cacheTableClauseMap.get(tableName))) {
                sql += " WHERE " + cacheTableClauseMap.get(tableName);
            }
            Statement statement = connection.createStatement();
            statement.setFetchSize(10_000);
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            log.debug("The connection url: %s ,ExecuteQuery: %s ,spend time : %s , thread id : %s", key.getConnectionUrl(), sql, (System.currentTimeMillis() - startTime), Thread.currentThread().getId());
            while (resultSet.next()) {
                JdbcJavaBean tableDataSet = new JdbcJavaBean(columns);
                for (int i = 1; i <= columns.size(); i++) {
                    Type type = types.get(columns.get(i - 1));
                    if (type.equals(BooleanType.BOOLEAN)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getBoolean(i));
                    }
                    else if (type.equals(BigintType.BIGINT)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getLong(i));
                    }
                    else if (type.equals(DateType.DATE)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getDate(i));
                    }
                    else if (type.equals(TimeType.TIME)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getTime(i));
                    }
                    else if (type.equals(TimestampType.TIMESTAMP)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getTimestamp(i));
                    }
                    else if (type.equals(DoubleType.DOUBLE)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getDouble(i));
                    }
                    else if (type.equals(VarcharType.VARCHAR)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getString(i));
                    }
                    else if (type.equals(VarbinaryType.VARBINARY)) {
                        tableDataSet.setFieldObjectValue((i - 1), resultSet.getBytes(i));
                    }
                }
                list.add(tableDataSet);
            }
            log.debug("The connection url: %s ,parse result: %s ,spend time : %s , thread id : %s", key.getConnectionUrl(), sql, (System.currentTimeMillis() - startTime), Thread.currentThread().getId());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return list;
    }
    public List<JdbcJavaBean> getResult(JdbcCacheSplit key)
    {
        try {
            return jdbcResultCache.get(key);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public HashMap<String, Type> getColumnTypes(JdbcCacheSplit key)
    {
        HashMap<String, Type> types = new HashMap<String, Type>();
        try (Connection connection = getConnection(key.getConnectionUrl())) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet resultSet = metadata.getColumns(key.getSchemaName(), key.getCatalogName(), key.getTableName(), null)) {
                while (resultSet.next()) {
                    Type columnType = JdbcUtil.toPrestoType(resultSet.getInt("DATA_TYPE"));
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME").toLowerCase(ENGLISH);
                        types.put(columnName, columnType);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
        return types;
    }

    public Connection getConnection(String connectionURL)
            throws SQLException
    {
        Connection connection = driver.connect(connectionURL, connectionProperties);
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    private void analyseCacheTableAndField(String cacheTableConfig, String cacheTableClause)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // table name and column field
            List<LinkedHashMap<String, Object>> readValue = objectMapper.readValue(cacheTableConfig.toLowerCase(ENGLISH), ArrayList.class);
            for (LinkedHashMap<String, Object> map : readValue) {
                for (String t : map.keySet()) {
                    tableList.add(t);
                    ArrayList<String> object = (ArrayList<String>) map.get(t);
                    fieldList.put(t, object);
                }
            }
            if (!isNullOrEmpty(cacheTableClause)) {
                // table where condition
                cacheTableClauseMap = (LinkedHashMap<String, String>) objectMapper.readValue(cacheTableClause, Map.class);
            }
        }
        catch (JsonParseException e) {
            throw Throwables.propagate(e);
        }
        catch (JsonMappingException e) {
            throw Throwables.propagate(e);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean isCacheTable(String tableName)
    {
        return tableList.contains(tableName);
    }

}
