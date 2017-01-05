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
package com.facebook.presto.plugin.jdbc.util;

import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Created by bianzexin on 16/12/23.
 */
public class JdbcUtil {
    private static final Logger log = Logger.get(JdbcUtil.class);

    private JdbcUtil()
    {
    }
    /**
     * Get table name : catalog.schema.table
     * @param catalog
     * @param schema
     * @param table
     * @return
     */
    public static String getTableName(String identifierQuote, String catalog, String schema, String table)
    {
        StringBuilder sql = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sql.append(quoted(identifierQuote, schema)).append('.');
        }
        else {
            if (!isNullOrEmpty(catalog)) {
                sql.append(quoted(identifierQuote, catalog)).append('.');
            }
        }
        sql.append(quoted(identifierQuote, table));
        return sql.toString();
    }

    private static String quoted(String identifierQuote, String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    /**
     * close JDBC connection
     * @param connection
     * @param stat
     * @param rs
     */
    public static void closeJdbcConnection(Connection connection, Statement stat, ResultSet rs)
    {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stat != null) {
                stat.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            log.error("close connection error : " + e.getMessage());
        }
    }

    public static Type toPrestoType(int jdbcType)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return BIGINT;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return VARCHAR;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
        }
        return null;
    }

    public static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static <E> boolean checkListNullOrEmpty(List<E> list)
    {
        return list == null || list.isEmpty();
    }

    public static boolean converStringToBoolean(String fieldValue, boolean defaultValue)
    {
        if (fieldValue == null || "".equals(fieldValue)) {
            return defaultValue;
        }
        else if ("Y".equals(fieldValue)
                || "y".equals(fieldValue)) {
            return true;
        }
        else {
            return false;
        }
    }

    public static String Base64Encode(String str)
    {
        str = str == null ? "" : str;
        return new String(Base64.getEncoder().encode(str.getBytes()), StandardCharsets.UTF_8);
    }

    public static String Base64Decode(String str)
    {
        str = str == null ? "" : str;
        return new String(Base64.getDecoder().decode(str.getBytes()), StandardCharsets.UTF_8);
    }
}
