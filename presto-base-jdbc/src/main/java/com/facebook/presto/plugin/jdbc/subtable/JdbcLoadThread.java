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

import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.subtable.PdboTableInfo.DBType;
import com.facebook.presto.plugin.jdbc.util.JdbcUtil;
import com.facebook.presto.plugin.jdbc.util.PdboMetadata;
import com.facebook.presto.server.PrestoServer;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.mysql.jdbc.Driver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static java.util.Locale.ENGLISH;

/**
 * Created by bianzexin on 16/12/24.
 */
public class JdbcLoadThread implements Runnable {
    private static final Logger log = Logger.get(JdbcLoadThread.class);
    protected final String connectionUrl;
    protected final Properties connectionProperties;
    protected final String connectorId;
    protected final Duration jdbcReloadSubtableInterval;
    private long lastLoadSubTableTimeStamp = 0L;
    private final Driver driver;
    private final boolean jdbcSubTableAllocator;

    private final ConcurrentMap<PdboTableInfo, ArrayList<PdboSplit>> pdboTables = new ConcurrentHashMap<>();

    public JdbcLoadThread(String connectionUrl,
                         Properties connectionProperties,
                         String connectorId,
                         Duration jdbcReloadSubtableInterval) throws SQLException
    {
        this.connectionUrl = connectionUrl;
        this.connectionProperties = connectionProperties;
        this.connectorId = connectorId;
        this.jdbcReloadSubtableInterval = jdbcReloadSubtableInterval;
        this.driver = new Driver();
        this.jdbcSubTableAllocator = PrestoServer.isCoordinator();
    }

    @Override
    public void run()
    {
        while (jdbcSubTableAllocator) {
            try {
                if (lastLoadSubTableTimeStamp == 0) {
                    loadPdboTableInfo();
                }
                else {
                    Thread.sleep(jdbcReloadSubtableInterval.toMillis());
                    long curTime = System.currentTimeMillis();
                    loadPdboTableInfo();
                    log.debug(connectorId + " load sub-table info spend time : " + (System.currentTimeMillis() - curTime) + " ms ");
                }
                lastLoadSubTableTimeStamp = System.currentTimeMillis();
            }
            catch (Exception e) {
                lastLoadSubTableTimeStamp = System.currentTimeMillis();
                log.error(e, connectorId + " Error reloading sub-table infomation : %s" , e.getMessage());
            }
        }
    }

    public synchronized void loadPdboTableInfo()
    {
        String sql = PdboMetadata.getPdboTableInfoSQL();
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        try {
            runner.query(conn, sql, new PdboTableResultHandle(), connectorId);
        }
        catch (SQLException e) {
            log.error(e, "loadPdboTableInfo Execute %s error : %s" , sql, e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }

    public List<PdboSplit> getPDBOLogs(String connectorId, String schemaName, String tableName)
    {
        String sql = PdboMetadata.getPdboLogsSQL();
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        List<PdboSplit> pdboSplits = null;
        try {
            pdboSplits = runner.query(conn, sql, new PdboLogsResultHandle(), connectorId, schemaName, tableName);
        }
        catch (SQLException e) {
            log.error(e, "getPDBOLogs Execute %s error : %s" , sql, e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return pdboSplits;
    }

    private class PdboLogsResultHandle implements ResultSetHandler<List<PdboSplit>>
    {
        @Override
        public List<PdboSplit> handle(ResultSet rs) throws SQLException
        {
            List<PdboSplit> tables = new ArrayList<>();
            int scannodenumber = 0;
            //A.CONNECTORID,A.SCHEMANAME,A.TABLENAME,A.ROWS,A.BEGININDEX,A.ENDINDEX,B.DBTYPE,
            //C.DBHOST,C.DBPORT,C.CONNECTION_PROPERTIES,C.PRESTO_WORK_HOST,C.REMOTELYACCESSIBLE,C.SPLITFIELD,
            //C.SCANNODENUMBER,D.USERNAME,D.PASSWORD,A.CONTROL_SCAN_CONCURRENCY_ENABLED,A.SCAN_CONCURRENCY_COUNT
            while (rs.next()) {
                scannodenumber = rs.getInt(14);
                tables.add(new PdboSplit().setConnectorId(rs.getString(1)).
                        setSchemaName(rs.getString(2)).
                        setTableName(rs.getString(3)).
                        setRows(rs.getLong(4)).
                        setBeginIndex(rs.getLong(5)).
                        setEndIndex(rs.getLong(6)).
                        setDbHost(rs.getString(8)).
                        setConnectionUrl(getConnectionURL(rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10))).
                        setPrestoWorkHost(rs.getString(11)).
                        setRemotelyAccessible(rs.getString(12)).
                        setSplitField(rs.getString(13)).
                        setScanNodes(rs.getInt(14)).
                        setUsername(JdbcUtil.Base64Decode(rs.getString(15))).
                        setPassword(JdbcUtil.Base64Decode(rs.getString(16))).
                        setCalcStepEnable("Y").
                        setControlScanConcurrencyEnabled(rs.getString(17)).
                        setScanConcurrencyCount(rs.getInt(18)));
            }
            if (scannodenumber != tables.size()) {
                tables.clear();
                loadPdboTableInfo();
            }
            return tables;
        }
    }

    private class PdboTableResultHandle implements ResultSetHandler<String>
    {
        @Override
        public String handle(ResultSet rs) throws SQLException
        {
            pdboTables.clear();
            //CONNECTORID,PRESTO_SCHEMA,PRESTO_TABLE,DBTYPE,PDBOENABLE,CONTROL_SCAN_CONCURRENCY_ENABLED,SCAN_CONCURRENCY_COUNT
            //DBHOST,DBPORT,CONNECTION_PROPERTIES,SOURCE_SCHEMA,SOURCE_TABLE,SPLITFIELD,REMOTELYACCESSIBLE,PRESTO_WORK_HOST,SCANNODENUMBER,
            //FIELDMAXVALUE,FIELDMINVALUE,USERNAME,PASSWORD,"
            while (rs.next()) {
                PdboTableInfo table = new PdboTableInfo(rs.getString(1).toLowerCase(ENGLISH),
                        rs.getString(2).toLowerCase(ENGLISH), rs.getString(3).toLowerCase(ENGLISH));
                table.setDbType(rs.getString(4));
                table.setCalcStepEnable(rs.getString(5));
                table.setControlScanConcurrencyEnabled(rs.getString(6));
                table.setScanConcurrencyCount(rs.getInt(7));
                String connectionUrl = getConnectionURL(table.getDbType(), rs.getString(8), rs.getString(9), rs.getString(10));
                PdboSplit pdboSplit = new PdboSplit().setSchemaName(rs.getString(11).toLowerCase(ENGLISH)).
                        setTableName(rs.getString(12).toLowerCase(ENGLISH)).
                        setDbHost(rs.getString(8)).
                        setConnectionUrl(connectionUrl).
                        setSplitField(rs.getString(13)).
                        setRemotelyAccessible(rs.getString(14)).
                        setPrestoWorkHost(rs.getString(15)).
                        setScanNodes(rs.getInt(16)).
                        setFieldMaxValue(rs.getLong(17)).
                        setFieldMinValue(rs.getLong(18)).
                        setUsername(JdbcUtil.Base64Decode(rs.getString(19))).
                        setPassword(JdbcUtil.Base64Decode(rs.getString(20))).
                        setCalcStepEnable(rs.getString(5)).
                        setControlScanConcurrencyEnabled(rs.getString(6)).
                        setScanConcurrencyCount(rs.getInt(7));
                ArrayList<PdboSplit> routeList = pdboTables.get(table);
                if (routeList == null) {
                    routeList = new ArrayList<>();
                }
                routeList.add(pdboSplit);
                pdboTables.put(table, routeList);
            }
            return null;
        }
    }

    private String getConnectionURL(String dbType, String dbHost, String dbPort, String connectionProperties)
            throws SQLException
    {
        String connectionUrl = "";
        if (dbType.equals(DBType.MYSQL.toString())) {
            connectionUrl = "jdbc:mysql://";
        }
        else if (dbType.equals(DBType.SQLSERVER.toString())) {
            connectionUrl = "jdbc:jtds:sqlserver://";
        }
        else if (dbType.equals(DBType.ORACLE.toString())) {
            connectionUrl = "jdbc:oracle:thin:@";
        }
        connectionUrl += dbHost + ":" + dbPort + (connectionProperties == null ? "" : connectionProperties);
        return connectionUrl;
    }

    public ConcurrentMap<PdboTableInfo, ArrayList<PdboSplit>> getPdboTableInfo()
    {
        return pdboTables;
    }

    public void commitPdboLogs(JdbcSplit split, long rowCount)
    {
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        String insertSql = PdboMetadata.getInsertPdboLogSQL(split, rowCount, connectorId);
        String updateSql = PdboMetadata.getUpdatePdboHistoryLogSQL(split, connectorId);
        try {
            runner.update(conn, updateSql);
            runner.update(conn, insertSql);
        }
        catch (SQLException e) {
            log.error(e, "insert sql : %s,update sql : %s commitPdboLogs error : %s", insertSql, updateSql, e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }

    public Connection getConnection()
    {
        Connection conn = null;
        try {
            conn = driver.connect(connectionUrl, connectionProperties);
        }
        catch (SQLException e) {
            log.error("Connect pdbo db error : " + e.getMessage());
        }
        return conn;
    }
}
