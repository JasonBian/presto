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
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.util.JdbcUtil;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Locale.ENGLISH;

/**
 * Created by bianzexin on 16/12/24.
 */
public class JdbcSubTableManager {
    private static final Logger log = Logger.get(JdbcSubTableManager.class);

    protected final String connectorId;
    protected final String identifierQuote;
    protected final Driver driver;
    protected final String defaultConnectionUrl;
    protected final Properties defaultConnectionProperties;
    protected final String jdbcSubTableConnectionUrl;
    protected final Properties jdbcSubTableConnectionProperties;
    private JdbcLoadThread loadThread;

    public JdbcSubTableManager(String connectorId, String identifierQuote,
                               Driver driver, String defaultConnectionUrl,
                               Properties defaultConnectionProperties, JdbcSubTableConfig config) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.identifierQuote = checkNotNull(identifierQuote, "identifierQuote is null");
        this.driver = checkNotNull(driver, "driver is null");
        this.defaultConnectionUrl = defaultConnectionUrl;
        this.defaultConnectionProperties = defaultConnectionProperties;

        checkNotNull(config, "config is null");
        jdbcSubTableConnectionUrl = config.getJdbcSubTableConnectionURL();
        jdbcSubTableConnectionProperties = new Properties();
        jdbcSubTableConnectionProperties.setProperty("user", config.getJdbcSubTableConnectionUser());
        jdbcSubTableConnectionProperties.setProperty("password", config.getJdbcSubTableConnectionPassword());

        if (config.getJdbcSubTableEnable()) {
            try {
                loadThread = new JdbcLoadThread(config.getJdbcSubTableConnectionURL(), jdbcSubTableConnectionProperties,
                        connectorId, config.getJdbcReloadSubtableInterval());
            }
            catch (SQLException e) {
                log.error("Init JdbcLoadTread error", e);
            }
            Thread loadTableThread = new Thread(loadThread);
            loadTableThread.setName("LoadTableThread");
            loadTableThread.setDaemon(true);
            loadTableThread.start();
        }
    }

    /**
     * Get table splits
     * @param jdbcPartition
     * @return
     */
    public ConnectorSplitSource getTableSplits(JdbcTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle jdbcTableHandle = layoutHandle.getTable();
        List<JdbcSplit> jdbcSplitsList = new ArrayList<JdbcSplit>();
        String schemaName = getSchemaName(jdbcTableHandle);
        PdboTableInfo key = new PdboTableInfo(connectorId, schemaName, jdbcTableHandle.getTableName().toLowerCase(ENGLISH));
        ArrayList<PdboSplit> pdboSplits = loadThread.getPdboTableInfo().get(key);
        if (JdbcUtil.checkListNullOrEmpty(pdboSplits)) {
            if (pdboSplits == null) {
                pdboSplits = new ArrayList<PdboSplit>();
            }
            PdboSplit config = new PdboSplit();
            config.setConnectionUrl(defaultConnectionUrl);
            config.setConnectorId(jdbcTableHandle.getCatalogName());
            config.setSchemaName(jdbcTableHandle.getSchemaName());
            config.setTableName(jdbcTableHandle.getTableName());
            config.setRemotelyAccessible("Y");
            config.setPrestoTableName(jdbcTableHandle.getTableName());
            pdboSplits.add(config);
        }
        long timeStamp = System.nanoTime();
        if (pdboSplits.get(0).isCalcStepEnable()) {
            jdbcSplitsList = getTableSplitsFromPdboLog(connectorId, schemaName, jdbcTableHandle.getTableName().toLowerCase(ENGLISH), layoutHandle, timeStamp);
        }
        if (JdbcUtil.checkListNullOrEmpty(jdbcSplitsList)) {
            for (PdboSplit config : pdboSplits) {
                constructJdbcSplits(layoutHandle, jdbcSplitsList, config, timeStamp);
            }
        }
        return new PdboSplitSource(connectorId, jdbcSplitsList, pdboSplits.get(0).getControlScanConcurrencyEnabled(), pdboSplits.get(0).getScanConcurrencyCount());
    }

    private String getSchemaName(JdbcTableHandle jdbcTableHandle)
    {
        String schemaName = "";
        if (defaultConnectionUrl.indexOf("mysql") != -1) {
            schemaName = jdbcTableHandle.getCatalogName().toLowerCase(ENGLISH);
        }
        else {
            schemaName = jdbcTableHandle.getSchemaName().toLowerCase(ENGLISH);
        }
        return schemaName;
    }

    private void constructJdbcSplits(JdbcTableLayoutHandle jdbcPartition,
                                     List<JdbcSplit> splits, PdboSplit config, long timeStamp)
    {
        List<HostAddress> addresses = getSplitHost(config.getPrestoWorkHost());
        Properties connectionProperties = resetConnectionProperties(config.getUsername(), config.getPassword());
        int scanNodes = config.getScanNodes() <= 0 ? 1 : config.getScanNodes();
        if (scanNodes == 1 || (scanNodes > 1 && isNullOrEmpty(config.getSplitField()))) {
            addJdbcSplit(jdbcPartition, splits, addresses, new String[]{"", "", ""}, connectionProperties, timeStamp, scanNodes, config);
        }
        else {
            splitTable(jdbcPartition, splits, config, addresses, connectionProperties, scanNodes, timeStamp);
        }
    }

    /**
     * Splitting table by field or limit
     */
    private void splitTable(JdbcTableLayoutHandle jdbcPartition,
                            List<JdbcSplit> splits, PdboSplit config,
                            List<HostAddress> addresses, Properties connectionProperties,
                            int scanNodes, long timeStamp)
    {
        long tableTotalRecords = 0L;
        Long[] autoIncrementFieldMinAndMaxValue = new Long[2];
        autoIncrementFieldMinAndMaxValue = getSplitFieldMinAndMaxValue(config, connectionProperties);
        tableTotalRecords = autoIncrementFieldMinAndMaxValue[0] - autoIncrementFieldMinAndMaxValue[1];
        long targetChunkSize = (long) Math.ceil(tableTotalRecords * 1.0 / scanNodes);
        long chunkOffset = 0L;
        long autoIncrementOffset = 0L;
        while (chunkOffset < tableTotalRecords) {
            long chunkLength = Math.min(targetChunkSize, tableTotalRecords - chunkOffset);
            if (chunkOffset == 0) {
                autoIncrementOffset = autoIncrementFieldMinAndMaxValue[1] - 1;
            }
            String[] splitInfo = getSplitInfo(chunkOffset, autoIncrementOffset,
                    autoIncrementFieldMinAndMaxValue, chunkLength, tableTotalRecords, config.getSplitField());
            addJdbcSplit(jdbcPartition, splits, addresses, splitInfo, connectionProperties, timeStamp, scanNodes, config);
            chunkOffset += chunkLength;
            autoIncrementOffset += chunkLength;
        }
        fillLastRecord(jdbcPartition, connectionProperties, splits, scanNodes, timeStamp, config.getFieldMaxValue(), config);
    }

    /**
     * If the table split by field,the filter conditions will follow like this :
     *      field > offset and field <= offset + chunkLength.
     * @return splitInfo[0] : splitPart; splitInfo[1] : beginIndex; splitInfo[2] : endIndex;
     */
    private String[] getSplitInfo(long chunkOffset, long autoIncrementOffset,
                                  Long[] autoIncrementFieldMinAndMaxValue, long chunkLength, long tableTotalRecords, String splitField)
    {
        String[] splitInfo = new String[3];
        String splitPart = "";
        splitInfo[1] = String.valueOf(autoIncrementOffset);
        splitPart = splitField + " > " + autoIncrementOffset + " and " + splitField + " <= ";
        if ((chunkOffset + chunkLength) == tableTotalRecords) {
            splitPart += autoIncrementFieldMinAndMaxValue[0];
            splitInfo[2] = String.valueOf(autoIncrementFieldMinAndMaxValue[0]);
        }
        else {
            splitPart += (autoIncrementOffset + chunkLength);
            splitInfo[2] = String.valueOf(autoIncrementOffset + chunkLength);
        }
        splitInfo[0] = splitPart;
        return splitInfo;
    }

    private void addJdbcSplit(JdbcTableLayoutHandle jdbcPartition,
                              List<JdbcSplit> builder, List<HostAddress> addresses, String[] splitInfo,
                              Properties connectionProperties, long timeStamp, int scanNodes, PdboSplit config)
    {
        builder.add(new JdbcSplit(connectorId, config.getConnectorId(), config.getSchemaName(), config.getTableName(),
                config.getConnectionUrl(), fromProperties(connectionProperties), jdbcPartition.getTupleDomain(),
                splitInfo[0], addresses, config.getRemotelyAccessible(), config.getPrestoTableName(),
                config.getSplitField(), splitInfo[1], splitInfo[2], timeStamp, scanNodes, config.isCalcStepEnable(), config.getDbHost()));
    }

    protected Long[] getSplitFieldMinAndMaxValue(PdboSplit conf, Properties connectionProperties)
    {
        Long[] value = new Long[2];
        if (conf.getFieldMaxValue() > 0) {
            value[0] = conf.getFieldMaxValue();
            value[1] = conf.getFieldMinValue();
            return value;
        }
        String sql = "SELECT MAX(" + conf.getSplitField() + "),MIN(" + conf.getSplitField() + ") FROM "
                + JdbcUtil.getTableName(identifierQuote, conf.getConnectorId(), conf.getSchemaName(), conf.getTableName());
        Connection connection = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            connection = driver.connect(conf.getConnectionUrl(), connectionProperties);
            stat = connection.createStatement();
            rs = stat.executeQuery(sql.toString());
            while (rs.next()) {
                value[0] = rs.getLong(1);
                value[1] = rs.getLong(2);
            }
        }
        catch (SQLException e) {
            log.error("SQL : " + sql + ",getSplitFieldMinAndMaxValue error : " + e.getMessage());
            return null;
        }
        finally {
            JdbcUtil.closeJdbcConnection(connection, stat, rs);
        }
        return value;
    }

    public List<JdbcSplit> getTableSplitsFromPdboLog(String catalogName, String schemaName, String tableName,
                                                     JdbcTableLayoutHandle jdbcPartition, long timeStamp)
    {
        List<JdbcSplit> splits = new ArrayList<>();
        List<PdboSplit> pdboLogs = loadThread.getPDBOLogs(catalogName, schemaName, tableName);
        if (JdbcUtil.checkListNullOrEmpty(pdboLogs)) {
            return splits;
        }
        int scanNodes = pdboLogs.size();
        for (PdboSplit table : pdboLogs) {
            table.setConnectorId(null);
            List<HostAddress> addresses = getSplitHost(table.getPrestoWorkHost());
            Properties connectionProperties = resetConnectionProperties(table.getUsername(), table.getPassword());
            String splitPart = table.getSplitField() + " > " + table.getBeginIndex() + " and "
                    + table.getSplitField() + " <= " + table.getEndIndex();
            addJdbcSplit(jdbcPartition, splits, addresses,
                    new String[]{splitPart, String.valueOf(table.getBeginIndex()), String.valueOf(table.getEndIndex())},
                    connectionProperties, timeStamp, scanNodes, table);
        }
        PdboSplit lastRecord = pdboLogs.get(scanNodes - 1);
        fillLastRecord(jdbcPartition, resetConnectionProperties(lastRecord.getUsername(), lastRecord.getPassword()),
                splits, scanNodes, timeStamp, lastRecord.getEndIndex(), lastRecord);
        return splits;
    }

    private void fillLastRecord(JdbcTableLayoutHandle jdbcPartition, Properties connectionProperties,
                                List<JdbcSplit> splits, int scanNodes, long timeStamp, long endIndex, PdboSplit config)
    {
        String splitPart = config.getSplitField() + " > " + endIndex;
        addJdbcSplit(jdbcPartition, splits, getSplitHost(config.getPrestoWorkHost()),
                new String[]{splitPart, "", ""}, connectionProperties, timeStamp, scanNodes, config);
    }

    private Properties resetConnectionProperties(String username, String password)
    {
        Properties connectionProperties = (Properties) defaultConnectionProperties.clone();
        if (!isNullOrEmpty(username) && !isNullOrEmpty(password)) {
            connectionProperties.setProperty("user", username);
            connectionProperties.setProperty("password", password);
        }
        return connectionProperties;
    }

    private List<HostAddress> getSplitHost(String host)
    {
        return isNullOrEmpty(host) ? ImmutableList.of() : ImmutableList.of(HostAddress.fromString(host));
    }

    public void commitPdboLogs(JdbcSplit split, long rowCount)
    {
        loadThread.commitPdboLogs(split, rowCount);
    }
}
