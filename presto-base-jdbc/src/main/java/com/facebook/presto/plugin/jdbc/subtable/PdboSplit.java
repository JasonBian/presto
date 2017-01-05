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

import com.facebook.presto.plugin.jdbc.util.JdbcUtil;

/**
 * Created by bianzexin on 16/12/26.
 */
public class PdboSplit {

    private String connectorId;
    private String schemaName;
    private String tableName;
    private Long rows;
    private Long beginIndex;
    private Long endIndex;
    private String recordFlag;
    private String dbHost;
    private String connectionUrl;
    private String prestoWorkHost;
    private String remotelyAccessible;
    private String splitField;
    private String username;
    private String password;
    private Long timeStamp;
    private int scanNodes;
    private long fieldMaxValue;
    private long fieldMinValue;
    private String prestoTableName;
    private String calcStepEnable;
    private String controlScanConcurrencyEnabled;
    private int scanConcurrencyCount;

    public String getConnectorId()
    {
        return connectorId;
    }

    public PdboSplit setConnectorId(String connectorId)
    {
        this.connectorId = connectorId;
        return this;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public PdboSplit setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public PdboSplit setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public Long getRows()
    {
        return rows;
    }

    public PdboSplit setRows(Long rows)
    {
        this.rows = rows;
        return this;
    }

    public Long getBeginIndex()
    {
        return beginIndex;
    }

    public PdboSplit setBeginIndex(Long beginIndex)
    {
        this.beginIndex = beginIndex;
        return this;
    }

    public Long getEndIndex()
    {
        return endIndex;
    }

    public PdboSplit setEndIndex(Long endIndex)
    {
        this.endIndex = endIndex;
        return this;
    }

    public String getRecordFlag()
    {
        return recordFlag;
    }

    public PdboSplit setRecordFlag(String recordFlag)
    {
        this.recordFlag = recordFlag;
        return this;
    }

    public String getDbHost()
    {
        return (dbHost == null || dbHost.length() == 0) ? "default" : dbHost;
    }

    public PdboSplit setDbHost(String dbHost)
    {
        this.dbHost = dbHost;
        return this;
    }

    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    public PdboSplit setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getPrestoWorkHost()
    {
        return prestoWorkHost;
    }

    public PdboSplit setPrestoWorkHost(String prestoWorkHost)
    {
        this.prestoWorkHost = prestoWorkHost;
        return this;
    }

    public boolean getRemotelyAccessible()
    {
        return JdbcUtil.converStringToBoolean(remotelyAccessible, false);
    }

    public PdboSplit setRemotelyAccessible(String remotelyAccessible)
    {
        this.remotelyAccessible = remotelyAccessible;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    public PdboSplit setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    public PdboSplit setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getSplitField()
    {
        return splitField;
    }

    public PdboSplit setSplitField(String splitField)
    {
        this.splitField = splitField;
        return this;
    }

    public Long getTimeStamp()
    {
        return timeStamp;
    }

    public PdboSplit setTimeStamp(Long timeStamp)
    {
        this.timeStamp = timeStamp;
        return this;
    }

    public int getScanNodes()
    {
        return scanNodes;
    }

    public PdboSplit setScanNodes(int scanNodes)
    {
        this.scanNodes = scanNodes;
        return this;
    }

    public long getFieldMaxValue()
    {
        return fieldMaxValue;
    }

    public PdboSplit setFieldMaxValue(long fieldMaxValue)
    {
        this.fieldMaxValue = fieldMaxValue;
        return this;
    }

    public long getFieldMinValue()
    {
        return fieldMinValue;
    }

    public PdboSplit setFieldMinValue(long fieldMinValue)
    {
        this.fieldMinValue = fieldMinValue;
        return this;
    }

    public String getPrestoTableName()
    {
        return prestoTableName;
    }

    public void setPrestoTableName(String prestoTableName)
    {
        this.prestoTableName = prestoTableName;
    }

    public boolean isCalcStepEnable()
    {
        return JdbcUtil.converStringToBoolean(calcStepEnable, false);
    }

    public PdboSplit setCalcStepEnable(String calcStepEnable)
    {
        this.calcStepEnable = calcStepEnable;
        return this;
    }

    public boolean getControlScanConcurrencyEnabled()
    {
        return JdbcUtil.converStringToBoolean(controlScanConcurrencyEnabled, false);
    }

    public PdboSplit setControlScanConcurrencyEnabled(
            String controlScanConcurrencyEnabled)
    {
        this.controlScanConcurrencyEnabled = controlScanConcurrencyEnabled;
        return this;
    }

    public int getScanConcurrencyCount()
    {
        return scanConcurrencyCount;
    }

    public PdboSplit setScanConcurrencyCount(int scanConcurrencyCount)
    {
        this.scanConcurrencyCount = scanConcurrencyCount;
        return this;
    }
}
