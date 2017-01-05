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

import java.util.Objects;

/**
 * Created by bianzexin on 16/12/24.
 */
public class PdboTableInfo {
    enum DBType
    {
        MYSQL,
        SQLSERVER,
        ORACLE
    }
    private String tableId;
    private String dbType;
    private String connectorId;
    private String prestoSchema;
    private String prestoTable;
    private String calcStepEnable;
    private String controlScanConcurrencyEnabled;
    private int scanConcurrencyCount;

    public PdboTableInfo(String connectorId, String prestoSchema, String prestoTable)
    {
        this.connectorId = connectorId;
        this.prestoSchema = prestoSchema;
        this.prestoTable = prestoTable;
    }
    public String getTableId()
    {
        return tableId;
    }

    public void setTableId(String tableId)
    {
        this.tableId = tableId;
    }

    public String getDbType()
    {
        return dbType;
    }

    public void setDbType(String dbType)
    {
        this.dbType = dbType;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public void setConnectorId(String connectorId)
    {
        this.connectorId = connectorId;
    }

    public String getPrestoSchema()
    {
        return prestoSchema;
    }

    public void setPrestoSchema(String prestoSchema)
    {
        this.prestoSchema = prestoSchema;
    }

    public String getPrestoTable()
    {
        return prestoTable;
    }

    public void setPrestoTable(String prestoTable)
    {
        this.prestoTable = prestoTable;
    }

    public boolean isCalcStepEnable()
    {
        return JdbcUtil.converStringToBoolean(calcStepEnable, false);
    }

    public void setCalcStepEnable(String calcStepEnable)
    {
        this.calcStepEnable = calcStepEnable;
    }

    public boolean getControlScanConcurrencyEnabled()
    {
        return JdbcUtil.converStringToBoolean(controlScanConcurrencyEnabled, false);
    }

    public void setControlScanConcurrencyEnabled(
            String controlScanConcurrencyEnabled)
    {
        this.controlScanConcurrencyEnabled = controlScanConcurrencyEnabled;
    }

    public int getScanConcurrencyCount()
    {
        return scanConcurrencyCount;
    }

    public void setScanConcurrencyCount(int scanConcurrencyCount)
    {
        this.scanConcurrencyCount = scanConcurrencyCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getConnectorId(), getPrestoSchema(), getPrestoTable());
    }

    @Override
    public String toString()
    {
        return getConnectorId() + "-" + getPrestoSchema() + "-" + getPrestoTable();
    }

    @Override
    public boolean equals(Object obj)
    {
        return this.hashCode() == obj.hashCode();
    }
}
