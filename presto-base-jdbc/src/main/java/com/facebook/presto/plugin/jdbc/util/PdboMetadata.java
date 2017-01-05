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

import com.facebook.presto.plugin.jdbc.JdbcSplit;

/**
 * Created by bianzexin on 16/12/26.
 */
public final class PdboMetadata {
    public static final String PDBO_DATABASE = "route_schema";
    public static final String PDBO_TABLE = PDBO_DATABASE + ".pdbo_table";
    public static final String PDBO_ROUTE = PDBO_DATABASE + ".pdbo_route";
    public static final String PDBO_LOG = PDBO_DATABASE + ".pdbo_log";
    public static final String DB_INFO = PDBO_DATABASE + ".db_info";

    private PdboMetadata()
    {
    }

    public static String getPdboLogsSQL()
    {
        return "SELECT A.CONNECTORID,A.SCHEMANAME,A.TABLENAME,A.ROWS,A.BEGININDEX,A.ENDINDEX,B.DBTYPE,"
                + " C.DBHOST,C.DBPORT,C.CONNECTION_PROPERTIES,C.PRESTO_WORK_HOST,C.REMOTELYACCESSIBLE,C.SPLITFIELD, "
                + " C.SCANNODENUMBER,D.USERNAME,D.PASSWORD,A.CONTROL_SCAN_CONCURRENCY_ENABLED,A.SCAN_CONCURRENCY_COUNT "
                + " FROM " + PDBO_LOG + " A INNER JOIN " + PDBO_TABLE + " B "
                + " ON A.CONNECTORID = B.CONNECTORID AND A.SCHEMANAME = B.PRESTO_SCHEMA AND A.TABLENAME = B.PRESTO_TABLE "
                + " INNER JOIN " + PDBO_ROUTE + " C ON B.TABLEID = C.TABLEID "
                + " LEFT JOIN " + DB_INFO + " D ON C.UID = D.UID "
                + " WHERE A.CONNECTORID = ? "
                + " AND A.SCHEMANAME =  ? "
                + " AND A.TABLENAME = ? "
                + " AND A.RECORDFLAG = 'finish' "
                + " AND B.CALC_STEP_ENABLE = 'Y' "
                + " ORDER BY A.BEGININDEX";
    }

    public static String getPdboTableInfoSQL()
    {
        return "SELECT CONNECTORID,PRESTO_SCHEMA,PRESTO_TABLE,DBTYPE,CALC_STEP_ENABLE,CONTROL_SCAN_CONCURRENCY_ENABLED,SCAN_CONCURRENCY_COUNT,"
                + " B.DBHOST,B.DBPORT,CONNECTION_PROPERTIES,SOURCE_SCHEMA,SOURCE_TABLE,SPLITFIELD,REMOTELYACCESSIBLE,PRESTO_WORK_HOST,SCANNODENUMBER,"
                + " FIELDMAXVALUE,FIELDMINVALUE,USERNAME,PASSWORD"
                + " FROM " + PDBO_TABLE + " A INNER JOIN " + PDBO_ROUTE  + " B ON A.TABLEID = B.TABLEID"
                + " LEFT JOIN " + DB_INFO + " C ON B.UID = C.UID"
                + " WHERE CONNECTORID = ? ";
    }

    public static String getInsertPdboLogSQL(JdbcSplit split, long rowCount, String connectorId)
    {
        return "INSERT INTO " + PDBO_LOG
                + " (CONNECTORID,SCHEMANAME,TABLENAME,ROWS,BEGININDEX,ENDINDEX,RECORDFLAG,SCANNODES,TIMESTAMP) VALUES "
                + "('" + connectorId + "',"
                + "'" + split.getSchemaName() + "',"
                + "'" + split.getTableName() + "',"
                + rowCount + ","
                + split.getBeginIndex() + ","
                + split.getEndIndex() + ","
                + "'new',"
                + split.getScanNodes() + ","
                + split.getTimeStamp() + ")";
    }

    public static String getUpdatePdboHistoryLogSQL(JdbcSplit split, String connectorId)
    {
        return "UPDATE " + PDBO_LOG + " SET RECORDFLAG='runhistory' "
                + "WHERE RECORDFLAG='new' AND CONNECTORID='" + connectorId
                + "' AND SCHEMANAME='" + split.getSchemaName() + "' AND TABLENAME='"
                + split.getTableName() + "'" + " AND timestamp < " + split.getTimeStamp();
    }

}
