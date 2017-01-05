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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Created by bianzexin on 16/12/26.
 */
public class PdboSplitSource implements ConnectorSplitSource {
    private final String dataSourceName;
    private final List<ConnectorSplit> splits;
    private int offset;
    private final boolean controlScanConcurrencyEnabled;
    private final int scanConcurrencyCount;

    public PdboSplitSource(String dataSourceName, Iterable<? extends ConnectorSplit> splits, boolean controlScanConcurrencyEnabled, int scanConcurrencyCount)
    {
        this.dataSourceName = dataSourceName;
        if (splits == null) {
            throw new NullPointerException("splits is null");
        }
        List<ConnectorSplit> splitsList = new ArrayList<>();
        sortSplitByDbHost(splits, controlScanConcurrencyEnabled, splitsList);
        this.splits = Collections.unmodifiableList(splitsList);
        this.controlScanConcurrencyEnabled = controlScanConcurrencyEnabled;
        this.scanConcurrencyCount = scanConcurrencyCount;
    }


    private void sortSplitByDbHost(Iterable<? extends ConnectorSplit> splits,
                                   boolean controlScanConcurrencyEnabled,
                                   List<ConnectorSplit> splitsList)
    {
        Map<String, List<ConnectorSplit>> map = new HashMap<String, List<ConnectorSplit>>();
        if (controlScanConcurrencyEnabled) {
            int splitSize = 0;
            for (ConnectorSplit split : splits) {
                if (split instanceof JdbcSplit) {
                    JdbcSplit jdbcSplit = (JdbcSplit) split;
                    List<ConnectorSplit> list = map.get(jdbcSplit.getDbHost());
                    if (list == null) {
                        list = new ArrayList<>();
                    }
                    list.add(split);
                    map.put(jdbcSplit.getDbHost(), list);
                    splitSize++;
                }
            }
            int loopCount = 0;
            while (loopCount < splitSize) {
                for (String dbHost : map.keySet()) {
                    List<ConnectorSplit> list = map.get(dbHost);
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    splitsList.add(list.get(0));
                    loopCount++;
                    list.remove(0);
                    map.put(dbHost, list);
                }
            }
        }
        else {
            for (ConnectorSplit split : splits) {
                splitsList.add(split);
            }
        }
    }

    public String getDataSourceName()
    {
        return dataSourceName;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = splits.subList(offset, offset + size);
        offset += size;
        return completedFuture(results);
    }

    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }

    @Override
    public void close()
    {
    }

    public boolean isControlScanConcurrencyEnabled()
    {
        return controlScanConcurrencyEnabled;
    }

    public int getScanConcurrencyCount()
    {
        return scanConcurrencyCount;
    }
}
