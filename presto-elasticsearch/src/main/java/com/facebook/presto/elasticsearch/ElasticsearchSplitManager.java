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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.elasticsearch.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchSplitManager implements ConnectorSplitManager
{
    private final String connectorId;
    private final ElasticsearchClient elasticsearchClient;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchConnectorId connectorId, ElasticsearchClient elasticsearchClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");
    }

    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ElasticsearchTableLayoutHandle layoutHandle = checkType(layout, ElasticsearchTableLayoutHandle.class, "partition");
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();
        ElasticsearchTable table = elasticsearchClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        //try to get shard number
        ElasticsearchTableSource uri = table.getSources().get(0);
        Client client = elasticsearchClient.getInternalClients().get(uri.getClusterName());
        String esIndexName = table.getSources().get(0).getIndex();
        GetSettingsRequest settingRequest = new GetSettingsRequest().indices(table.getSources().get(0).getIndex());
        List<ConnectorSplit> splits = new ArrayList<>();

        try {
            GetSettingsResponse gettingResp = client.admin().indices().getSettings(settingRequest).get();
            Settings contents = gettingResp.getIndexToSettings().get(esIndexName);

            int shardNumber = Integer.valueOf(contents.get("index.number_of_shards"));

            for (int shardId = 0; shardId < shardNumber; shardId ++ ) {
                splits.add(new ElasticsearchSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), shardId, layoutHandle.getTupleDomain(),uri));
            }

            Collections.shuffle(splits);

        }catch(Exception ex) {
            System.out.println(ex.getMessage());
        }

        return new FixedSplitSource(splits);
    }
}