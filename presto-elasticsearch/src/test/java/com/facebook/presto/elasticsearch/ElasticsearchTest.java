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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;

/**
 * Created by bianzexin on 16/9/20.
 */
public class ElasticsearchTest {
    private static Map<String, List<ElasticsearchTable>> catalog = new HashMap<String, List<ElasticsearchTable>>();
    public static void main(String[] args) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .build();

        String esServerAddr = "192.168.10.29";
        Integer esPort = 9300;
        String esClusterName = "elasticsearch";


        Client transportClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(esServerAddr, esPort));
        IndicesAdminClient indicesAdmin = transportClient.admin().indices();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> f =
                indicesAdmin.getMappings(new GetMappingsRequest()).actionGet().getMappings();

        Map<String, List<String>> indexTypeMapping = new HashMap<String, List<String>>();
        List<String> typeList = null;
        List<ElasticsearchTable> tblList = null;

        //index的名称列表
        Object[] indexList = f.keys().toArray();
        for (Object indexObj : indexList) {
            String index = indexObj.toString();
            ImmutableOpenMap<String, MappingMetaData> mapping = f.get(index);
            typeList = new ArrayList<String>();
            tblList = new ArrayList<ElasticsearchTable>();

            for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
                typeList.add(c.key);
                ElasticsearchTableSource tblSource = new ElasticsearchTableSource(esServerAddr, esPort, esClusterName, index, c.key);
                List<ElasticsearchTableSource> tblSrcList = new ArrayList<ElasticsearchTableSource>();
                tblSrcList.add(tblSource);
                ElasticsearchTable esTable = new ElasticsearchTable(c.key, tblSrcList);
                tblList.add(esTable);
            }
            indexTypeMapping.put(index, typeList);
            catalog.put(index.replace("-", "_"), tblList);
        }
        ImmutableMap.copyOf(
                transformValues(
                        catalog,
                        resolveAndIndexTablesFunction()));
    }

    static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTablesFunction() {
        return tables -> ImmutableMap.copyOf(uniqueIndex(transform(tables,table -> new ElasticsearchTable(table.getName(), table.getSources())),ElasticsearchTable::getName));
    }
}
