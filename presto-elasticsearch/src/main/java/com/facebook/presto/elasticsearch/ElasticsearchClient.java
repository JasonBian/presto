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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

//import org.json.JSONObject;


/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchClient {
    private static final Logger log = Logger.get(ElasticsearchClient.class);
    private static int initialized = 0;
    private static Map<String, List<ElasticsearchTable>> catalog = new HashMap<String, List<ElasticsearchTable>>();
    private static Map<String, Map<String, ElasticsearchTable>> schemasMap = null;

    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;
    private static ElasticsearchConfig config;
    private JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec;
    private ImmutableMap<String, Client> internalClients;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.config = config;
        this.catalogCodec = catalogCodec;
        this.schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getEsServer(),config.getEsPort()));
        this.internalClients = createClients(this.schemas.get());
        //this.catalog = new HashMap<String,List<ElasticsearchTable>>();
    }

    public ImmutableMap<String, Client> getInternalClients() {
        return internalClients;
    }

    public Set<String> getSchemaNames() {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ElasticsearchTable getTable(String schema, String tableName) {
        try {
            updateSchemas();
        } catch (IOException e) {
            e.printStackTrace();
        }

        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema.toLowerCase(Locale.ENGLISH));
        if (tables == null) {
            return null;
        }
        return tables.get(tableName.toLowerCase(Locale.ENGLISH));
    }

    Map<String, Map<String, ElasticsearchTable>> updateSchemas()
            throws IOException {

        if ( schemasMap == null ) {
            schemas = Suppliers.memoize(schemasSupplier(catalogCodec,config.getEsServer(),config.getEsPort()));
            schemasMap = schemas.get();
            for (Map.Entry<String, Map<String, ElasticsearchTable>> schemaEntry : schemasMap.entrySet()) {
                Map<String, ElasticsearchTable> tablesMap = schemaEntry.getValue();
                for (Map.Entry<String, ElasticsearchTable> tableEntry : tablesMap.entrySet()) {
                    updateTableColumns(tableEntry.getValue());
                }
            }
        }
        schemas = Suppliers.memoize(Suppliers.ofInstance(schemasMap));

        return schemasMap;
    }

    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException {
        int port = src.getPort();
        String hostAddress = src.getHostAddress();
        String clusterName = src.getClusterName();
        String index = src.getIndex();
        String type = src.getType();

        log.debug(String.format("Connecting to cluster %s from %s:%d, index %s, type %s", clusterName, hostAddress, port, index, type));
        Client client = internalClients.get(clusterName);
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(type);

        // an index is optional - if no index is configured for the table, it will retrieve all indices for the doc type
        if (index != null && !index.isEmpty()) {
            mappingsRequest.indices(index);
        }

        return client
                .admin()
                .indices()
                .getMappings(mappingsRequest)
                .get()
                .getMappings();
    }

    Set<ElasticsearchColumn> getColumns(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException, IOException, JSONException {
        Set<ElasticsearchColumn> result = new HashSet();

        String type = src.getType();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = getMappings(src);

        // what makes sense is to get the reunion of all the columns from all the mappings for the specified document type
        for (ObjectCursor<String> currentIndex : allMappings.keys()) {//这个会有并行执行的可能 ffpeng
            //for(Iterator<ObjectCursor<String>> currentIndex = allMappings.keys().iterator(); currentIndex.hasNext(); )
            MappingMetaData mappingMetaData = allMappings.get(currentIndex.value).get(type);
            JSONObject json = new JSONObject(mappingMetaData.source().toString())
                    .getJSONObject(type)
                    .getJSONObject("properties");

            List<String> allColumnMetadata = getColumnsMetadata(null, json);
            HashMap<String, Integer> filter = new HashMap<String, Integer>();
            for (String columnMetadata : allColumnMetadata) {
                ElasticsearchColumn clm = createColumn(columnMetadata, filter,currentIndex.value,type);
                if (!(clm == null)) {
                    result.add(clm);
                }
            }
        }

        return result;
    }

    List<String> getColumnsMetadata(String parent, JSONObject json)
            throws JSONException {
        List<String> leaves = new ArrayList();

        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object child = json.get(key);
            String childKey = parent == null || parent.isEmpty() ? key : parent.concat(".").concat(key);

            if (child instanceof JSONObject) {//如果存在嵌套的object的情况下，如何区分
                if (((JSONObject) child).has("properties")) {
                    leaves.addAll(getColumnsMetadata(childKey, (JSONObject) ((JSONObject) child).getJSONObject("properties")));
                } else {
                    leaves.addAll(getColumnsMetadata(childKey, (JSONObject) child));
                }
            } else if (child instanceof JSONArray) {
                // ignoring arrays for now
                continue;
            } else {
                if ("type".equals(key))//只提取type字段
                    leaves.add(childKey.concat(":").concat(child.toString()));
            }
        }

        return leaves;
    }

    ElasticsearchColumn createColumn(String fieldPathType, HashMap<String, Integer> filter,String indexName, String typeName)
            throws JSONException, IOException {
        String[] items = fieldPathType.split(":");
        String type = items[1];
        String path = items[0];
        String dateFormat="";
        Type prestoType;

        if (items.length != 2) {
            log.error("Invalid column path format. Ignoring...");
            return null;
        }
        if (!path.endsWith(".type")) {
            log.error("Invalid column has no type info. Ignoring...");
            return null;
        }

        if (path.contains(".properties.")) {
            log.error("Invalid complex column type. Ignoring...");
            return null;
        }

        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "integer":
            case "long":
                prestoType = BIGINT;
                break;
            case "string":
                prestoType = VARCHAR;
                break;
            case "date":
                prestoType = TIMESTAMP;//todo  读取时间数据异常
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            default:
                log.error("Unsupported column type. Ignoring...");
                log.error(type + " " +fieldPathType);
                return null;
        }

        path = path.substring(0, path.lastIndexOf('.'));
        //path = path.replaceAll("\\.properties\\.", ".");

        String originalPath = path;
        path = path.replaceAll("\\.", "_");

        String lowkey = path.toLowerCase();
        if(filter.containsKey(lowkey))
        {
            int size = filter.get(lowkey);
            path = path.concat("_dup"+ size);
            filter.put(lowkey, size+1);
        }
        else
        {
            filter.put(lowkey, 0);
        }

        return new ElasticsearchColumn(path, prestoType, originalPath, type, dateFormat);
    }

    void updateTableColumns(ElasticsearchTable table) {
        Set<ElasticsearchColumn> columns = new HashSet();

        // the table can have multiple sources
        // the column set should be the reunion of all
        for (ElasticsearchTableSource src : table.getSources()) {
            try {
                columns.addAll(getColumns(src));
            } catch (ExecutionException | InterruptedException | IOException | JSONException e) {
                e.printStackTrace();
            }
        }

        table.setColumns(columns
                .stream()
                .collect(Collectors.toList()));
        table.setColumnsMetadata(columns
                .stream()
                .map(ElasticsearchColumnMetadata::new)
                .collect(Collectors.toList()));
    }

    static Map<String, Map<String, ElasticsearchTable>> lookupSchemas(JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException {

        //Map<String, List<ElasticsearchTable>> catalog = catalogCodec.fromJson(tableMappings);

        if (initialized == 0) {

            //catalog = catalogCodec.fromJson(tableMappings);

            //construct the es client
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", config.getEsClusterName())
                    .build();


            String esServerAddr = config.getEsServer();
            Integer esPort = config.getEsPort();
            String esClusterName = config.getEsClusterName();

            Client transportClient = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress
                    (InetAddress.getByName(esServerAddr), esPort));

//            Client transportClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(esServerAddr, esPort));
            IndicesAdminClient indicesAdmin = transportClient.admin().indices();

            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> f =
                    indicesAdmin.getMappings(new GetMappingsRequest()).actionGet().getMappings();

            Map<String, List<String>> indexTypeMapping = new HashMap<String, List<String>>();
            List<String> typeList = null;
            List<ElasticsearchTable> tblList = null;

            //index的名陈列表
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

            transportClient.close();

            initialized = 1;
        }

        return ImmutableMap.copyOf(
                transformValues(
                        catalog,
                        resolveAndIndexTablesFunction()));
    }

    static Supplier<Map<String, Map<String, ElasticsearchTable>>> schemasSupplier(final JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec,
                                                                                  final String esServer,
                                                                                  final int esPort) {
        return () -> {
            try {
                return lookupSchemas(catalogCodec);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTablesFunction() {
        return tables -> ImmutableMap.copyOf(
                uniqueIndex(
                        transform(
                                tables,
                                table -> new ElasticsearchTable(table.getName(), table.getSources())),
                        ElasticsearchTable::getName));
    }

    static ImmutableMap<String, Client> createClients(Map<String, Map<String, ElasticsearchTable>> schemas) throws UnknownHostException {
        Map<String, Client> transportClients = new HashMap<String, Client>();


        for (String key : schemas.keySet()) {
            Map<String, ElasticsearchTable> tableMap = schemas.get(key);
            for (String tableName : tableMap.keySet()) {
                ElasticsearchTable elasticsearchTable = tableMap.get(tableName);
                List<ElasticsearchTableSource> tableSources = elasticsearchTable.getSources();
                for (ElasticsearchTableSource tableSource : tableSources) {
                    String clusterName = tableSource.getClusterName();
                    if (transportClients.get(clusterName) == null) {
                        Settings settings = Settings.settingsBuilder()
                                .put("cluster.name", clusterName)
                                .build();
                        Client transportClient = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress
                                (InetAddress.getByName(tableSource.getHostAddress()), tableSource.getPort()));
                        transportClients.put(clusterName, transportClient);
                    }
                }
            }
        }
        return ImmutableMap.copyOf(transportClients);
    }
}
