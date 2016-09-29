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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.joda.time.*;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
//import org.joda.time.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(ElasticsearchRecordCursor.class);
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex;
    private Iterator<SearchHit> lines;
    private long totalBytes;
    private List<String> fields;
    private int shardId;
    private SearchResponse scrollResp;
    private Client client;
    private ElasticsearchSplit split;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchSplit split, ElasticsearchClient elasticsearchClient)
    {
        this.columnHandles = columnHandles;
        this.jsonPathToIndex = new HashMap();
        this.totalBytes = 0;
        this.split = split;
        ArrayList<String> fieldsNeeded = new ArrayList();

        for (int i = 0; i < columnHandles.size(); i++) {
            this.jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
            fieldsNeeded.add(columnHandles.get(i).getColumnJsonPath());
        }
        this.shardId = split.getShardId();

        this.lines = getRows(split.getUri(), fieldsNeeded, elasticsearchClient).iterator();
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {

        if (!lines.hasNext()) {

            scrollResp = client
                    .prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(6000)).execute().actionGet();

            if (scrollResp.getHits().getHits().length == 0) {
                return false;
            }

            List<SearchHit> result = new ArrayList<>();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                result.add(hit);
            }

            lines = result.iterator();
        }

        SearchHit hit = lines.next();

        fields = new ArrayList(Collections.nCopies(columnHandles.size(), "-1"));

        Map<String, SearchHitField> map = hit.getFields();
        for (Map.Entry<String, SearchHitField> entry : map.entrySet()) {
            String jsonPath = entry.getKey().toString();
            SearchHitField entryValue = entry.getValue();

            // we get the value, wrapped in a list (of size 1 of course) -> [value] (The java api returns in this way)
            ArrayList<Object> lis = new ArrayList(entryValue.getValues());
            String value = String.valueOf(lis.get(0));

            fields.set(jsonPathToIndex.get(jsonPath), value);
        }

        totalBytes += fields.size();

        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        String timeStr = getFieldValue(field);
        Long milliSec = 0L;
        if ( getType( field ) == TIMESTAMP ) {
            if ( timeStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}") ) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                try {
                    milliSec = sdf.parse(timeStr).getTime();
                }catch(Exception except){
                    log.error(except.getMessage());
                }
                return milliSec;
            }

            DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser();

            milliSec =  formatter.parseDateTime(timeStr).getMillis();
            return milliSec;
        }

        //suppose the
        return Math.round(Double.parseDouble(getFieldValue(field)));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }

    String[] getIndices(Client client, String type)
    {
        return Arrays.asList(client
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetaData()
                .concreteAllIndices())
                .stream()
                .filter(e -> e.startsWith(type.concat("_")))
                .toArray(size -> new String[size]);
    }

    List<SearchHit> getRows(ElasticsearchTableSource tableSource, ArrayList<String> fieldsNeeded, ElasticsearchClient elasticsearchClient)
    {
        List<SearchHit> result = new ArrayList<>();
        String clusterName = tableSource.getClusterName();
        String hostAddress = tableSource.getHostAddress();
        int port = tableSource.getPort();
        String index = tableSource.getIndex();
        String type = tableSource.getType();

        log.debug(format("Connecting to cluster %s from %s:%d, index %s, type %s", clusterName, hostAddress, port, index, type));


        //limit操作需要下推才行
        client = elasticsearchClient.getInternalClients().get(clusterName);
        QueryBuilder fb = prepareTermQuery(elasticsearchClient,split, fieldsNeeded);//default return bool null query
        int scrollSize  = 5000;
        String preferenceShard = "_shards:"+shardId;
        scrollResp = client
                .prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(fb)
                .setPreference(preferenceShard)
                .addFields(fieldsNeeded.toArray(new String[fieldsNeeded.size()]))
                .setScroll(new TimeValue(6000))
                .setSize(scrollSize).execute()
                .actionGet(); //20000 hits per shard will be returned for each scroll

        //要避免全表扫描
        //Scroll until no hits are returned
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                result.add(hit);
            }
            break;
        }

        return result;
    }

    String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }


    public QueryBuilder prepareTermQuery(ElasticsearchClient esClient,ElasticsearchSplit split, ArrayList<String> columnHandles)
    {
        List<QueryBuilder> allqb = new ArrayList<QueryBuilder>();
        QueryBuilder qb = null;
        Domain domain = null;

        if(split.getTupleDomain() == null)return null;
        for (ColumnHandle c : split.getTupleDomain().getDomains().get().keySet()) {
            ElasticsearchColumnHandle esch = (ElasticsearchColumnHandle)c;
            String columnName = esch.getColumnJsonPath();
            Type type = esch.getColumnType();
            domain = split.getTupleDomain().getDomains().get().get(c);

            /** start to fetch the datetype for timestamp
             *
             */
            String dateFormat = "";
            if ( type.equals(TimestampType.TIMESTAMP) ) {
                String indexName = split.getUri().getIndex();
                String typeName = split.getUri().getType();
                String fieldName = columnName;

                Client client = esClient.getInternalClients().get(split.getUri().getClusterName());
                GetFieldMappingsRequest fieldMappingRequest = (new GetFieldMappingsRequest())
                        .indices(indexName)
                        .types(typeName)
                        .fields(fieldName);
                GetFieldMappingsResponse fieldMappingsResponse = client.admin().indices().getFieldMappings(fieldMappingRequest).actionGet();
                String fetchField;

                if (fieldName.contains(".")) {
                    fetchField = fieldName.substring(fieldName.indexOf(".") + 1);
                } else
                    fetchField = fieldName;

                log.debug(format("fieldName: %s, fetchField:%s", fieldName, fetchField));
                if (!fieldMappingsResponse.mappings().get(indexName).get(typeName).get(fieldName).isNull()) {
                    log.debug(format("fetchField: %s",fetchField));
                    Map<String, Object> formatMap = (Map) fieldMappingsResponse.mappings().get(indexName).get(typeName).get(fieldName).sourceAsMap().get(fetchField);

                    if (formatMap.containsKey("format")) {
                        log.debug(format("fetchField:%s, format: %s", fetchField, formatMap.get("format")));
                        dateFormat = (String)formatMap.get("format");
                    }
                } else {
                    log.warn(format("%s",fieldMappingsResponse.mappings().get(indexName).get(typeName).get(fieldName)));
                }

                if ( dateFormat.contains("dateOptionalTime") ) {
                    /**
                     * GET flightagg_demo/_search?size=1
                     {
                     "fields": ["InboundDate"],
                     "query":{
                     "constant_score": {
                     "filter": {
                     "exists": {
                     "field": "InboundDate"
                     }
                     }
                     }
                     }
                     }
                     */
                    //new ConstantScoreQueryBuilder(FilterBuilders.existsFilter(columnName))
                    QueryBuilder existQuery = QueryBuilders.constantScoreQuery(FilterBuilders.existsFilter(columnName));

                    SearchResponse searchResp =
                            client.prepareSearch().setIndices(indexName).setTypes(typeName)
                                    .setSize(1)
                                    .setFetchSource(columnName,null)
                                    .execute().actionGet();

                    Iterator iter = searchResp.getHits().iterator();
                    String dateSampleValue = "";
                    while  ( iter.hasNext() ) {
                        SearchHit hit = (SearchHit)iter.next();
                        dateSampleValue = (String)hit.sourceAsMap().get(columnName);
                    }

                    log.debug(format("dateSampleValue: %s",dateSampleValue));

                    if ( dateSampleValue.matches("\\d{4}-\\d{2}-\\d{2}") ) {
                        dateFormat = "YYYY-MM-dd";
                    }else{
                        dateFormat = "YYYY-MM-dd'T'HH:mm:ssZ";
                    }
                }
            }
            //end of fetch date format

            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

            if (domain.getValues().isNone()) {
                return QueryBuilders.filteredQuery(null, FilterBuilders.missingFilter(columnName));
                //return domain.isNullAllowed() ? columnName + " IS NULL" : "FALSE";
            }

            if (domain.getValues().isAll()) {
                return QueryBuilders.filteredQuery(null, FilterBuilders.existsFilter(columnName));
                //return domain.isNullAllowed() ? "TRUE" : columnName + " IS NOT NULL";
            }

            List<Object> singleValues = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else
                {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:

                                if ( type == TimestampType.TIMESTAMP ) {
                                    //fetch field mapping firstly
                                    SimpleDateFormat sdf = null;
                                    sdf = new SimpleDateFormat(dateFormat);
                                    String endTime = sdf.format(new Date(Long.valueOf(range.getLow().getValue().toString())));

                                    String jsonQueryTemplate = " {\"range\": { \"%s\": {\"from\": \"%s\", \"format\":\"%s\" } } } ";
                                    String jsonQuery = String.format(jsonQueryTemplate, columnName, endTime, dateFormat);

                                    //qb = QueryBuilders.rangeQuery(columnName).to(endTime);
                                    qb = QueryBuilders.wrapperQuery(jsonQuery);
                                    log.debug((jsonQueryTemplate));
                                }else
                                    qb = QueryBuilders.rangeQuery(columnName).lt(range.getHigh().getValue());

                                allqb.add(qb);
                                break;
                            case EXACTLY:
                                qb = QueryBuilders.rangeQuery(columnName).gte(range.getLow().getValue());
                                allqb.add(qb);
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                //qb =  (qb != null) ? QueryBuilders.rangeQuery(columnName).from(range.getLow().getValue()).lte(range.getHigh().getValue()) : QueryBuilders.rangeQuery(columnName).lte(range.getHigh().getValue());
                                qb =  QueryBuilders.rangeQuery(columnName).lte(range.getHigh().getValue());
                                allqb.add(qb);
                                break;
                            case BELOW:
                                //qb =  (qb != null) ? QueryBuilders.rangeQuery(columnName).from(range.getLow().getValue()).lt(range.getHigh().getValue()) : QueryBuilders.rangeQuery(columnName).lt(range.getHigh().getValue());
                                if ( type == TimestampType.TIMESTAMP ) {
                                    //fetch field mapping firstly
                                    //GET fltonline-20160623/onlinelog/_mapping/field/stored.LogTime
                                    //esch.getColumnMetadata().
                                    //esch.get
                                    SimpleDateFormat sdf = null;
                                    sdf = new SimpleDateFormat(dateFormat);
                                    String endTime = sdf.format(new Date(Long.valueOf(range.getHigh().getValue().toString())));

                                    String jsonQueryTemplate = " {\"range\": { \"%s\": {\"to\": \"%s\", \"format\":\"%s\" } } } ";
                                    String jsonQuery = String.format(jsonQueryTemplate, columnName, endTime, dateFormat);

                                    //qb = QueryBuilders.rangeQuery(columnName).to(endTime);
                                    qb = QueryBuilders.wrapperQuery(jsonQuery);
                                }else
                                    qb = QueryBuilders.rangeQuery(columnName).lt(range.getHigh().getValue());

                                allqb.add(qb);

                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                if(type.equals(VarcharType.VARCHAR)) {
                    qb = QueryBuilders.matchPhraseQuery(columnName, ((Slice)singleValues.get(0)).toStringUtf8());
                    //qb = QueryBuilders.matchQuery(columnName, ((Slice)singleValues.get(0)).toStringUtf8());
                }
                else if(type.equals(IntegerType.INTEGER))
                {
                    qb = QueryBuilders.termQuery(columnName, (Integer)(singleValues.get(0)));
                }
                else if(type.equals(BigintType.BIGINT))
                {
                    qb = QueryBuilders.termQuery(columnName, (Long)((singleValues.get(0))));
                }
                else if(type.equals(BooleanType.BOOLEAN))
                {
                    qb = QueryBuilders.termQuery(columnName, (Boolean)((singleValues.get(0))));
                }
                else if(type.equals(TimestampType.TIMESTAMP))
                {
                    SimpleDateFormat sdf = null;
                    sdf = new SimpleDateFormat(dateFormat);
                    Long startTimeVal  = (long)(singleValues.get(0)) - 5000;
                    Long endTimeVal = (long)(singleValues.get(0)) + 5000;
                    String startTime = sdf.format(new Date(startTimeVal));
                    String endTime = sdf.format(new Date(endTimeVal));

                    String jsonQueryTemplate = " {\"range\": { \"%s\": {\"from\": \"%s\", \"to\": \"%s\", \"format\":\"%s\" } } } ";
                    String jsonQuery = String.format(jsonQueryTemplate, columnName, startTime, endTime, dateFormat);

                    qb = QueryBuilders.wrapperQuery(jsonQuery);
                }
                allqb.add(qb);
            }
            else if (singleValues.size() > 1) {
                String s = "";
                if(type.equals(VarcharType.VARCHAR))
                {
                    s = singleValues.stream().map((a)->
                    {
                        return ((Slice) a).toStringUtf8();
                    }).collect(Collectors.joining(" "));

                    qb = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery(columnName, s));
                    //qb = QueryBuilders.termsQuery(columnName, s).minimumMatch(1);//not ok
                }
                else if(type.equals(TimestampType.TIMESTAMP))
                {
                    Iterator iter = singleValues.iterator();
                    List<QueryBuilder> subQuery = new ArrayList<QueryBuilder>();
                    while ( iter.hasNext()) {
                        long milliSec = (long)iter.next();

                        SimpleDateFormat sdf = null;
                        sdf = new SimpleDateFormat(dateFormat);
                        Long startTimeVal  = milliSec - 5000;
                        Long endTimeVal = milliSec + 5000;
                        String startTime = sdf.format(new Date(startTimeVal));
                        String endTime = sdf.format(new Date(endTimeVal));

                        String jsonQueryTemplate = " {\"range\": { \"%s\": {\"from\": \"%s\", \"to\": \"%s\", \"format\":\"%s\" } } } ";
                        String jsonQuery = String.format(jsonQueryTemplate, columnName, startTime, endTime, dateFormat);

                        qb = QueryBuilders.wrapperQuery(jsonQuery);

                        subQuery.add(qb);
                    }

                    BoolQueryBuilder bqb = QueryBuilders.boolQuery();
                    for(QueryBuilder sqb : subQuery)
                    {
                        bqb.should(sqb);
                    }
                    qb = bqb.minimumNumberShouldMatch(1);
                }
                else
                {
                    qb = QueryBuilders.termsQuery(columnName, singleValues.stream().map((a) ->
                    {
                        if (type.equals(IntegerType.INTEGER)) {
                            return (Integer) a;
                        } else if (type.equals(BigintType.BIGINT)) {
                            return (Long) a;
                        } else if (type.equals(BooleanType.BOOLEAN)) {
                            return (Boolean) a;
                        }else {
                            return ((Slice) a).toStringUtf8();
                        }
                    }).toArray());
                }
                allqb.add(qb);
            }
        }

        BoolQueryBuilder sum = QueryBuilders.boolQuery();
        for(QueryBuilder qbb : allqb)
            sum.must(qbb);

        return sum;
    }
}