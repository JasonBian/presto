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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchTable
{
    private final String name;

    private List<ElasticsearchColumn> columns;
    private List<ElasticsearchColumnMetadata> columnsMetadata;
    private final List<ElasticsearchTableSource> sources;
    private static boolean columnReady = false;

    @JsonCreator
    public ElasticsearchTable(
            @JsonProperty("name") String name,
            //@JsonProperty("tupleDomain")
            @JsonProperty("sources") List<ElasticsearchTableSource> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ElasticsearchColumn> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ElasticsearchColumn> columns)
    {
        columnReady = true;
        this.columns = columns;
    }

    @JsonProperty
    public List<ElasticsearchTableSource> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        //List<? extends ColumnMetadata> bases = columnsMetadata;
        //return bases;

        //List<ColumnMetadata> lis = new ArrayList<ColumnMetadata>(columnsMetadata);
        //return lis;

        return new ArrayList<>(columnsMetadata);
    }

    public void setColumnsMetadata(List<ElasticsearchColumnMetadata> columnsMetadata)
    {
        this.columnsMetadata = columnsMetadata;
    }

    public boolean getColumnReady() {
        return columnReady;
    }
}
