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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;


import static java.util.Objects.requireNonNull;

/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ElasticsearchTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;


    @JsonCreator
    public ElasticsearchTableLayoutHandle(
            @JsonProperty("table") ElasticsearchTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public ElasticsearchTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }


    @Override
    public String toString()
    {
        return table.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ElasticsearchTableLayoutHandle that = (ElasticsearchTableLayoutHandle) obj;
        return Objects.equals(table, that.table) && Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, tupleDomain);
    }
}
