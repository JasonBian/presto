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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Created by bianzexin on 16/9/18.
 */

public class ElasticsearchTableSource
{
    private String hostAddress;
    private int port;
    private String clusterName;
    private String index;
    private String type;

    @JsonCreator
    public ElasticsearchTableSource(
            @JsonProperty("hostAddress") String hostAddress,
            @JsonProperty("port") int port,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("index") String index,
            @JsonProperty("type") String type)
    {
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
        this.port = requireNonNull(port, "port is null");
        this.clusterName = requireNonNull(clusterName, "clusterName is null");
        this.index = index;
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getHostAddress()
    {
        return hostAddress;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public String getClusterName()
    {
        return clusterName;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }
}
