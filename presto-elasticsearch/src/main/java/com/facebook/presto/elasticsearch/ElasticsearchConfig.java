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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchConfig {
    private String esServer;
    private String clusterName;
    private int esPort;

    @NotNull
    public String getEsServer() { return esServer;}

    @NotNull
    public String getEsClusterName() {
        return clusterName;
    }

    @NotNull
    public int getEsPort() {
        return esPort;
    }

    @Config("elasticsearch-server")
    public ElasticsearchConfig setEsServer(String nodes) {
        this.esServer = nodes;
        return this;
    }

    @Config("elasticsearch-port")
    public ElasticsearchConfig setEsPort(String port) {
        this.esPort = Integer.valueOf(port);
        return this;
    }

    @Config("elasticsearch-clustername")
    public ElasticsearchConfig setEsClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }
}
