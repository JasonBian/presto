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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Created by bianzexin on 16/9/18.
 */
public class ElasticsearchConnectorFactory implements ConnectorFactory {

    private final String name;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final TypeManager typeManager;

    public ElasticsearchConnectorFactory(TypeManager typeManager, String name, Map<String, String> optionalConfig, ClassLoader classLoader) {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.typeManager = typeManager;
        this.name = name;
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName() {
        return "elasticsearch";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new ElasticsearchHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");
        requireNonNull(optionalConfig, "optionalConfig is null");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new JsonModule(), new ElasticsearchModule(connectorId, typeManager));
            Injector injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig).initialize();

            return injector.getInstance(ElasticsearchConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
