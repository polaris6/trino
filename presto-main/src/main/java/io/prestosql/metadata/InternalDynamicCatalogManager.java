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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.server.Server;
import io.prestosql.spi.catalog.CatalogConfigurationManager;
import io.prestosql.spi.catalog.CatalogConfigurationManagerFactory;

import javax.inject.Inject;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.prestosql.metadata.DynamicCatalogStore.CONNECTOR_NAME_KEY;
import static io.prestosql.server.Server.ConnectorAction.ADD;
import static io.prestosql.server.Server.ConnectorAction.DELETE;
import static io.prestosql.server.Server.ConnectorAction.MODIFY;
import static io.prestosql.server.Server.updateConnectorIds;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
//import static io.prestosql.util.PropertiesUtil.loadProperties;

public final class InternalDynamicCatalogManager
        implements DynamicCatalogManager
{
    private static final Logger log = Logger.get(InternalDynamicCatalogManager.class);
    private static final File CATALOG_CONFIGURATION = new File("etc/catalog.properties");
    private static final String CONFIGURATION_MANAGER_PROPERTY_NAME = "catalog.configuration-manager";
    private final ConcurrentMap<String, CatalogConfigurationManagerFactory> configurationManagerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<CatalogConfigurationManager> configurationManager = new AtomicReference<>();
    private final CatalogManager catalogManager;
    private final ConnectorManager connectorManager;
    private final DiscoveryNodeManager discoveryNodeManager;
    private final HttpClient httpClient;

    @Inject
    public InternalDynamicCatalogManager(
            CatalogManager catalogManager,
            ConnectorManager connectorManager,
            DiscoveryNodeManager discoveryNodeManager,
            @ForNodeManager HttpClient httpClient)
    {
        this.catalogManager = requireNonNull(catalogManager, "connectorManager is null");
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.discoveryNodeManager = requireNonNull(discoveryNodeManager, "discoveryNodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public void addCatalogConfigurationManagerFactory(CatalogConfigurationManagerFactory factory)
    {
        if (configurationManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Catalog configuration manager '%s' is already registered", factory.getName()));
        }
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        if (CATALOG_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(CATALOG_CONFIGURATION.getPath()));

            String configurationManagerName = properties.remove(CONFIGURATION_MANAGER_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(configurationManagerName),
                    "Catalog configuration %s does not contain %s", CATALOG_CONFIGURATION.getAbsoluteFile(), CONFIGURATION_MANAGER_PROPERTY_NAME);

            setConfigurationManager(configurationManagerName, properties);
        }
    }

    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading catalog configuration manager --");

        CatalogConfigurationManagerFactory configurationManagerFactory = configurationManagerFactories.get(name);
        checkState(configurationManagerFactory != null, "Catalog configuration manager %s is not registered", name);

        CatalogConfigurationManager configurationManager = configurationManagerFactory.create(ImmutableMap.copyOf(properties));

        this.configurationManager.set(requireNonNull(configurationManager, "configurationManager is null"));

        log.info("-- Loaded catalog configuration manager %s --", name);
    }

    @Override
    public CatalogConfigurationManager getCatalogConfigurationManager()
    {
        checkState(configurationManager.get() != null, "configurationManager was not loaded");
        return configurationManager.get();
    }

    @Override
    public void addCatalog(String catalogName, Map<String, String> properties)
    {
        log.info("-- add dynamic catalog %s --", catalogName);
        checkArgument(!catalogManager.getCatalog(catalogName).isPresent(), "A catalog already exists for %s", catalogName);
        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(CONNECTOR_NAME_KEY)) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }
        checkState(connectorName != null, "Configuration for dynamic catalog %s does not contain connector.name", catalogName);
        configurationManager.get().addCatalog(catalogName, properties);
        connectorManager.createCatalog(catalogName, connectorName, connectorProperties.build());
        //connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added dynamic catalog %s using connector %s --", catalogName, connectorName);
        updateConnectorIds(catalogName, ADD);
        dispatchNodes(catalogName, ADD);
    }

    @Override
    public void updateCatalog(String catalogName, Map<String, String> properties)
    {
        log.info("-- update dynamic catalog %s --", catalogName);
        checkArgument(catalogManager.getCatalog(catalogName).isPresent(), "A catalog does not exists for %s", catalogName);
        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(CONNECTOR_NAME_KEY)) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }
        checkState(connectorName != null, "Configuration for dynamic catalog %s does not contain connector.name", catalogName);
        configurationManager.get().updateCatalog(catalogName, properties);
        connectorManager.dropConnection(catalogName);
        connectorManager.createCatalog(catalogName, connectorName, connectorProperties.build());
        //connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        dispatchNodes(catalogName, MODIFY);
    }

    @Override
    public void deleteCatalog(String catalogName)
    {
        configurationManager.get().deleteCatalog(catalogName);
        connectorManager.dropConnection(catalogName);
        updateConnectorIds(catalogName, DELETE);
        dispatchNodes(catalogName, DELETE);
    }

    @Override
    public Optional<Map<String, String>> getCatalog(String catalogName)
    {
        return configurationManager.get().getCatalog(catalogName);
    }

    private void dispatchNodes(String catalogName, Server.ConnectorAction action)
    {
        if (!Server.isCoordinator()) {
            return;
        }
        for (InternalNode internalNode : discoveryNodeManager.getNodes(NodeState.ACTIVE)) {
            if (internalNode.isCoordinator()) {
                continue;
            }
            String nodeUri = internalNode.getHttpUri().toString();
            URI uri = URI.create(nodeUri + "/v1/catalog/" + catalogName + "/");
            Request.Builder requestBuilder = null;
            switch (action) {
                case ADD: requestBuilder = preparePost();
                    break;
                case MODIFY:
                    requestBuilder = preparePut();
                    break;
                case DELETE:
                    requestBuilder = prepareDelete();
                    break;
            }
            requestBuilder.setUri(uri).setHeader("User-Agent", internalNode.getNodeIdentifier());
            httpClient.executeAsync(requestBuilder.build(), new ResponseHandler<Object, Exception>()
            {
                @Override
                public Object handleException(Request request, Exception exception)
                {
                    log.error(exception, "update node %s failed, action: %s", internalNode.getHostAndPort(), action.name());
                    return null;
                }

                @Override
                public Object handle(Request request, Response response)
                {
                    log.info("update node %s success, action: %s", internalNode.getHostAndPort(), action.name());
                    return null;
                }
            });
        }
    }
}
