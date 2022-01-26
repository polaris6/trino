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
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    public static final String CONNECTOR_NAME_KEY = "connector.name";
    private final ConnectorManager connectorManager;
    private final DynamicCatalogManager dynamicCatalogManager;

    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogManager dynamicCatalogManager)
    {
        this.connectorManager = connectorManager;
        this.dynamicCatalogManager = dynamicCatalogManager;
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        loadCatalogs(ImmutableMap.of());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        log.info("-- Loading dynamic catalog properties --");
        Map<String, Map<String, String>> catalogs = dynamicCatalogManager.getCatalogConfigurationManager().loadCatalogs();

        catalogs.forEach(this::loadCatalog);

        additionalCatalogs.forEach(this::loadCatalog);

        catalogsLoaded.set(true);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties)
    {
        log.info("-- Loading dynamic catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(CONNECTOR_NAME_KEY)) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for dynamic catalog %s does not contain connector.name", catalogName);

        connectorManager.createCatalog(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added dynamic catalog %s using connector %s --", catalogName, connectorName);
    }

    public void loadCatalog(String catalogName)
    {
        log.info("-- Loading dynamic catalog %s --", catalogName);

        Optional<Map<String, String>> properties = dynamicCatalogManager.getCatalogConfigurationManager().getCatalog(catalogName);
        checkState(properties.isPresent(), "Configuration for dynamic catalog %s does not exist", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.get().entrySet()) {
            if (entry.getKey().equals(CONNECTOR_NAME_KEY)) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for dynamic catalog %s does not contain connector.name", catalogName);

        connectorManager.createCatalog(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added dynamic catalog %s using connector %s --", catalogName, connectorName);
    }
}
