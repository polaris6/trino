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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.metadata.DynamicCatalogStore.CONNECTOR_NAME_KEY;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogManager
{
    private final ConcurrentMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Map<String, String>> catalogInfos = new ConcurrentHashMap<>();

    public synchronized void registerCatalog(Catalog catalog)
    {
        requireNonNull(catalog, "catalog is null");

        checkState(catalogs.put(catalog.getCatalogName(), catalog) == null, "Catalog '%s' is already registered", catalog.getCatalogName());
    }

    public Optional<CatalogName> removeCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.remove(catalogName))
                .map(Catalog::getConnectorCatalogName);
    }

    public List<Catalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    public Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.get(catalogName));
    }

    public void registerCatalogInfo(String catalogName, String connectorName, Map<String, String> properties)
    {
        checkState(catalogs.containsKey(catalogName), "Catalog '%s' is not exist", catalogName);
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(catalogName, "connectorName is null");
        requireNonNull(properties, "properties is null");
        ImmutableMap.Builder<String, String> props = ImmutableMap.builder();
        props.put(CONNECTOR_NAME_KEY, connectorName);
        props.putAll(properties);
        Map<String, String> m = props.build();
        catalogInfos.put(catalogName, m);
    }

    public void removeCatalogInfo(String catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        catalogInfos.remove(catalogName);
    }

    public Optional<Map<String, String>> getCatalogInfo(String catalogName)
    {
        return Optional.ofNullable(catalogInfos.get(catalogName));
    }
}
