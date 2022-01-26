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
package io.prestosql.plugin.catalog;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.catalog.CatalogConfigurationManager;

import java.util.Map;
import java.util.Optional;

public class NoOpCatalogConfigurationManager
        implements CatalogConfigurationManager
{
    @Override
    public Map<String, Map<String, String>> loadCatalogs()
    {
        return ImmutableMap.of();
    }

    @Override
    public void addCatalog(String catalogName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCatalog(String catalogName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteCatalog(String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Map<String, String>> getCatalog(String catalogName)
    {
        return Optional.empty();
    }
}
