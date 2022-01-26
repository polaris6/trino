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
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestDbCatalogConfigurationManager
{
    static H2CatalogDaoProvider setup(String prefix)
    {
        DbCatalogConfig config = new DbCatalogConfig().setCatalogConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime());
        return new H2CatalogDaoProvider(config);
    }

    @Test
    public void testDeleteCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("delete_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");

        DbCatalogConfigurationManager manager = new DbCatalogConfigurationManager(daoProvider.get());
        assertTrue(manager.getCatalog("catalog_mysql").isPresent());
        dao.deleteCatalog("catalog_mysql");
        assertThrows(IllegalStateException.class, () -> manager.getCatalog("catalog_mysql"));
    }

    @Test
    public void testGetCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("get_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");

        DbCatalogConfigurationManager manager = new DbCatalogConfigurationManager(daoProvider.get());
        Optional<Map<String, String>> properties = manager.getCatalog("catalog_mysql");
        assertTrue(properties.isPresent());
        assertEquals(properties.get().get("connector.name"), "mysql");
    }

    @Test
    public void testLoadCatalogs()
    {
        H2CatalogDaoProvider daoProvider = setup("load_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        dao.insertCatalog("catalog_mysql2", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3307\",\"connection-user\":\"test\",\"connection-password\":\"test\"}");

        DbCatalogConfigurationManager manager = new DbCatalogConfigurationManager(daoProvider.get());
        Map<String, Map<String, String>> catalogs = manager.loadCatalogs();
        assertEquals(catalogs.size(), 2);
        assertEquals(catalogs.get("catalog_mysql1"), ImmutableMap.of("connector.name", "mysql", "connection-url", "jdbc:mysql://localhost:3306", "connection-user", "user", "connection-password", "pwd"));
        assertEquals(catalogs.get("catalog_mysql2").get("connection-user"), "test");
    }
}
