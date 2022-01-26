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

import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestCatalogDao
{
    static H2CatalogDaoProvider setup(String prefix)
    {
        DbCatalogConfig config = new DbCatalogConfig().setCatalogConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime());
        return new H2CatalogDaoProvider(config);
    }

    @Test
    public void testUpdateCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("update_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        assertEquals(dao.getCatalogByCatalogName("catalog_mysql1"), "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        dao.updateCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"test\",\"connection-password\":\"test\"}");
        assertEquals(dao.getCatalogByCatalogName("catalog_mysql1"), "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"test\",\"connection-password\":\"test\"}");
    }

    @Test
    public void testDeleteCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("delete_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        dao.deleteCatalog("catalog_mysql1");
        assertNull(dao.getCatalogByCatalogName("catalog_mysql1"));
    }

    @Test
    public void testGetCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("get_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();
        // two catalog properties are the same except the catalogName
        dao.insertCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");

        // check catalog properties
        String properties = dao.getCatalogByCatalogName("catalog_mysql1");
        assertEquals(properties, "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
    }

    @Test
    public void testListCatalog()
    {
        H2CatalogDaoProvider daoProvider = setup("list_catalog");
        H2CatalogDao dao = daoProvider.get();
        dao.createDynamicCatalogTable();

        dao.insertCatalog("catalog_mysql1", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        dao.insertCatalog("catalog_mysql2", "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3307\",\"connection-user\":\"test\",\"connection-password\":\"test\"}");

        List<DbCatalogConfigurationManager.CatalogInfo> catalogInfo = dao.listCatalog();
        assertEquals(catalogInfo.size(), 2);
        assertEquals(catalogInfo.get(0).getCatalog(), "catalog_mysql1");
        assertEquals(catalogInfo.get(0).getProperties(), "{\"connector.name\":\"mysql\",\"connection-url\":\"jdbc:mysql://localhost:3306\",\"connection-user\":\"user\",\"connection-password\":\"pwd\"}");
        assertEquals(catalogInfo.get(1).getCatalog(), "catalog_mysql2");
    }
}
