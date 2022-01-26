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

import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

public interface CatalogDao
{
    @SqlUpdate("INSERT INTO presto_catalog_config (catalog_name, properties)\n" +
            "VALUES (:catalogName, :properties)")
    int insertCatalog(@Bind("catalogName") String catalogName, @Bind("properties") String properties);

    @SqlUpdate("UPDATE presto_catalog_config SET\n" +
            "  properties = :properties\n" +
            "WHERE catalog_name = :catalogName")
    int updateCatalog(@Bind("catalogName") String catalogName, @Bind("properties") String properties);

    @SqlUpdate("DELETE FROM presto_catalog_config\n" +
            "WHERE catalog_name = :catalogName")
    int deleteCatalog(@Bind("catalogName") String catalogName);

    @SqlQuery("SELECT properties\n" +
            "FROM presto_catalog_config\n" +
            "WHERE catalog_name = :catalogName")
    String getCatalogByCatalogName(@Bind("catalogName") String catalogName);

    @SqlQuery("SELECT catalog_name,properties\n" +
            "FROM presto_catalog_config")
    @RegisterConstructorMapper(DbCatalogConfigurationManager.CatalogInfo.class)
    List<DbCatalogConfigurationManager.CatalogInfo> listCatalog();
}
