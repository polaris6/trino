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

import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface H2CatalogDao
        extends CatalogDao
{
    @SqlUpdate("CREATE TABLE `presto_catalog_config` (\n" +
            "  `catalog_name` varchar(256) NOT NULL COMMENT 'catalog名称，唯一标示',\n" +
            "  `properties` varchar(512) NOT NULL COMMENT '配置信息',\n" +
            "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
            "  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\n" +
            "  PRIMARY KEY (`catalog_name`)\n" +
            ")")
    void createDynamicCatalogTable();
}
