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
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestDbCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(DbCatalogConfig.class)
                .setCatalogConfigDbUrl(null)
                .setCatalogConfigDbUser(null)
                .setCatalogConfigDbPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("catalog.config-db-url", "jdbc:mysql//localhost:3306/config?user=presto_admin")
                .put("catalog.config-db-user", "user")
                .put("catalog.config-db-password", "password")
                .build();
        DbCatalogConfig expected = new DbCatalogConfig()
                .setCatalogConfigDbUrl("jdbc:mysql//localhost:3306/config?user=presto_admin")
                .setCatalogConfigDbUser("user")
                .setCatalogConfigDbPassword("password");

        assertFullMapping(properties, expected);
    }
}
