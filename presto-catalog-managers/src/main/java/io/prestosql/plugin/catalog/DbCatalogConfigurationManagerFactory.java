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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.catalog.CatalogConfigurationManager;
import io.prestosql.spi.catalog.CatalogConfigurationManagerFactory;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class DbCatalogConfigurationManagerFactory
        implements CatalogConfigurationManagerFactory
{
    @Override
    public String getName()
    {
        return "db";
    }

    @Override
    public CatalogConfigurationManager create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new DbCatalogModule());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(DbCatalogConfigurationManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
