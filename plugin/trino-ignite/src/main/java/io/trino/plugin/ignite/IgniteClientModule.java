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
package io.trino.plugin.ignite;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import org.apache.ignite.IgniteJdbcThinDriver;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class IgniteClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class)
                .annotatedWith(ForBaseJdbc.class)
                .to(IgniteClient.class)
                .in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(IgniteConfig.class);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, IgniteConfig igniteConfig)
    {
        Properties connectionProperties = new Properties();
        if (igniteConfig.isLazy()) {
            connectionProperties.setProperty("isLazy", String.valueOf(igniteConfig.isLazy()));
        }
        return new DriverConnectionFactory(
                new IgniteJdbcThinDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
