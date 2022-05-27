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

import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class TestingIgniteServer
        implements Closeable
{
    public static final int PORT = 10800;
    public static final String USER = "root";
    public static final String PASSWORD = "test|test";
    public static final String JDBC_URL = "jdbc:ignite:thin://localhost:" + PORT;
    private final GenericContainer<?> container;

    public TestingIgniteServer()
    {
        container = new GenericContainer<>("ignite:2.8.1")
                .withExposedPorts(PORT);
        container.start();
    }

    public void execute(String sql)
    {
        try (Connection connection = createConnection(JDBC_URL);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    private Connection createConnection(String url) throws ClassNotFoundException, SQLException
    {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        return DriverManager.getConnection(url, USER, PASSWORD);
    }

    @Override
    public void close() throws IOException
    {
        container.close();
    }
}
