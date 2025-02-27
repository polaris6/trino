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
package io.trino.plugin.redis;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class RedisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, RedisTableDescription>>
{
    private static final Logger log = Logger.get(RedisTableDescriptionSupplier.class);

    private final RedisConnectorConfig redisConnectorConfig;
    private final RedisTableConfigManager redisTableConfigManager;
    private final JsonCodec<RedisTableDescription> tableDescriptionCodec;
    private final boolean useConfigDb;

    @Inject
    RedisTableDescriptionSupplier(RedisConnectorConfig redisConnectorConfig, RedisTableConfigManager redisTableConfigManager, JsonCodec<RedisTableDescription> tableDescriptionCodec)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.redisTableConfigManager = requireNonNull(redisTableConfigManager, "redisTableConfigManager is null");
        this.tableDescriptionCodec = requireNonNull(tableDescriptionCodec, "tableDescriptionCodec is null");
        this.useConfigDb = redisConnectorConfig.isUseConfigDb();
    }

    @Override
    public Map<SchemaTableName, RedisTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> builder = ImmutableMap.builder();

        if (useConfigDb) {
            List<String> tablesConfigList = redisTableConfigManager.getTablesConfig(redisConnectorConfig.getClusterName());
            for (String tableConfig : tablesConfigList) {
                RedisTableDescription table = tableDescriptionCodec.fromJson(tableConfig.getBytes(UTF_8));
                String schemaName = table.getSchemaName();
                builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
            }
            return builder.buildOrThrow();
        }

        try {
            for (File file : listFiles(redisConnectorConfig.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    RedisTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), redisConnectorConfig.getDefaultSchema());
                    log.debug("Redis table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, RedisTableDescription> tableDefinitions = builder.buildOrThrow();

            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            for (String definedTable : redisConnectorConfig.getTableNames()) {
                SchemaTableName tableName;
                try {
                    tableName = parseTableName(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(redisConnectorConfig.getDefaultSchema(), definedTable);
                }

                if (!tableDefinitions.containsKey(tableName)) {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new RedisTableDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of()),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of())));
                }
            }

            return builder.buildOrThrow();
        }
        catch (IOException e) {
            log.warn(e, "Failed to get table description files for Redis");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
