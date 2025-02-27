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

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific implementation of {@link ConnectorSplitManager}.
 */
public class RedisSplitManager
        implements ConnectorSplitManager
{
    private final RedisConnectorConfig redisConnectorConfig;
    private final RedisJedisManager jedisManager;

    private static final long REDIS_MAX_SPLITS = 100;
    private static final long REDIS_STRIDE_SPLITS = 100;

    @Inject
    public RedisSplitManager(
            RedisConnectorConfig redisConnectorConfig,
            RedisJedisManager jedisManager)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        RedisTableHandle redisTableHandle = (RedisTableHandle) table;

        List<HostAddress> nodes = new ArrayList<>(redisConnectorConfig.getNodes());
        Collections.shuffle(nodes);

        checkState(!nodes.isEmpty(), "No Redis nodes available");
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        long numberOfKeys = 1;
        // when Redis keys are provides in a zset, create multiple
        // splits by splitting zset in chunks
        if (redisTableHandle.getKeyDataFormat().equals("zset")) {
            try (Jedis jedis = jedisManager.getJedisPool(nodes.get(0)).getResource()) {
                numberOfKeys = jedis.zcount(redisTableHandle.getKeyName(), "-inf", "+inf");
            }
        }

        long stride = REDIS_STRIDE_SPLITS;

        if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
            stride = numberOfKeys / REDIS_MAX_SPLITS;
        }

        for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
            long endIndex = startIndex + stride - 1;
            if (endIndex >= numberOfKeys) {
                endIndex = -1;
            }

            RedisSplit split = new RedisSplit(
                    redisTableHandle.getSchemaName(),
                    redisTableHandle.getTableName(),
                    redisTableHandle.getKeyDataFormat(),
                    redisTableHandle.getValueDataFormat(),
                    redisTableHandle.getKeyName(),
                    redisTableHandle.getConstraint(),
                    startIndex,
                    endIndex,
                    nodes);

            builder.add(split);
        }
        return new FixedSplitSource(builder.build());
    }
}
