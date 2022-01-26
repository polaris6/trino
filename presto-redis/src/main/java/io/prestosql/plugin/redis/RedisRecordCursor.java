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
package io.prestosql.plugin.redis;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.Type;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

public class RedisRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(RedisRecordCursor.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final RedisSplit split;
    private final List<RedisColumnHandle> columnHandles;
    private final RedisJedisManager redisJedisManager;
    private final JedisPool jedisPool;
    private final ScanParams scanParms;

    private ScanResult<String> redisCursor;
    private List<String> keys;

    private final AtomicBoolean reported = new AtomicBoolean();

    private List<String> values;
    private Map<String, String> valueMap;

    private long totalBytes;
    private long totalValues;

    private final Queue<FieldValueProvider[]> currentMultipleRows;

    private long getDataNum;
    private long getDataTime;

    RedisRecordCursor(
            RowDecoder keyDecoder,
            RowDecoder valueDecoder,
            RedisSplit split,
            List<RedisColumnHandle> columnHandles,
            RedisJedisManager redisJedisManager)
    {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.split = split;
        this.columnHandles = columnHandles;
        this.redisJedisManager = redisJedisManager;
        this.jedisPool = redisJedisManager.getJedisPool(split.getNodes().get(0));
        this.scanParms = setScanParms();
        // this.currentRowValues = new FieldValueProvider[columnHandles.size()];
        this.currentMultipleRows = new LinkedList<>();
        this.getDataNum = 0;
        this.getDataTime = 0;
        this.keys = new ArrayList<>();
//        this.values = new ArrayList<>();

        fetchKeys();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    public boolean hasUnscannedData()
    {
        if (redisCursor == null) {
            // return false;
            return true;
        }
        // no more keys are unscanned when
        // when redis scan command
        // returns 0 string cursor
        return (!redisCursor.getStringCursor().equals("0"));
    }

    @Override
    public boolean advanceNextPosition()
    {
        log.info("advanceNextPosition_before: %s", currentMultipleRows.size());
        currentMultipleRows.poll();
        log.info("advanceNextPosition_after: %s", currentMultipleRows.size());
        if (currentMultipleRows.isEmpty()) {
            while (keys.isEmpty()) {
                if (!hasUnscannedData()) {
                    return endOfData();
                }
                long before = System.currentTimeMillis();
                fetchKeys();
                log.info("fetchKeysTime: %s", System.currentTimeMillis() - before);
            }
            return nextRow();
        }
        else {
            return true;
        }
//        getDataNum++;
//        long start = System.currentTimeMillis();
//        boolean nextRow = nextRow(keys);
//        long end = System.currentTimeMillis();
//        getDataTime += end - start;
//        log.info("getDataNum: %s, getDataTime: %s", getDataNum, getDataTime);
//        return nextRow;
    }

    private boolean endOfData()
    {
        if (!reported.getAndSet(true)) {
            log.debug("Read a total of %d values with %d bytes.", totalValues, totalBytes);
        }
        return false;
    }

    private boolean nextRow()
    {
        long fetchDataTime = System.currentTimeMillis();
        fetchData();
        log.info("fetchDataTime: %s", System.currentTimeMillis() - fetchDataTime);

        for (int i = 0; i < keys.size(); i++) {
            String keyString = keys.get(i);
            String valueString = values.get(i);
            byte[] keyData = keyString.getBytes(StandardCharsets.UTF_8);

            byte[] valueData = EMPTY_BYTE_ARRAY;
            if (valueString != null) {
                valueData = valueString.getBytes(StandardCharsets.UTF_8);
            }

            totalBytes += valueData.length;
            totalValues++;

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = valueDecoder.decodeRow(
                    valueData,
                    valueMap);

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    RedisInternalFieldDescription fieldDescription = RedisInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case KEY_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                            break;
                        case VALUE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(valueData));
                            break;
                        case KEY_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                            break;
                        case VALUE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(valueData.length));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedKey.isEmpty()));
                            break;
                        case VALUE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(decodedValue.isEmpty()));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            FieldValueProvider[] fieldValues = new FieldValueProvider[columnHandles.size()];
            for (int j = 0; j < columnHandles.size(); j++) {
                ColumnHandle columnHandle = columnHandles.get(j);
                fieldValues[j] = currentRowValuesMap.get(columnHandle);
            }
            currentMultipleRows.offer(fieldValues);
        }
        keys.clear();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return getFieldValueProvider(field, boolean.class).getBoolean();
    }

    @Override
    public long getLong(int field)
    {
        return getFieldValueProvider(field, long.class).getLong();
    }

    @Override
    public double getDouble(int field)
    {
        return getFieldValueProvider(field, double.class).getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        return getFieldValueProvider(field, Slice.class).getSlice();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentMultipleRows.peek() == null || currentMultipleRows.peek()[field].isNull();
    }

    @Override
    public Object getObject(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        throw new IllegalArgumentException(format("Type %s is not supported", getType(field)));
    }

    private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        checkFieldType(field, expectedType);
        return requireNonNull(currentMultipleRows.peek())[field];
    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }

    private ScanParams setScanParms()
    {
        if (split.getKeyDataType() == RedisDataType.STRING) {
            ScanParams scanParms = new ScanParams();
            scanParms.count(redisJedisManager.getRedisConnectorConfig().getRedisScanCount());

            // when Redis key string follows "schema:table:*" format
            // scan command can efficiently query tables
            // by returning matching keys
            // the alternative is to set key-prefix-schema-table to false
            // and treat entire redis as single schema , single table
            // redis Hash/Set types are to be supported - they can also be
            // used to filter out table data

            // "default" schema is not prefixed to the key

            if (redisJedisManager.getRedisConnectorConfig().isKeyPrefixSchemaTable()) {
                String keyMatch = "";
                if (!split.getSchemaName().equals("default")) {
                    keyMatch = split.getSchemaName() + Character.toString(redisJedisManager.getRedisConnectorConfig().getRedisKeyDelimiter());
                }
                keyMatch = keyMatch + split.getTableName() + Character.toString(redisJedisManager.getRedisConnectorConfig().getRedisKeyDelimiter()) + "*";
                scanParms.match(keyMatch);
            }
            return scanParms;
        }

        return null;
    }

    // Redis keys can be contained in the user-provided ZSET
    // Otherwise they need to be found by scanning Redis
    private boolean fetchKeys()
    {
        try (Jedis jedis = jedisPool.getResource()) {
            switch (split.getKeyDataType()) {
                case STRING: {
                    String cursor = SCAN_POINTER_START;
                    if (redisCursor != null) {
                        cursor = redisCursor.getStringCursor();
                    }

                    keys.clear();
                    log.info("Scanning new Redis keys from cursor %s . %d values read so far", cursor, totalValues);
                    redisCursor = jedis.scan(cursor, scanParms);
                    log.info("keys_before: %s", Arrays.toString(keys.toArray()));
                    keys = redisCursor.getResult();
                    log.info("keys_after: %s", Arrays.toString(keys.toArray()));
                }
                break;
                case ZSET:
                    keys = (List<String>) jedis.zrange(split.getKeyName(), split.getStart(), split.getEnd());
                    break;
                default:
                    log.info("Redis type of key %s is unsupported", split.getKeyDataFormat());
                    return false;
            }
        }
        return true;
    }

    private boolean fetchData()
    {
        values = null;
        // values.clear();
        valueMap = null;
        // Redis connector supports two types of Redis
        // values: STRING and HASH
        // HASH types requires hash row decoder to
        // fill in the columns
        // whereas for the STRING type decoders are optional
        try (Jedis jedis = jedisPool.getResource()) {
            switch (split.getValueDataType()) {
                case STRING:
                    if (values != null) {
                        log.info("values_before: %s", Arrays.toString(values.toArray()));
                    }
                    values = jedis.mget(keys.toArray(new String[0]));
                    log.info("values_after_size: %s", values.size());
                    if (values == null) {
                        log.info("Redis data modified while query was running, string value at key %s deleted", keys.get(0));
                        return false;
                    }
                    break;
//                case HASH:
//                    valueMap = jedis.hgetAll(keyString);
//                    if (valueMap == null) {
//                        log.warn("Redis data modified while query was running, hash value at key %s deleted", keyString);
//                        return false;
//                    }
//                    break;
                default:
                    log.info("Redis type for key %s is unsupported", keys.get(0));
                    return false;
            }
        }
        return true;
    }
}
