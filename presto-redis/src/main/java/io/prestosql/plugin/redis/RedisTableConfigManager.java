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

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public class RedisTableConfigManager
{
    private final RedisTableConfigDao redisTableConfigDao;

    @Inject
    public RedisTableConfigManager(RedisTableConfigDao redisTableConfigDao)
    {
        this.redisTableConfigDao = redisTableConfigDao;
    }

    public List<String> getTablesConfig(String clusterName)
    {
        List<String> tableConfigList = redisTableConfigDao.listTableConfig(clusterName);
        return ImmutableList.copyOf(tableConfigList);
    }
}
