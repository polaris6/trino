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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestDeltaLakeDatabricksCreateTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCanReadInitialCreateTable()
    {
        String tableName = "test_dl_create_table_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) with (location = 's3://%s/%s')",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM default." + tableName)).contains(row(0));
            String showCreateTable = format(
                    "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\nLOCATION 's3://%s/%s'\n",
                    tableName,
                    bucketName,
                    tableDirectory);
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE default." + tableName))
                    .containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of());
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCanReadInitialCreatePartitionedTable()
    {
        String tableName = "test_dl_create_table_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(
                format("CREATE TABLE delta.default.%s (integer int, string varchar, timetz timestamp with time zone) " +
                                "with (location = 's3://%s/%s', partitioned_by = ARRAY['string'])",
                        tableName,
                        bucketName,
                        tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM " + tableName)).contains(row(0));
            String showCreateTable = format(
                    "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\n" +
                            "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n",
                    tableName,
                    bucketName,
                    tableDirectory);
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(tableName, ImmutableList.of());
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCanReadInitialCreateTableAs()
    {
        String tableName = "test_dl_create_table_as_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s') AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES FROM default LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM default." + tableName)).contains(row(3));
            String showCreateTable = format(
                    "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\nLOCATION 's3://%s/%s'\n",
                    tableName,
                    bucketName,
                    tableDirectory);
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE default." + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)));
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCanReadInitialCreatePartitionedTableAs()
    {
        String tableName = "test_dl_create_table_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (integer, string, timetz) with (location = 's3://%s/%s', partitioned_by = ARRAY['string']) AS " +
                        "VALUES (4, 'four', TIMESTAMP '2020-01-01 01:00:00.000 UTC'), (5, 'five', TIMESTAMP '2025-01-01 01:00:00.000 UTC'), (null, null, null)",
                tableName,
                bucketName,
                tableDirectory));

        try {
            assertThat(onDelta().executeQuery("SHOW TABLES LIKE '" + tableName + "'")).contains(row("default", tableName, false));
            assertThat(onDelta().executeQuery("SELECT count(*) FROM " + tableName)).contains(row(3));
            String showCreateTable = format(
                    "CREATE TABLE `default`.`%s` (\n  `integer` INT,\n  `string` STRING,\n  `timetz` TIMESTAMP)\nUSING DELTA\n" +
                            "PARTITIONED BY (string)\nLOCATION 's3://%s/%s'\n",
                    tableName,
                    bucketName,
                    tableDirectory);
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE " + tableName)).containsExactlyInOrder(row(showCreateTable));
            testInsert(
                    tableName,
                    ImmutableList.of(
                            row(4, "four", "2020-01-01T01:00:00.000Z"),
                            row(5, "five", "2025-01-01T01:00:00.000Z"),
                            row(null, null, null)));
        }
        finally {
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    private void testInsert(String tableName, List<QueryAssert.Row> existingRows)
    {
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'one', TIMESTAMP '2960-10-31 01:00:00')");
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 'two', TIMESTAMP '2020-10-31 01:00:00')");
        onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'three', TIMESTAMP '1900-10-31 01:00:00')");

        ImmutableList.Builder<QueryAssert.Row> expected = ImmutableList.builder();
        expected.addAll(existingRows);
        expected.add(row(1, "one", "2960-10-31T01:00:00.000Z"));
        expected.add(row(2, "two", "2020-10-31T01:00:00.000Z"));
        expected.add(row(3, "three", "1900-10-31T01:00:00.000Z"));

        assertThat(onTrino().executeQuery("SELECT integer, string, to_iso8601(timetz) FROM delta.default." + tableName))
                .containsOnly(expected.build());
    }
}
