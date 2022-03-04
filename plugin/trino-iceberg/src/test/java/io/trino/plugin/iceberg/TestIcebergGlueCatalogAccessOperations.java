package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.trino.Session;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.glue.GlueIcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.glue.TrinoGlueCatalogFactory;
import io.trino.spi.Plugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.GlueMetastoreMethod.CREATE_TABLE;
import static io.trino.plugin.iceberg.GlueMetastoreMethod.GET_DATABASE;
import static io.trino.plugin.iceberg.GlueMetastoreMethod.GET_TABLE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.lang.String.join;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

/*
 * TestIcebergGlueCatalogAccessOperations currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Test(singleThreaded = true)
public class TestIcebergGlueCatalogAccessOperations
        extends AbstractTestQueryFramework
{
    private static AtomicReference<GlueMetastoreStats> catalogStatsPin;
    private static AtomicReference<GlueMetastoreStats> tableOperationStatsPin;

    private final String schema = "test_schema_" + randomTableSuffix();
    private final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema(schema)
            .build();

    private GlueMetastoreStats catalogStats;
    private GlueMetastoreStats tableOperationStats;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tmp = Files.createTempDirectory("test_iceberg").toFile();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();
        Plugin plugin = new TestingIcebergPlugin(Optional.empty(), Optional.empty(), Optional.of(new StealStatsModule()));

        queryRunner.installPlugin(plugin);
        queryRunner.createCatalog("iceberg", "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.glue.default-warehouse-dir", tmp.getAbsolutePath()
                ));
        queryRunner.execute("CREATE SCHEMA " + schema);
        catalogStats = catalogStatsPin.get();
        tableOperationStats = tableOperationStatsPin.get();
        return queryRunner;
    }

    @Test
    public void testCreateTable()
    {
        try {
            assertMetastoreInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                    ImmutableMultiset.builder()
                            .add(CREATE_TABLE)
                            .add(GET_DATABASE)
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_create");
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        try {
            assertMetastoreInvocations("CREATE TABLE test_ctas AS SELECT 1 AS age",
                    ImmutableMultiset.builder()
                            .add(GET_DATABASE)
                            .add(CREATE_TABLE)
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_ctas");
        }
    }

    @Test
    public void testSelect()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");
            assertMetastoreInvocations("SELECT * FROM test_select_from",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from");
        }
    }

    @Test
    public void testSelectWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 as age", 1);

            assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_from_where");
        }
    }

    @Test(enabled = false)
    public void testSelectFromView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
            assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

            assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_view_table");
        }
    }

    @Test(enabled = false)
    public void testSelectFromViewWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 as age", 1);
            assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");
            assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP VIEW IF EXISTS test_select_view_where_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_view_where_view");
        }
    }

    @Test(enabled = false)
    public void testSelectFromMaterializedView()
    {
        try {
            assertUpdate("CREATE TABLE test_select_mview_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table");

            assertMetastoreInvocations("SELECT * FROM test_select_mview_view",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 3)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_mview_table");
        }
    }

    @Test(enabled = false)
    public void testSelectFromMaterializedViewWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_select_mview_where_table AS SELECT 2 as age", 1);
            assertUpdate("CREATE MATERIALIZED VIEW test_select_mview_where_view AS SELECT age FROM test_select_mview_where_table");

            assertMetastoreInvocations("SELECT * FROM test_select_mview_where_view WHERE age = 2",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 3)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_select_mview_where_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_mview_where_table");
        }
    }

    @Test(enabled = false)
    public void testRefreshMaterializedView()
    {
        try {
            assertUpdate("CREATE TABLE test_refresh_mview_table (id VARCHAR, age INT)");
            assertUpdate("CREATE MATERIALIZED VIEW test_refresh_mview_view AS SELECT id, age FROM test_refresh_mview_table");

            assertMetastoreInvocations("REFRESH MATERIALIZED VIEW test_refresh_mview_view",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 5)
                            //.addCopies(REPLACE_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP MATERIALIZED VIEW IF EXISTS test_refresh_mview_view");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_refresh_mview_table");
        }
    }

    @Test
    public void testJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
            assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

            assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_join_t1");
            getQueryRunner().execute("DROP TABLE IF EXISTS test_join_t2");
        }
    }

    @Test
    public void testSelfJoin()
    {
        try {
            assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 as age, 0 parent, 3 AS id", 1);

            assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_self_join_table");
        }
    }

    @Test
    public void testExplainSelect()
    {
        try {
            assertUpdate("CREATE TABLE test_explain AS SELECT 2 as age", 1);

            assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_explain");
        }
    }

    @Test
    public void testShowStatsForTable()
    {
        try {
            assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 as age", 1);

            assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_show_stats");
        }
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        try {
            assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 as age", 1);

            assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                    ImmutableMultiset.builder()
                            .add(GET_TABLE)
                            .build());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_show_stats_with_filter");
        }
    }

    @Test
    public void testSelectSystemTable()
    {
        try {
            assertUpdate("CREATE TABLE test_select_snapshots AS SELECT 2 AS age", 1);
            // select from $history
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$history\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $snapshots
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$snapshots\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $manifests
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$manifests\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $partitions
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$partitions\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $files
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$files\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // select from $properties
            assertMetastoreInvocations("SELECT * FROM \"test_select_snapshots$properties\"",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 1)
                            .build());

            // This test should get updated if a new system table is added.
            assertThat(TableType.values())
                    .containsExactly(DATA, HISTORY, SNAPSHOTS, MANIFESTS, PARTITIONS, FILES, PROPERTIES);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_select_snapshots");
        }
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        Map<GlueMetastoreMethod, Integer> startingCounts = Arrays.stream(GlueMetastoreMethod.values())
                .collect(Collectors.toMap(
                                Function.identity(),
                                method -> (int) method.getStatFrom(catalogStats).getTime().getAllTime().getCount() +
                                        (int) method.getStatFrom(tableOperationStats).getTime().getAllTime().getCount()
                        )
                );

        getQueryRunner().execute(query);
        Map<GlueMetastoreMethod, Integer> endingCounts = Arrays.stream(GlueMetastoreMethod.values())
                .collect(Collectors.toMap(
                                Function.identity(),
                                method -> (int) method.getStatFrom(catalogStats).getTime().getAllTime().getCount() +
                                        (int) method.getStatFrom(tableOperationStats).getTime().getAllTime().getCount()
                        )
                );

        Map<GlueMetastoreMethod, Integer> deltas = Arrays.stream(GlueMetastoreMethod.values())
                .collect(Collectors.toMap(Function.identity(), method -> endingCounts.get(method) - startingCounts.get(method)));
        ImmutableMultiset.Builder<GlueMetastoreMethod> builder = ImmutableMultiset.builder();
        deltas.entrySet().stream().filter(entry -> entry.getValue() > 0).forEach(entry -> builder.setCount(entry.getKey(), entry.getValue()));
        Multiset<GlueMetastoreMethod> actualInvocations = builder.build();

        if (expectedInvocations.equals(actualInvocations)) {
            return;
        }

        List<String> mismatchReport = Sets.union(expectedInvocations.elementSet(), actualInvocations.elementSet()).stream()
                .filter(key -> expectedInvocations.count(key) != actualInvocations.count(key))
                .flatMap(key -> {
                    int expectedCount = expectedInvocations.count(key);
                    int actualCount = actualInvocations.count(key);
                    if (actualCount < expectedCount) {
                        return Stream.of(format("%s more occurrences of %s", expectedCount - actualCount, key));
                    }
                    if (actualCount > expectedCount) {
                        return Stream.of(format("%s fewer occurrences of %s", actualCount - expectedCount, key));
                    }
                    return Stream.of();
                })
                .collect(toImmutableList());

        fail("Expected: \n\t\t" + join(",\n\t\t", mismatchReport));
    }

    @AfterClass
    public void cleanUpSchema()
            throws Exception
    {
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schema);
    }

    static class TheStatsBurgler
    {
        @Inject
        TheStatsBurgler(TrinoCatalogFactory factory, IcebergTableOperationsProvider provider)
        {
            verify(factory instanceof TrinoGlueCatalogFactory, "Must use this module with the configuration of iceberg.catalog.type=glue");
            TestIcebergGlueCatalogAccessOperations.catalogStatsPin = new AtomicReference<>(((TrinoGlueCatalogFactory) factory).getStats());
            verify(provider instanceof GlueIcebergTableOperationsProvider, "Must use this module with the configuration of iceberg.catalog.type=glue");
            TestIcebergGlueCatalogAccessOperations.tableOperationStatsPin = new AtomicReference<>(((GlueIcebergTableOperationsProvider) provider).getStats());
        }
    }

    class StealStatsModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            // Eager singleton to make singleton immediately as a dummy object to trigger code that will extract the stats out of the catalog factory
            binder.bind(TheStatsBurgler.class).asEagerSingleton();
        }
    }
}
