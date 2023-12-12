package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;

import static org.junit.jupiter.api.Assumptions.abort;

public class TestIcebergRestConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    public TestIcebergRestConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .put("iceberg.catalog.type", "REST")
                        .put("fs.native-s3.enabled", "true")
                        .put("iceberg.rest-catalog.uri", "https://api.tabular.io/ws")
                        .put("iceberg.rest-catalog.security", "OAUTH2")
                        .put("iceberg.rest-catalog.oauth2.credential", "t-XAxglvTCD2U:tf2ss6cXvsLR7unQtfElObqSV2g")
                        .put("iceberg.rest-catalog.warehouse", "alex-j-test-warehouse")
                        .put("s3.region", "us-east-1")
                        .buildOrThrow())
                .build();
    }


    @Override
    protected void deleteDirectory(String location)
    {
        abort();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        abort();
        return false;
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        abort();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        abort();
        return "";
    }

    @Override
    protected String schemaPath()
    {
        abort();
        return "";
    }

    @Override
    protected boolean locationExists(String location)
    {
        abort();
        return false;
    }
}
