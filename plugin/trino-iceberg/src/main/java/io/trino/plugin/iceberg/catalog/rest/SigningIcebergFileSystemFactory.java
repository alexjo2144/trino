package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.REMOTE_SIGNING_ENABLED;

public class SigningIcebergFileSystemFactory
        implements IcebergFileSystemFactory
{
    private final S3FileSystemConfig config;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public SigningIcebergFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, S3FileSystemConfig s3FileSystemConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.config = s3FileSystemConfig;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        TrinoFileSystem fileSystem;
        if ("true".equals(fileIoProperties.get(REMOTE_SIGNING_ENABLED))) {
            return new IcebergRestFileSystemFactory(config).create(identity, fileIoProperties);
        }
        else {
            return fileSystemFactory.create(identity);
        }
    }
}
