package io.trino.plugin.iceberg;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

public interface IcebergFileSystemFactory
{
    TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties);
}
