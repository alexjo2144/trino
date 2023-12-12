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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3Context;
import io.trino.filesystem.s3.S3FileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;

public class IcebergRestFileSystemFactory
        implements SigningFileSystemFactory
{
    private final String region;

    @Inject
    public IcebergRestFileSystemFactory(S3FileSystemConfig s3FileSystemConfig)
    {
        this.region = s3FileSystemConfig.getRegion();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> config)
    {
        S3ClientBuilder s3 = S3Client.builder();
        s3.region(Region.of(region));
        new S3FileIOProperties(config).applySignerConfiguration(s3);
        return new S3FileSystem(
                s3.build(),
                new S3Context(
                        toIntExact(DataSize.of(16, MEGABYTE).toBytes()),
                        false,
                        S3FileSystemConfig.S3SseType.NONE,
                        null));
    }
}
