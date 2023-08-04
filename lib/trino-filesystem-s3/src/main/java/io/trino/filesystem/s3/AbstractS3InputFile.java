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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

abstract class AbstractS3InputFile
        implements TrinoInputFile
{
    protected final S3Client client;
    protected final S3Location location;
    protected Long length;
    private final RequestPayer requestPayer;

    private Instant lastModified;

    public AbstractS3InputFile(S3Client client, S3Context context, S3Location location, Long length)
    {
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.length = length;
        this.requestPayer = context.requestPayer();
        location.location().verifyValidFileLocation();
    }

    @Override
    public long length()
            throws IOException
    {
        if ((length == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if ((lastModified == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return lastModified;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return headObject();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private boolean headObject()
            throws IOException
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .build();

        try {
            HeadObjectResponse response = client.headObject(request);
            if (length == null) {
                length = response.contentLength();
            }
            if (lastModified == null) {
                lastModified = response.lastModified();
            }
            return true;
        }
        catch (NoSuchKeyException e) {
            return false;
        }
        catch (SdkException e) {
            throw new IOException("S3 HEAD request failed for file: " + location, e);
        }
    }
}
