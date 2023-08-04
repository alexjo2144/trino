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
import io.trino.filesystem.TrinoInput;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Request;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Optional;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

abstract class AbstractS3Input<R extends S3Request>
        implements TrinoInput
{
    private final Location location;
    private final S3Client client;
    private boolean closed;

    public AbstractS3Input(Location location, S3Client client)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
    }

    abstract R getRequest(Optional<Long> position, long length);
    abstract InputStream streamForRequest(R request, S3Client client, Location location) throws IOException;

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        R rangeRequest = getRequest(Optional.of(position), length);
        try (InputStream in = streamForRequest(rangeRequest, client, location)) {
            int n = readNBytes(in, buffer, offset, length);
            if (n < length) {
                throw new EOFException("Read %s of %s requested bytes: %s".formatted(n, length, location));
            }
        }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }

        R request = getRequest(Optional.empty(), length);
        try (InputStream in = streamForRequest(request, client, location)) {
            return readNBytes(in, buffer, offset, length);
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + location);
        }
    }

    private static int readNBytes(InputStream in, byte[] buffer, int offset, int length)
            throws IOException
    {
        try {
            return in.readNBytes(buffer, offset, length);
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }
}
