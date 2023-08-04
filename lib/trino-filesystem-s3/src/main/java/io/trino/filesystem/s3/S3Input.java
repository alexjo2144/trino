package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class S3Input
    extends AbstractS3Input<GetObjectRequest>
{
    private final GetObjectRequest request;

    public S3Input(Location location, S3Client client, GetObjectRequest request)
    {
        super(location, client);
        this.request = requireNonNull(request, "request is null");
    }

    @Override
    GetObjectRequest getRequest(Optional<Long> position, long length)
    {
        String range = position.map(start -> "bytes=%s-%s".formatted(start, (start + length) - 1))
                .orElse("bytes=-%s".formatted(length));
        return request.toBuilder().range(range).build();
    }

    @Override
    InputStream streamForRequest(GetObjectRequest request, S3Client client, Location location)
            throws IOException
    {
        try {
            return client.getObject(request);
        }
        catch (NoSuchKeyException e) {
            throw new FileNotFoundException(location.toString());
        }
        catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + location, e);
        }
    }
}
