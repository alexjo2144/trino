package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static java.util.Objects.requireNonNull;

public class S3InputStream
    extends AbstractS3InputStream<GetObjectResponse>
{
    private final S3Client client;
    private final GetObjectRequest request;

    public S3InputStream(Location location, S3Client client, GetObjectRequest request, Long length)
    {
        super(location, length);
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
    }

    @Override
    protected ResponseInputStream<GetObjectResponse> inputStreamFor(long nextReadPosition)
    {
        String range = "bytes=%s-".formatted(nextReadPosition);
        GetObjectRequest rangeRequest = request.toBuilder().range(range).build();
        return client.getObject(rangeRequest);
    }
}
