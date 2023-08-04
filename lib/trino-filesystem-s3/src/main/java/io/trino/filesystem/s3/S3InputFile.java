package io.trino.filesystem.s3;

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;

final class S3InputFile
    extends AbstractS3InputFile
{
    private final RequestPayer requestPayer;

    public S3InputFile(S3Client client, S3Context context, S3Location location, Long length)
    {
        super(client, context, location, length);
        this.requestPayer = context.requestPayer();
    }

    @Override
    public TrinoInput newInput()
    {
        return new S3Input(location(), client, newGetObjectRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return new S3InputStream(location(), client, newGetObjectRequest(), length);
    }

    private GetObjectRequest newGetObjectRequest()
    {
        return GetObjectRequest.builder()
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .build();
    }
}
