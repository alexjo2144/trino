package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;

public class S3SelectInputStream
    extends AbstractS3InputStream<SelectObjectContentResponse>
{
    public S3SelectInputStream(Location location, Long length)
    {
        super(location, length);
    }

    @Override
    ResponseInputStream<SelectObjectContentResponse> inputStreamFor(long nextReadPosition)
    {
        return null;
    }
}
