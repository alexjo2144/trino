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

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;

import static java.util.Objects.requireNonNull;

public class S3SelectInputFile
        extends AbstractS3InputFile
{
    private final S3Client s3;
    private final S3AsyncClient asyncS3;
    private final String query;
    private final InputSerialization inputSerialization;
    private final OutputSerialization outputSerialization;

    public S3SelectInputFile(
            S3Client s3,
            S3AsyncClient asyncS3,
            S3Context context,
            S3Location location,
            String query,
            InputSerialization inputSerialization,
            OutputSerialization outputSerialization)
    {
        super(s3, context, location, null);
        this.s3 = requireNonNull(s3, "s3 is null");
        this.asyncS3 = requireNonNull(asyncS3, "asyncS3 is null");
        this.query = requireNonNull(query, "query is null");
        this.inputSerialization = requireNonNull(inputSerialization, "inputSerialization is null");
        this.outputSerialization = requireNonNull(outputSerialization, "outputSerialization is null");
    }

    @Override
    public TrinoInput newInput()
    {
        return new S3SelectInput(
                location(),
                s3,
                asyncS3,
                buildSelectObjectContentRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return null; // new S3InputStream(location(), client, newGetObjectRequest(), length);
    }

    private SelectObjectContentRequest buildSelectObjectContentRequest()
    {
        return SelectObjectContentRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .expression(query)
                .expressionType(ExpressionType.SQL)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization)
                .build();
    }
}
