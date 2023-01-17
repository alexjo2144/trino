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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.deltalake.CorruptedDeltaLakeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final String SCHEMA_NAME = "system";
    private static final String NAME = "table_changes";
    public static final SchemaFunctionName TABLE_CHANGES_NAME = new SchemaFunctionName(SCHEMA_NAME, NAME);
    public static final String SCHEMA_NAME_ARGUMENT = "SCHEMA_NAME";
    private static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    private static final String SINCE_VERSION_ARGUMENT = "SINCE_VERSION";
    private static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";
    private static final String COMMIT_VERSION_COLUMN_NAME = "_commit_version";
    private static final String COMMIT_TIMESTAMP_COLUMN_NAME = "_commit_timestamp";

    private final DeltaLakeMetadataFactory deltaLakeMetadataFactory;

    public TableChangesFunction(DeltaLakeMetadataFactory deltaLakeMetadataFactory)
    {
        super(
                SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(SINCE_VERSION_ARGUMENT).type(BIGINT).defaultValue(-1L).build()),
                GENERIC_TABLE);
        this.deltaLakeMetadataFactory = requireNonNull(deltaLakeMetadataFactory, "deltaLakeMetadataFactory is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        ScalarArgument schemaNameArgument = (ScalarArgument) arguments.get(SCHEMA_NAME_ARGUMENT);
        checkFunctionArgument(schemaNameArgument.getValue() != null, "schema_name cannot be null");
        String schemaName = ((Slice) schemaNameArgument.getValue()).toStringUtf8();

        ScalarArgument tableNameArgument = (ScalarArgument) arguments.get(TABLE_NAME_ARGUMENT);
        checkFunctionArgument(tableNameArgument.getValue() != null, "table_name value for function table_changes() cannot be null");
        String tableName = ((Slice) tableNameArgument.getValue()).toStringUtf8();

        ScalarArgument sinceVersionArgument = (ScalarArgument) arguments.get(SINCE_VERSION_ARGUMENT);
        if (sinceVersionArgument.getValue() == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "since_version cannot be null");
        }
        long sinceVersion = (long) sinceVersionArgument.getValue() + 1; //+1 to ensure that the sinceVersion is exclusive
        checkFunctionArgument(sinceVersion >= 0, "Invalid value of since_version: %s", sinceVersionArgument.getValue());

        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(session.getIdentity());
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        ConnectorTableHandle connectorTableHandle = deltaLakeMetadata.getTableHandle(session, schemaTableName);
        if (connectorTableHandle instanceof CorruptedDeltaLakeTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        if (connectorTableHandle == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) connectorTableHandle;

        if (sinceVersion > tableHandle.getReadVersion()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "since_version is higher then current table version");
        }
        List<DeltaLakeColumnHandle> columnHandles = deltaLakeMetadata.getColumnHandles(session, tableHandle)
                .values().stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .filter(column -> column.getColumnType() != SYNTHESIZED)
                .collect(toImmutableList());

        ImmutableList.Builder<Descriptor.Field> outputFields = ImmutableList.builder();
        columnHandles.stream()
                .map(columnHandle -> new Descriptor.Field(columnHandle.getName(), Optional.of(columnHandle.getType())))
                .forEach(outputFields::add);

        // add at the end to follow databricks convention
        outputFields.add(new Descriptor.Field(CHANGE_TYPE_COLUMN_NAME, Optional.of(VARCHAR)));
        outputFields.add(new Descriptor.Field(COMMIT_VERSION_COLUMN_NAME, Optional.of(INTEGER)));
        outputFields.add(new Descriptor.Field(COMMIT_TIMESTAMP_COLUMN_NAME, Optional.of(TIMESTAMP_TZ_MILLIS)));

        return TableFunctionAnalysis.builder()
                .handle(new TableChangesTableFunctionHandle(schemaTableName, sinceVersion, tableHandle.getReadVersion(), columnHandles))
                .returnedType(new Descriptor(outputFields.build()))
                .build();
    }
}
