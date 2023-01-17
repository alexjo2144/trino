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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;

import java.util.List;

public record TableChangesTableFunctionHandle(
        @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
        @JsonProperty("sinceVersion") long sinceVersion, // inclusive, contrary to table_changes function parameter since_version
        @JsonProperty("tableReadVersion") long tableReadVersion,
        @JsonProperty("columns") List<DeltaLakeColumnHandle> columns) implements ConnectorTableFunctionHandle
{}
