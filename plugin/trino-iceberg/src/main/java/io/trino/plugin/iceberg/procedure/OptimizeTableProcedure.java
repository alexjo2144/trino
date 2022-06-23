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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.type.IntegerType.INTEGER;

public class OptimizeTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String SELECTED_PARTITIONING_IDS_PROPERTY = "selected_partitioning_ids";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                OPTIMIZE.name(),
                distributedWithFilteringAndRepartitioning(),
                ImmutableList.of(
                        dataSizeProperty(
                                "file_size_threshold",
                                "Only compact files smaller than given threshold in bytes",
                                DataSize.of(100, DataSize.Unit.MEGABYTE),
                                false),
                        new PropertyMetadata<>(
                                SELECTED_PARTITIONING_IDS_PROPERTY,
                                "List of partition spec ids to optimize",
                                new ArrayType(INTEGER),
                                List.class,
                                ImmutableList.of(),
                                false,
                                value -> ImmutableList.copyOf((Collection<?>) value),
                                value -> value)));
    }

    @SuppressWarnings("unchecked")
    public static Set<Integer> getSelectedPartitioningIds(Map<String, Object> procedureProperties)
    {
        List<Integer> selectedPartitioningIds = (List<Integer>) procedureProperties.get(SELECTED_PARTITIONING_IDS_PROPERTY);
        return ImmutableSet.copyOf(selectedPartitioningIds);
    }
}
