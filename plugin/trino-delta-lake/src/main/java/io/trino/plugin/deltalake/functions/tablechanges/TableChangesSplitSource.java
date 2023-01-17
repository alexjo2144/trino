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
import io.trino.filesystem.Locations;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdfFileEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DELETE_OPERATION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.MERGE_OPERATION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.UPDATE_OPERATION;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.DATA_FILE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final TrinoFileSystem fileSystem;
    private final String tableLocation;
    private final AtomicBoolean isFinished;
    private final AtomicLong currentVersion;
    private final long tableReadVersion;

    public TableChangesSplitSource(
            ConnectorSession session,
            DeltaLakeMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            TableChangesTableFunctionHandle functionHandle)
    {
        tableLocation = metastore.getTableLocation(functionHandle.schemaTableName());
        fileSystem = fileSystemFactory.create(session);
        currentVersion = new AtomicLong(functionHandle.sinceVersion());
        isFinished = new AtomicBoolean(false);
        tableReadVersion = functionHandle.tableReadVersion();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long processedVersion = currentVersion.getAndIncrement();
        String transactionLogDir = getTransactionLogDir(tableLocation);
        try {
            if (processedVersion <= tableReadVersion) {
                Optional<List<DeltaLakeTransactionLogEntry>> entriesFromJson = getEntriesFromJson(processedVersion, transactionLogDir, fileSystem);
                if (entriesFromJson.isPresent()) {
                    List<DeltaLakeTransactionLogEntry> entries = entriesFromJson.get();
                    if (!entries.isEmpty()) {
                        validateThatCdfWasEnabled(entries, processedVersion);
                        List<CommitInfoEntry> commitInfoEntries = entries.stream()
                                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                                .filter(Objects::nonNull)
                                .collect(toImmutableList());
                        if (commitInfoEntries.size() != 1) {
                            throw new TrinoException(DELTA_LAKE_BAD_DATA, "There should be exactly 1 commitInfo present in a metadata file");
                        }
                        CommitInfoEntry commitInfo = getOnlyElement(commitInfoEntries);
                        List<ConnectorSplit> splits;
                        if (commitInfo.getOperation().equalsIgnoreCase(MERGE_OPERATION) ||
                                commitInfo.getOperation().equalsIgnoreCase(DELETE_OPERATION) ||
                                commitInfo.getOperation().equalsIgnoreCase(UPDATE_OPERATION)) {
                            splits = entries.stream()
                                    .map(DeltaLakeTransactionLogEntry::getCDC)
                                    .filter(Objects::nonNull)
                                    .map(cdfFileEntry -> mapToDeltaLakeTableChangesSplit(
                                            commitInfo,
                                            CDF_FILE,
                                            cdfFileEntry.getSize(),
                                            cdfFileEntry.getPath(),
                                            cdfFileEntry.getCanonicalPartitionValues()))
                                    .collect(toImmutableList());
                        }
                        else {
                            List<ConnectorSplit> temporarySplits = new ArrayList<>();
                            entries.forEach(entry -> {
                                if (entry.getAdd() != null) {
                                    AddFileEntry addEntry = entry.getAdd();
                                    temporarySplits.add(mapToDeltaLakeTableChangesSplit(
                                            commitInfo,
                                            DATA_FILE,
                                            addEntry.getSize(),
                                            addEntry.getPath(),
                                            addEntry.getCanonicalPartitionValues()));
                                }
                                if (entry.getCDC() != null) {
                                    CdfFileEntry cdfEntry = entry.getCDC();
                                    mapToDeltaLakeTableChangesSplit(
                                            commitInfo,
                                            CDF_FILE,
                                            cdfEntry.getSize(),
                                            cdfEntry.getPath(),
                                            cdfEntry.getCanonicalPartitionValues());
                                }
                            });
                            splits = temporarySplits;
                        }
                        return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, processedVersion == tableReadVersion));
                    }
                }
                else {
                    throw new TrinoException(DELTA_LAKE_BAD_DATA, "Delta Lake log entries are missing for version " + currentVersion);
                }
            }
            else {
                isFinished.set(true);
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table metadata", e);
        }
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
    }

    private void validateThatCdfWasEnabled(List<DeltaLakeTransactionLogEntry> entries, long processedVersion)
    {
        boolean containsRemoveEntry = false;
        boolean containsCdcEntry = false;
        for (DeltaLakeTransactionLogEntry entry : entries) {
            if (entry.getCDC() != null) {
                containsCdcEntry = true;
            }
            if (entry.getRemove() != null) {
                containsRemoveEntry = true;
            }
        }
        if (containsRemoveEntry && !containsCdcEntry) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Change Data Feed is not enabled at version %d. Version contains 'remove' entries without 'cdc' entries", processedVersion));
        }
    }

    private TableChangesSplit mapToDeltaLakeTableChangesSplit(
            CommitInfoEntry commitInfoEntry,
            TableChangesFileType source,
            long length,
            String entryPath,
            Map<String, Optional<String>> canonicalPartitionValues)
    {
        String path = Locations.appendPath(tableLocation, entryPath);
        return new TableChangesSplit(
                path,
                length,
                canonicalPartitionValues,
                commitInfoEntry.getTimestamp(),
                source,
                commitInfoEntry.getVersion());
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished()
    {
        return isFinished.get();
    }
}
