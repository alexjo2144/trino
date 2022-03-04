package io.trino.plugin.iceberg;

import io.trino.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;

public enum GlueMetastoreMethod
{
    BATCH_CREATE_PARTITION,
    BATCH_UPDATE_PARTITION,
    CREATE_DATABASE,
    CREATE_PARTITIONS,
    CREATE_TABLE,
    DELETE_COLUMN_STATISTICS_FOR_PARTITION,
    DELETE_COLUMN_STATISTICS_FOR_TABLE,
    DELETE_DATA_BASE,
    DELETE_PARTITION,
    DELETE_TABLE,
    GET_COLUMN_STATISTICS_FOR_PARTITION,
    GET_COLUMN_STATISTICS_FOR_TABLE,
    GET_DATABASE,
    GET_DATABASES,
    GET_PARTITION,
    GET_PARTITION_BY_NAME,
    GET_PARTITION_NAMES,
    GET_PARTITIONS,
    GET_TABLE,
    GET_TABLES,
    UPDATE_COLUMN_STATISTICS_FOR_PARTITION,
    UPDATE_COLUMN_STATISTICS_FOR_TABLE,
    UPDATE_DATABASE,
    UPDATE_PARTITION,
    UPDATE_TABLE;

    public GlueMetastoreApiStats getStatFrom(GlueMetastoreStats stats) {
        switch (this) {
            case BATCH_CREATE_PARTITION:
                return stats.getBatchCreatePartition();
            case BATCH_UPDATE_PARTITION:
                return stats.getBatchUpdatePartition();
            case CREATE_DATABASE:
                return stats.getCreateDatabase();
            case CREATE_PARTITIONS:
                return stats.getCreatePartitions();
            case CREATE_TABLE:
                return stats.getCreateTable();
            case DELETE_COLUMN_STATISTICS_FOR_PARTITION:
                return stats.getDeleteColumnStatisticsForPartition();
            case DELETE_COLUMN_STATISTICS_FOR_TABLE:
                return stats.getDeleteColumnStatisticsForTable();
            case DELETE_DATA_BASE:
                return stats.getDeleteDatabase();
            case DELETE_PARTITION:
                return stats.getDeletePartition();
            case DELETE_TABLE:
                return stats.getDeleteTable();
            case GET_COLUMN_STATISTICS_FOR_PARTITION:
                return stats.getGetColumnStatisticsForPartition();
            case GET_COLUMN_STATISTICS_FOR_TABLE:
                return stats.getGetColumnStatisticsForTable();
            case GET_DATABASE:
                return stats.getGetDatabase();
            case GET_DATABASES:
                return stats.getGetDatabases();
            case GET_PARTITION:
                return stats.getGetPartition();
            case GET_PARTITION_BY_NAME:
                return stats.getGetPartitionByName();
            case GET_PARTITION_NAMES:
                return stats.getGetPartitionNames();
            case GET_PARTITIONS:
                return stats.getGetPartitions();
            case GET_TABLE:
                return stats.getGetTable();
            case GET_TABLES:
                return stats.getGetTables();
            case UPDATE_COLUMN_STATISTICS_FOR_PARTITION:
                return stats.getUpdateColumnStatisticsForPartition();
            case UPDATE_COLUMN_STATISTICS_FOR_TABLE:
                return stats.getUpdateColumnStatisticsForTable();
            case UPDATE_DATABASE:
                return stats.getUpdateDatabase();
            case UPDATE_PARTITION:
                return stats.getUpdatePartition();
            case UPDATE_TABLE:
                return stats.getUpdateTable();
            default:
                // Proper way to make this into trino exception?
                throw new RuntimeException(String.format("Make Glue Metastore method %s has corresponding stat", this.name()));
        }
    }
}
