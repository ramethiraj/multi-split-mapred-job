package com.mapr.health.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SchemaUtility {
    
    // Column Families
    public static final byte[] RAW_PHI_CF = "raw_cf".getBytes(StandardCharsets.UTF_8);
    public static final byte[] ANONYMIZED_CF = "anon_cf".getBytes(StandardCharsets.UTF_8);
    public static final byte[] METADATA_CF = "meta_cf".getBytes(StandardCharsets.UTF_8);

    // Columns
    public static final byte[] PATIENT_NAME_COL = "name".getBytes(StandardCharsets.UTF_8);
    public static final byte[] PATIENT_HASH_COL = "anon_id".getBytes(StandardCharsets.UTF_8);
    public static final byte[] PHI_DATE_COL = "dob".getBytes(StandardCharsets.UTF_8);
    public static final byte[] VISIT_DATE_COL = "visit_dt".getBytes(StandardCharsets.UTF_8);
    public static final byte[] RETENTION_TAG_COL = "retention_tag".getBytes(StandardCharsets.UTF_8);
    
    // Tables
    public static final TableName RAW_TABLE = TableName.valueOf("/user/mapr/phi_raw_data"); // MapR-DB Path
    public static final TableName ARCHIVE_TABLE = TableName.valueOf("/user/mapr/phi_archive_data"); // MapR-DB Path

    /**
     * Creates the MapR-DB tables if they don't exist.
     * @param conf The Hadoop/HBase configuration.
     * @throws IOException
     */
    public static void createTables(Configuration conf) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create(conf);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
             Admin admin = connection.getAdmin()) {
            
            // 1. Create RAW_TABLE (Input for Job 1)
            if (!admin.tableExists(RAW_TABLE)) {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(RAW_TABLE);
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(RAW_PHI_CF));
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(METADATA_CF));
                admin.createTable(builder.build());
                System.out.println("Created table: " + RAW_TABLE.getNameAsString());
            }

            // 2. Create ARCHIVE_TABLE (Output for Job 2)
            if (!admin.tableExists(ARCHIVE_TABLE)) {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(ARCHIVE_TABLE);
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(ANONYMIZED_CF));
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(METADATA_CF));
                admin.createTable(builder.build());
                System.out.println("Created table: " + ARCHIVE_TABLE.getNameAsString());
            }

        }
    }
}
