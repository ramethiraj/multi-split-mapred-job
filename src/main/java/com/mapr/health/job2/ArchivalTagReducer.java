package com.mapr.health.job2;

import com.mapr.health.util.SchemaUtility;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Writes the final anonymized and tagged record to the MapR-DB archival table.
 * Output: MapR-DB Put operation.
 */
public class ArchivalTagReducer 
    extends TableReducer<Text, Text, ImmutableBytesWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        // The key is the final RowKey: AnonymizedID_VisitDate
        byte[] rowKey = key.toString().getBytes(StandardCharsets.UTF_8);
        Put put = new Put(rowKey);
        
        // In a real-world scenario, you would aggregate 'values' here, 
        // but for this example, we just take the first value (visit date).
        String visitDate = values.iterator().next().toString();
        
        // 1. Add Anonymized Data (The Anonymized ID is in the RowKey)
        put.addColumn(SchemaUtility.ANONYMIZED_CF, SchemaUtility.VISIT_DATE_COL, visitDate.getBytes(StandardCharsets.UTF_8));

        // 2. Retention Logic: Assign an Archival Tag based on, say, the visit date or key structure
        // Simple logic: Visits before 2020 get a 'LONG_TERM' tag.
        String retentionTag = visitDate.startsWith("201") ? "RET_GROUP_A_LONG_TERM" : "RET_GROUP_B_STANDARD";
        
        put.addColumn(SchemaUtility.METADATA_CF, SchemaUtility.RETENTION_TAG_COL, retentionTag.getBytes(StandardCharsets.UTF_8));
        
        // Write the Put operation to the MapR-DB table (set in the driver)
        context.write(new ImmutableBytesWritable(rowKey), put);
        context.getCounter("PHIPipeline", "RECORDS_ARCHIVED").increment(1);
    }
}
