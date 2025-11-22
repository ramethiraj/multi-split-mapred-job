package com.mapr.health.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Reads the Anonymized records from the intermediate MapR-FS file.
 * Input format is Key (Anonymized ID) and Value (Visit Date).
 * The output key must be the final RowKey for the MapR-DB archival table.
 * Key: Anonymized ID | Visit Date
 * Value: The Visit Date itself (for aggregation in reducer)
 */
public class ArchivalTagMapper 
    extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // Input: "AnonymizedID\tVisitDate" (standard MapReduce text output)
        String[] parts = value.toString().split("\t"); 
        
        if (parts.length != 2) {
            context.getCounter("PHIPipeline", "JOB2_INVALID_RECORD").increment(1);
            return;
        }

        String anonymizedId = parts[0];
        String visitDate = parts[1];

        // The final RowKey in the Archival Table will be a composite: ANONYMIZED_ID + "_" + VISIT_DATE
        outputKey.set(anonymizedId + "_" + visitDate); 
        outputValue.set(visitDate);

        context.write(outputKey, outputValue);
        context.getCounter("PHIPipeline", "RECORDS_READ_FOR_ARCHIVE").increment(1);
    }
}
