package com.mapr.health.job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Identity Reducer. Simply passes the anonymized key/value pairs
 * from the mapper to the file output (MapR-FS).
 */
public class PHIAnonymizationReducer 
    extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Output format: Key (Anonymized ID), Value (Non-PHI data)
        for (Text value : values) {
            context.write(key, value); 
        }
        context.getCounter("PHIPipeline", "ANONYMIZED_OUTPUT").increment(1);
    }
}
