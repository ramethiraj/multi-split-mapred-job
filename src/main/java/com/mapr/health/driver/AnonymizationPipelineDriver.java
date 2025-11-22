package com.mapr.health.driver;

import com.mapr.health.job1.PHIAnonymizationMapper;
import com.mapr.health.job1.PHIAnonymizationReducer;
import com.mapr.health.job2.ArchivalTagMapper;
import com.mapr.health.job2.ArchivalTagReducer;
import com.mapr.health.util.SchemaUtility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil; 

public class AnonymizationPipelineDriver extends Configured implements Tool {
    
    // Intermediate MapR-FS path between Job 1 and Job 2
    private static final Path INTERMEDIATE_PATH = new Path("/tmp/phi_intermediate_data"); 
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        // 1. Setup - Create the MapR-DB tables
        SchemaUtility.createTables(conf); 

        // 2. Clean up intermediate path if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(INTERMEDIATE_PATH)) {
            System.out.println("Cleaning up intermediate path: " + INTERMEDIATE_PATH);
            fs.delete(INTERMEDIATE_PATH, true);
        }

        // --- Job 1: PHI Identification and Anonymization (MapR-DB to MapR-FS) ---
        Job job1 = Job.getInstance(conf, "PHI Anonymization Job");
        job1.setJarByClass(AnonymizationPipelineDriver.class);

        // Input: MapR-DB Table
        TableMapReduceUtil.initTableMapperJob(
            SchemaUtility.RAW_TABLE.getNameAsString(), // input table name
            null, // Scan (null for full table scan)
            PHIAnonymizationMapper.class, // mapper
            Text.class, // mapper output key
            Text.class, // mapper output value
            job1);

        job1.setReducerClass(PHIAnonymizationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(4); // Use 4 reducers for Job 1

        // Output: MapR-FS intermediate path
        FileOutputFormat.setOutputPath(job1, INTERMEDIATE_PATH);

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 (Anonymization) failed!");
            return 1;
        }
        System.out.println("Job 1 (Anonymization) completed successfully.");


        // --- Job 2: Archival Storage Preparation (MapR-FS to MapR-DB) ---
        Job job2 = Job.getInstance(conf, "Archival Tagging Job");
        job2.setJarByClass(AnonymizationPipelineDriver.class);
        
        // Input: MapR-FS intermediate path (Output of Job 1)
        FileInputFormat.addInputPath(job2, INTERMEDIATE_PATH);
        job2.setInputFormatClass(KeyValueTextInputFormat.class); // Reads key/value separated by tab

        job2.setMapperClass(ArchivalTagMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // Output: MapR-DB Archival Table
        TableMapReduceUtil.initTableReducerJob(
            SchemaUtility.ARCHIVE_TABLE.getNameAsString(), // output table name
            ArchivalTagReducer.class, // reducer
            job2);

        job2.setNumReduceTasks(2); // Use 2 reducers for Job 2
        
        boolean success = job2.waitForCompletion(true);

        // 3. Cleanup after success
        if (success) {
            fs.delete(INTERMEDIATE_PATH, true);
            System.out.println("Job 2 (Archival Tagging) completed successfully. Intermediate data cleaned up.");
        } else {
            System.err.println("Job 2 (Archival Tagging) failed!");
            return 1;
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Run the main driver using the Hadoop ToolRunner
        int exitCode = ToolRunner.run(new AnonymizationPipelineDriver(), args);
        System.exit(exitCode);
    }
}
