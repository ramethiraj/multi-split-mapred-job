package com.mapr.health.job1;

import com.mapr.health.util.SchemaUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

/**
 * Reads raw PHI from MapR-DB and outputs a key-value pair 
 * for the next job's input.
 * Key: Anonymized Patient ID (Hash)
 * Value: Anonymized data string (non-PHI data)
 */
public class PHIAnonymizationMapper 
    extends TableMapper<Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    public void map(ImmutableBytesWritable rowKey, Result result, Context context)
            throws IOException, InterruptedException {

        // 1. Extract Raw PHI and Metadata
        byte[] patientNameBytes = result.getValue(SchemaUtility.RAW_PHI_CF, SchemaUtility.PATIENT_NAME_COL);
        byte[] visitDateBytes = result.getValue(SchemaUtility.METADATA_CF, SchemaUtility.VISIT_DATE_COL);

        if (patientNameBytes == null || visitDateBytes == null) {
            context.getCounter("PHIPipeline", "MISSING_DATA").increment(1);
            return; // Skip records with missing essential data
        }

        String patientName = new String(patientNameBytes, StandardCharsets.UTF_8);
        String visitDate = new String(visitDateBytes, StandardCharsets.UTF_8);

        // 2. Anonymization: Hash the Patient Name (a core PHI element)
        String anonymizedId;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(patientName.getBytes(StandardCharsets.UTF_8));
            anonymizedId = Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not found for anonymization.", e);
        }

        // 3. Prepare output for temporary MapR-FS location
        // Key: Hashed Patient ID
        // Value: All non-PHI/Anonymized data that needs to be retained (e.g., VISIT_DATE)
        
        outputKey.set(anonymizedId);
        outputValue.set(visitDate);
        
        context.write(outputKey, outputValue);
        context.getCounter("PHIPipeline", "RECORDS_ANONYMIZED").increment(1);
    }
}
