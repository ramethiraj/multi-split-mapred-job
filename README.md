# PHI Anonymization and Retention Pipeline

**Project Name:** `multi-split-mapred-job`

## 1. Project Overview 🏥

This project implements a multi-stage **MapReduce pipeline** designed to solve a critical **Healthcare Data Retention** problem: the **de-identification (anonymization)** of Protected Health Information (PHI) before archival storage.

By processing raw PHI data, the pipeline reduces regulatory risk (e.g., HIPAA violations) and allows for cheaper, safer long-term storage of essential, non-PHI clinical metadata for research and regulatory compliance.

### **Core Technologies**

* **Big Data System:** MapR-DB (HBase API) and MapR-FS (HDFS API)
* **Processing Framework:** Hadoop MapReduce
* **Language:** Java 11
* **Build Tool:** Apache Maven

## 2. Pipeline Stages (Multi-Split Job)

The solution is orchestrated by the `AnonymizationPipelineDriver` and runs in two sequential stages:

### **Job 1: PHI Identification and Anonymization**
* **Input:** Raw PHI data from **MapR-DB (`/user/mapr/phi_raw_data`)**
* **Mapper (`PHIAnonymizationMapper`):** Reads the raw records, hashes key PHI fields (e.g., Patient Name $\rightarrow$ Anonymized ID), and retains non-PHI metadata.
* **Output:** Key-Value text files to **MapR-FS (`/tmp/phi_intermediate_data`)**

### **Job 2: Archival Storage Preparation and Tagging**
* **Input:** Anonymized data from **MapR-FS (`/tmp/phi_intermediate_data`)**
* **Reducer (`ArchivalTagReducer`):** Aggregates the records (if needed) and applies a **Retention Tag** (`RET_GROUP_A_LONG_TERM`) based on metadata.
* **Output:** Final, anonymized records written back to an **Archival MapR-DB table (`/user/mapr/phi_archive_data`)**

## 3. Setup and Execution

### **Prerequisites**

1.  MapR Client setup with access to a cluster.
2.  Java 11 installed.
3.  Apache Maven installed.

### **Build**

```bash
mvn clean package -DskipTests
```
This command generates the runnable fat JAR: **target/phi-anonymization-pipeline-1.0.0-SNAPSHOT-jar-with-dependencies.jar.**

### **Run**

1.  The driver class automatically performs the following:
2.  Creates the input and output MapR-DB tables (SchemaUtility).
3.  Cleans up any previous intermediate data on MapR-FS.
4.  Runs Job 1 (Anonymization).
5.  Runs Job 2 (Archival Tagging).
6.  Cleans up intermediate data upon success.

```bash
# Example command to run the pipeline on the MapR/Hadoop cluster
hadoop jar target/phi-anonymization-pipeline-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.mapr.health.driver.AnonymizationPipelineDriver
```
