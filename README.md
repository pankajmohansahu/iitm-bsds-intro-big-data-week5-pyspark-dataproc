# Week 5 - Graded Assignment 4: Dataproc PySpark Data Processing Pipeline

## Project Overview

This assignment demonstrates a production-grade **data cleaning, validation, and aggregation pipeline** using **Apache Spark on Google Cloud Dataproc**. 

**Objective:** Process customer and transaction datasets, clean invalid records, detect duplicates, enforce referential integrity, and produce aggregated metrics using PySpark on Google Cloud.

---

## Project Structure

```
Week-5/
├── README.md                                    # This file - complete documentation
├── customer_dataset.csv                         # Input: Customer master data
├── transaction_dataset.csv                      # Input: Transaction records
├── week5_assignment4_dataproc_pyspark.py       # Main PySpark application
├── RUN_IN_CLOUD_SHELL.md                       # Step-by-step Cloud Shell guide
├── submit_dataproc_job.sh                       # Automated job submission script
├── instructions.txt                             # Reference: project ID and command templates
└── .../week5/                                   # GCS bucket structure (see below)
    ├── input/                                   # Input datasets
    │   ├── customer_dataset.csv
    │   └── transaction_dataset.csv
    ├── code/                                    # PySpark application code
    │   └── week5_assignment4_dataproc_pyspark.py
    └── output/                                  # Job outputs (created by Spark)
        ├── cleaned_data/
        │   ├── customers/
        │   └── transactions/
        ├── invalid_rows/
        │   ├── customers/
        │   └── transactions/
        ├── joined_data/
        └── aggregates/
            ├── total_avg_per_customer/
            ├── total_per_city/
            └── top3_customers/
```

---

## File Descriptions

### Input Data Files

#### 1. **customer_dataset.csv**
- **Purpose:** Master customer records
- **Columns:** `customer_id`, `customer_name`, `city`, `status`
- **Role in Pipeline:** Cleaned, validated, and joined with transactions
- **Expected Issues Detected:**
  - Missing or blank fields
  - Invalid `customer_id` format (must match `c\d+`)
  - Invalid `status` values (must be 'active' or 'inactive')
  - Duplicate full-row records
  - City name variations (normalized to standard format)

#### 2. **transaction_dataset.csv**
- **Purpose:** Individual transaction records
- **Columns:** `transaction_id`, `customer_id`, `transaction_date`, `transaction_amount`, `payment_mode`
- **Role in Pipeline:** Cleaned, validated, categorized, and aggregated
- **Expected Issues Detected:**
  - Missing or blank fields
  - Invalid `transaction_amount` (must be positive and numeric)
  - Malformed `transaction_date` (must be `YYYY-MM-DD` format)
  - Duplicate full-row records
  - Referential integrity violations (customer_id not in customer dataset)

### Code Files

#### 3. **week5_assignment4_dataproc_pyspark.py**
- **Type:** Apache Spark Python application
- **Engine:** PySpark (Spark SQL + DataFrames)
- **Execution:** Runs on Google Cloud Dataproc cluster
- **Main Functions:**

  **Data Loading & Inspection**
  - Reads CSVs from GCS with schema inference
  - Prints sample records and schemas for verification

  **Data Cleaning (Customer)**
  - Trims whitespace from all columns
  - Normalizes city names (Kochi↔Cochin, Trivandrum↔Thiruvananthapuram, etc.)
  - Converts status to lowercase
  - Detects duplicates using row_number over all columns

  **Data Cleaning (Transaction)**
  - Trims and casts transaction_amount to double
  - Parses transaction_date to proper date type
  - Categorizes transactions (Low: <3000, Medium: 3000-6999, High: ≥7000)
  - Extracts transaction month
  - Detects duplicates

  **Validation**
  - Marks invalid customer rows with `invalid_reason` column
  - Marks invalid transaction rows with `invalid_reason` column
  - Enforces referential integrity: all transaction customer_ids must exist in cleaned customers

  **Aggregations**
  1. **total_avg_per_customer:** Sum and average transaction_amount per customer
  2. **total_per_city:** Sum of transaction_amount grouped by city
  3. **top3_customers:** Top 3 customers by total transaction amount

  **Output**
  - Writes all cleaned data, invalid records, joined data, and aggregations to GCS in CSV format

#### 4. **RUN_IN_CLOUD_SHELL.md**
- **Purpose:** Step-by-step execution guide for Cloud Shell
- **Contains:**
  - Variable setup (PROJECT_ID, REGION, BUCKET, directories)
  - API enablement commands
  - File upload instructions
  - Dataproc cluster creation
  - Job submission command
  - Output verification and download instructions
- **Execution Mode:** Manual command-by-command in Cloud Shell

#### 5. **submit_dataproc_job.sh**
- **Purpose:** Automated end-to-end job submission script
- **Features:**
  - Auto-detects project ID from gcloud config
  - Validates all input files exist locally
  - Checks bucket accessibility
  - Creates folder structure (.keep files)
  - Uploads datasets and code to GCS
  - Auto-creates Dataproc cluster if missing
  - Submits the job
  - Lists output folders
- **Execution Mode:** One-command execution: `./submit_dataproc_job.sh`

#### 6. **instructions.txt**
- **Purpose:** Reference file with actual GCP project ID and command templates
- **Contains:**
  - Project ID: `project-399f087e-fef4-4831-b4e`
  - Bucket name: `ibd-week5-21f2001203`
  - Sample execution commands with full GCS paths
  - Alternative workflow (pull files from GCS, process locally in Cloud Shell)

---

## Google Cloud Storage (GCS) Bucket Structure

**Bucket Name:** `ibd-week5-21f2001203`  
**Location:** US (multiple regions)  
**Storage Class:** Standard  

### Folder Hierarchy

```
gs://ibd-week5-21f2001203/
└── week5/
    ├── input/
    │   ├── customer_dataset.csv          (5.4 KB)
    │   └── transaction_dataset.csv       (10.6 KB)
    ├── code/
    │   └── week5_assignment4_dataproc_pyspark.py
    └── output/
        ├── cleaned_data/
        │   ├── customers/                (part-00000.csv, part-00001.csv, ...)
        │   └── transactions/             (part-00000.csv, part-00001.csv, ...)
        ├── invalid_rows/
        │   ├── customers/                (part-00000.csv, ...)
        │   └── transactions/             (part-00000.csv, ...)
        ├── joined_data/                  (part-00000.csv, part-00001.csv, ...)
        └── aggregates/
            ├── total_avg_per_customer/   (part-00000.csv)
            ├── total_per_city/           (part-00000.csv)
            └── top3_customers/           (part-00000.csv)
```

---

## How the Pipeline Works

### Step 1: Data Loading
```
customer_dataset.csv ─┐
transaction_dataset.csv ┤──> Load into Spark DataFrames
                       └──> Print schemas & samples
```

### Step 2: Customer Cleaning & Validation
```
Raw Customers ──> Trim columns ──> Normalize cities ──> Mark duplicates ──> Flag invalid rows
                                                                           │
                                                    ┌─────────────────────┘
                                                    │
                            ┌───────────────────────┴──────────┐
                            ↓                                  ↓
                  Cleaned Customers         Invalid Customer Rows
                   (ready to join)          (with reasons)
```

### Step 3: Transaction Cleaning & Validation
```
Raw Transactions ──> Cast types ──> Parse dates ──> Categorize amounts ──> Mark duplicates
                                                                            │
                                                    ┌────────────────────────┘
                                                    │
                            ┌───────────────────────┴──────────────┐
                            ↓                                      ↓
              Transaction Candidates         Invalid Transaction Rows
                        │
                    Validate referential integrity
                    (customer_id exists in cleaned customers?)
                        │
                ┌───────┴──────┐
                ↓              ↓
        Cleaned Transactions  Referential Integrity Violations
```

### Step 4: Join Data
```
Cleaned Transactions ──┐
                      ├──> INNER JOIN on customer_id ──> Joined Dataset
Cleaned Customers ────┘    (transactions + customer details)
```

### Step 5: Aggregations
```
Joined Dataset ──┬──> GROUP BY customer_id ──> Total & Avg per Customer
                 ├──> GROUP BY city ──> Total per City
                 └──> ORDER BY total DESC, LIMIT 3 ──> Top 3 Customers
```

### Step 6: Output
```
All results written to GCS in partitioned CSV format (part-00000.csv, etc.)
```

---

## Execution Instructions

### Option 1: Using submission script (RECOMMENDED)

**Prerequisites:**
- Google Cloud project with Dataproc enabled
- GCS bucket created: `ibd-week5-21f2001203`
- Cloud Shell access

**Steps:**
1. Open Cloud Shell
2. Download/copy the three files into Cloud Shell home:
   ```bash
   # Use "Upload" button in Cloud Shell or copy files
   ls ~/customer_dataset.csv ~/transaction_dataset.csv ~/week5_assignment4_dataproc_pyspark.py
   ```
3. Download and run the submission script:
   ```bash
   chmod +x submit_dataproc_job.sh
   ./submit_dataproc_job.sh
   ```
4. Script automatically:
   - Validates your GCP project and bucket
   - Creates folder structure
   - Uploads datasets and code to GCS
   - Creates Dataproc cluster (if needed)
   - Submits the PySpark job
   - Verifies outputs

**Expected Duration:** 5-10 minutes (cluster creation takes 3-5 min)

### Option 2: Manual step-by-step (using RUN_IN_CLOUD_SHELL.md)

Follow the numbered sections in [RUN_IN_CLOUD_SHELL.md](RUN_IN_CLOUD_SHELL.md):
1. Set variables (PROJECT_ID, BUCKET, REGION)
2. Enable APIs
3. Create bucket structure
4. Upload files
5. Create cluster
6. Submit job
7. Verify and download outputs

---

## Expected Job Outputs

After successful job completion, verify outputs in GCS:

```bash
gsutil ls -r "gs://ibd-week5-21f2001203/week5/output/**"
```

### Output Folders & Sample Content

#### 1. **cleaned_data/customers/**
- Contains valid, deduplicated customer records
- Columns: `customer_id`, `customer_name`, `city`, `status`
- Files: `part-00000.csv`, `part-00001.csv`, ... (partitioned by Spark)

#### 2. **cleaned_data/transactions/**
- Contains valid, deduplicated transaction records
- Columns: `transaction_id`, `customer_id`, `transaction_date`, `transaction_month`, `transaction_amount`, `amount_category`, `payment_mode`
- Files: `part-00000.csv`, `part-00001.csv`, ...

#### 3. **invalid_rows/customers/**
- Customer records that failed validation
- Columns: All original columns + `invalid_reason`
- Sample invalid_reasons: `missing_customer_id`, `invalid_status`, `duplicate_record`, `invalid_customer_id_format`

#### 4. **invalid_rows/transactions/**
- Transaction records that failed validation or referential integrity check
- Columns: All original columns + `invalid_reason`
- Sample invalid_reasons: `missing_transaction_id`, `non_positive_transaction_amount`, `invalid_transaction_date_format`, `customer_id_not_found_in_customer_dataset`

#### 5. **joined_data/**
- Transactions enriched with customer information
- Columns: `transaction_id`, `customer_id`, `customer_name`, `city`, `status`, `transaction_date`, `transaction_month`, `transaction_amount`, `amount_category`, `payment_mode`
- Files: `part-00000.csv`, `part-00001.csv`, ...

#### 6. **aggregates/total_avg_per_customer/**
- Summary per customer
- Columns: `customer_id`, `customer_name`, `total_transaction_amount`, `avg_transaction_amount`
- Files: `part-00000.csv`

#### 7. **aggregates/total_per_city/**
- Summary per city
- Columns: `city`, `total_transaction_amount`, `count`
- Files: `part-00000.csv`

#### 8. **aggregates/top3_customers/**
- Top 3 customers by total transaction amount
- Columns: `customer_id`, `customer_name`, `total_transaction_amount`, `avg_transaction_amount`
- Files: `part-00000.csv`

---

## Submission Checklist

Before submitting, verify:

- [ ] All 8 output folders exist in GCS under `week5/output/`
- [ ] Each output folder contains at least one `part-*.csv` file
- [ ] Job Status in Dataproc console shows "Succeeded"
- [ ] Driver log shows all `show()` outputs for inspection
- [ ] Screenshot captured with Status: Succeeded + sample driver logs
- [ ] Downloaded outputs zipped and ready for submission

---

## Troubleshooting

### Issue: "Bucket not found"
**Solution:** Verify bucket name in `submit_dataproc_job.sh` or set it:
```bash
export BUCKET="ibd-week5-21f2001203"
./submit_dataproc_job.sh
```

### Issue: "Cluster creation failed"
**Solution:** Check quota in Compute Engine > Quotas. If quota full, delete old clusters:
```bash
gcloud dataproc clusters delete week5-dataproc-cluster --region=us-central1 -q
```

### Issue: "File not found in GCS"
**Solution:** Manually upload files:
```bash
gsutil cp customer_dataset.csv gs://ibd-week5-21f2001203/week5/input/
gsutil cp transaction_dataset.csv gs://ibd-week5-21f2001203/week5/input/
gsutil cp week5_assignment4_dataproc_pyspark.py gs://ibd-week5-21f2001203/week5/code/
```

### Issue: "Job failed with error"
**Solution:** Check Dataproc job logs:
1. Open GCP Console > Dataproc > Jobs
2. Click the failed job
3. Scroll to "Logs" section and check stderr/stdout

---

## Key Technical Details

| Component | Details |
|-----------|---------|
| **Spark Version** | 2.2 (Dataproc image version 2.2-debian12) |
| **Python Version** | 3.9+ |
| **Machine Type** | e2-standard-2 (2 vCPU, 8 GB RAM) |
| **Cluster Mode** | Single-node (master only, no workers) |
| **Data Format** | CSV with headers |
| **Output Format** | CSV (partitioned by Spark writers) |
| **Region** | us-central1 (compatible with US multi-region bucket) |
| **Deduplication Logic** | Full-row match using row_number() over all columns |
| **Referential Integrity** | Left-anti join to catch missing customer_ids |

---

## References

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Google Cloud Dataproc](https://cloud.google.com/dataproc)
- [GCS Tools (gsutil)](https://cloud.google.com/storage/docs/gsutil)
- [gcloud CLI Reference](https://cloud.google.com/cli/docs)

---

## Assignment Summary

**Student:** [Your Name]  
**Course:** IIT Madras BS in Data Science & Applications - Intro to Big Data  
**Term:** JAN2026  
**Assignment:** Week 5, Graded Assignment 4  
**Submission Date:** [Your Date]  
**Project ID:** `project-399f087e-fef4-4831-b4e`  
**Bucket Name:** `ibd-week5-21f2001203`

---

**Last Updated:** March 19, 2026
