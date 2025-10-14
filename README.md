# Apache Airflow ETL Project Toll Data Consolidation

### Project Overview
This project demonstrates the use of **Apache Airflow** for orchestrating an **ETL (Extract–Transform–Load)** pipeline.  
The goal is to **analyze and consolidate traffic data** collected from multiple toll plaza systems — each providing data in different formats — to help identify bottlenecks and decongest national highways.

Each toll operator has its own IT setup, so data arrives in:
- CSV format
- TSV format
- Fixed-width text format  

This DAG automates the process of unzipping, extracting, transforming, and consolidating this data into a single unified dataset.

## Technologies Used
- **Apache Airflow**
- **Python**
- **BashOperator**
- **Linux shell utilities** (`tar`, `cut`, `awk`, `paste`, `tr`)
- **CSV/TSV file handling**
- **ETL data transformation logic**
  
## DAG Details

| Parameter | Value |
|------------|--------|
| **DAG ID** | `ETL_toll_data` |
| **Description** | Apache Airflow Final Assignment — Toll Data ETL |
| **Owner** | `Sangeeta K` |
| **Email** | `srk@gmail.com` |
| **Start Date** | Today’s date |
| **Schedule** | Daily (`@daily`) |
| **Retries** | 1 |
| **Retry Delay** | 5 minutes |
| **Email on Failure** | True |
| **Email on Retry** | True |


## Task Pipeline


| Task | Operator | Description |
|------|-----------|-------------|
| **1. unzip_data** | `BashOperator` | Unzips the toll data archive (`tolldata.tgz`) |
| **2. extract_data_from_csv** | `BashOperator` | Extracts `Rowid`, `Timestamp`, `Anonymized Vehicle number`, and `Vehicle type` from CSV |
| **3. extract_data_from_tsv** | `BashOperator` | Extracts `Number of axles`, `Tollplaza id`, and `Tollplaza code` from TSV |
| **4. extract_data_from_fixed_width** | `BashOperator` | Extracts `Type of Payment code` and `Vehicle Code` from fixed-width text file |
| **5. consolidate_data** | `BashOperator` | Merges extracted data files into a single `extracted_data.csv` using the `paste` command |
| **6. transform_data** | `BashOperator` | Converts the `vehicle_type` field to uppercase using the `tr` command |

##  Data Flow Diagram


