# ⚡ Power Outages Data Loader (ANEEL)

This project automates the ingestion of annual power outage datasets (CSV format) into an Oracle database, following preprocessing and validation steps. The datasets, published by ANEEL (Brazilian Electric Energy Agency), contain detailed records of electricity supply interruptions across various regions and utility providers in Brazil.

## 📌 Project Overview

The script processes CSV files from 2017 to 2023 and loads the cleaned and validated data into a designated Oracle table for each year. It ensures consistent formatting, handles missing values, and supports secure Oracle connections using the `keyring` library.

---

## 🧩 Features

- 🔄 **Automatic batch loading** of large datasets into Oracle using `executemany`.
- 🧹 **Data cleaning**: string trimming, handling of `NaN`, and data type standardization.
- 🗃️ **Database schema** provided in SQL format to ensure correct table structure.
- 🔐 **Secure database connection** using system keyring credentials.
- 📊 **Query examples** to filter and analyze duration and frequency of outages by region.

---

## 🛠️ Technologies Used

- Python (`pandas`, `numpy`, `cx_Oracle`, `keyring`)
- Oracle Database
- CSV files encoded in ANSI
- SQL for schema creation and analytics

---

## 📂 Folder Structure

```
project-root/
│
├── interrupcoes_ANEEL_20240202.py   # Main script for processing and loading data
├── Tabela_Oracle.sql                # SQL script to create and query the Oracle table
└── data/
    └── interrupcoes-energia-eletrica-YYYY.csv  # Raw input files from ANEEL for each year
```

---

## 🧪 Execution Workflow

1. **File Reading**: Load CSV file from a specified path.
2. **Data Treatment**: 
   - Convert all columns to strings.
   - Replace 'nan' with None.
   - Strip white spaces.
   - Cast specific columns to integers (`NumNivelTensao`, `NumUnidadeConsumidora`).
3. **Database Insertion**:
   - Connect to Oracle DB using credentials stored in `keyring`.
   - Truncate target table for the year.
   - Insert rows in batches of 50,000.
4. **Logging**:
   - Prints log of file read, data cleaning, and batch uploads.

---

## 🧠 Sample SQL Analytics

- Filter valid outages based on duration and cause.
- Aggregate metrics like total hours interrupted and number of outages per utility.
- Example metrics:
  - Average hours interrupted per incident
  - Outage durations sorted by longest/shortest

---

## ✅ Requirements

- Python 3.x
- Oracle Client libraries installed
- System keyring configured with DB credentials

