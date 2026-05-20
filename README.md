# Omnichannel Retail Data Pipeline | Python

An end-to-end Python ELT project that consolidates omnichannel retail, payment, and customer-behavior data into a Google BigQuery warehouse for downstream analytics.

This repository is designed for technical readers who want to understand how the pipeline ingests raw data from multiple operational systems, standardizes inconsistent schemas, applies data quality checks, loads analytics-ready tables, and publishes derived views for customer journey, payment, and cashflow analysis.

## Table of Contents

- [1. Overview](#1-overview)
- [2. Business Context](#2-business-context)
- [3. Architecture and Design](#3-architecture-and-design)
- [4. Data Sources](#4-data-sources)
- [5. Core Features](#5-core-features)
- [6. Data Model](#6-data-model)
- [7. Analytics Views](#7-analytics-views)
- [8. Results in BigQuery](#8-results-in-bigquery)
- [9. Repository Structure](#9-repository-structure)
- [10. Getting Started](#10-getting-started)
- [11. How to Run](#11-how-to-run)
- [12. Tech Stack](#12-tech-stack)

## 1. Overview

This project builds a centralized analytics layer for an omnichannel retail business operating across:

- e-commerce platforms
- offline/store systems
- direct online orders
- digital payment gateways
- bank transaction logs
- customer tracking events

The pipeline extracts raw files from Google Cloud Storage, transforms them in pandas, loads curated fact and dimension tables into BigQuery, updates customer-level aggregates, and creates reusable SQL views for BI consumption.

## 2. Business Context

Retail and payment data often live in disconnected systems. Orders, products, payment transactions, customer information, and user behavior events usually follow different schemas, naming conventions, and quality standards. That makes reporting slow, manual, and difficult to trust.

This project addresses that problem by creating a single analytics-ready warehouse that supports questions such as:

- Which channels generate the most revenue?
- How healthy is payment collection and outstanding receivables?
- What does customer journey behavior look like before purchase?
- How does cashflow evolve over time?
- Which customers are active, loyal, VIP, or still unclassified?

## 3. Architecture and Design

### 3.1 High-Level Architecture
<img width="1916" height="821" alt="image" src="https://github.com/user-attachments/assets/84245742-35c4-4d9b-80f8-4e43c4e96710" />

Figure 1: High-Level End-to-End Architecture

Raw files from all sources land in Google Cloud Storage (GCS) as the single source of truth. A Python orchestrator coordinates the extract, transform, and load phases in strict dependency order. Cleaned and enriched data is then loaded into Google BigQuery as a star-schema warehouse, followed by post-load aggregate updates and analysis view creation for BI reporting.

### 3.2 Medallion Architecture (ETL Flow)

<img width="1086" height="1449" alt="image" src="https://github.com/user-attachments/assets/b77191e4-b232-49bb-9d31-66515db7da03" />

Figure 2: Data Transformation Pipeline (Bronze → Silver → Gold)

Data flows through three layers:

| Layer | Location | Description |
| --- | --- | --- |
| Bronze (Raw) | Google Cloud Storage | Compressed `.json.gz` source files are ingested and stored as-is. No transformations are applied at this stage. |
| Silver (Cleaned) | In-memory pandas processing | Schemas are standardized, data types are fixed, surrogate keys are generated, nested structures are flattened, and data quality checks are applied. |
| Gold (Serving) | Google BigQuery | Dimension and fact tables are loaded into the warehouse, customer aggregates are updated, and analytical SQL views are created for reporting. |

### 3.3 Pipeline Flow

1. Extract raw data from GCS using source-specific extractor classes.
2. Standardize schemas, cast types, generate surrogate keys, and handle missing values.
3. Run table-specific data quality checks before loading.
4. Load star-schema tables into BigQuery with partitioning and clustering where applicable.
5. Update customer lifetime metrics and segmentation after fact tables are loaded.
6. Create reusable analytical views for BI dashboards.

## 4. Data Sources

The pipeline integrates multiple source families:

### 4.1 Sales and Commerce

- Shopify orders
- Sapo orders and locations
- Direct online orders
- Product catalog
- Customer master data

### 4.2 Payments and Banking

- MoMo
- ZaloPay
- PayPal
- Odoo / Sapo payment exports
- Mercury bank transaction logs

### 4.3 Customer Behavior

- cart and browsing event tracking
- session-level events such as `view_item`, `add_to_cart`, `begin_checkout`, and `purchase`

## 5. Core Features

- Modular OOP pipeline architecture with dedicated extractors, transformers, loader, and orchestrator
- Multi-source schema harmonization across retail, payment, and tracking systems
- Automated surrogate key generation for facts
- Shipping city and shipping address enrichment for online orders
- Built-in data quality checks for nulls, duplicates, dates, and numeric anomalies
- BigQuery loading with support for partitioning, clustering, and configurable write disposition
- Automatic customer aggregate refresh after each run
- Customer segmentation logic maintained directly in the warehouse
- Analysis-ready SQL views for journey, sankey flow, cashflow, and payment status reporting

## 6. Data Model

The BigQuery warehouse follows a star-schema style design.

<img width="1423" height="1310" alt="image" src="https://github.com/user-attachments/assets/24b48b49-5dc3-4457-ad6b-745bcbd39539" />

Figure 3: Star Schema Data Model

### 6.1 Dimension Tables

| Table | Description |
| --- | --- |
| `dim_customers` | Customer profile, geography, aggregate metrics, and segment |
| `dim_products` | Product catalog, pricing, category, brand, and stock fields |
| `dim_date` | Calendar dimension with fiscal and reporting attributes |

### 6.2 Fact Tables

| Table | Description |
| --- | --- |
| `fact_orders` | Unified order-level facts across channels |
| `fact_order_items` | Order line-item detail |
| `fact_payments` | Payment transactions by gateway and method |
| `fact_cart_events` | Customer behavior and funnel tracking events |
| `fact_bank_transactions` | Bank movements by account and transaction type |

### 6.3 Customer Segmentation Logic

After each run, the pipeline updates customer-level metrics in `dim_customers` and reclassifies segments using warehouse-side SQL:

- `vip`: `total_orders >= 10` or `lifetime_value_vnd >= 10,000,000`
- `loyal`: `total_orders >= 5` or `lifetime_value_vnd >= 5,000,000`
- `active`: `total_orders >= 1`
- `unknown`: no qualifying purchase history or missing segment input

## 7. Analytics Views

The pipeline creates analysis-ready views directly in BigQuery:

| View | Purpose |
| --- | --- |
| `vw_customer_journey` | Customer-level touchpoint sequence and purchase timing |
| `vw_customer_journey_sankey` | Pre-shaped edge table for Sankey journey visualizations |
| `vw_cashflow_daily` | Daily revenue, payment, and bank cashflow rollup |
| `vw_payment_status` | Order vs payment matching, delays, and outstanding amount |

These views are intended to reduce modeling effort in Power BI and keep business logic centralized in SQL where possible.

## 8. Results in BigQuery

After a successful pipeline run, the curated outputs are available in the `anh27tuan02.Ancestry` BigQuery dataset.

### 8.1 Base Tables

| Table | Type | Snapshot Row Count |
| --- | --- | ---: |
| `dim_customers` | Dimension | 100,000 |
| `dim_products` | Dimension | 1,300 |
| `dim_date` | Dimension | 366 |
| `fact_orders` | Fact | 700,000 |
| `fact_order_items` | Fact | 2,089,696 |
| `fact_payments` | Fact | 111,000 |
| `fact_cart_events` | Fact | 3,010,000 |
| `fact_bank_transactions` | Fact | 5,000 |

<img width="3259" height="1601" alt="image" src="https://github.com/user-attachments/assets/622319aa-0413-437d-b95b-a72d291f861e" />

Figure 4: `dim_customers` table

<img width="2532" height="1543" alt="image" src="https://github.com/user-attachments/assets/6e7a854d-c84b-4158-8339-d7e06fa996b7" />

Figure 5: `dim_products` table

<img width="3249" height="1568" alt="image" src="https://github.com/user-attachments/assets/82774db8-f7e9-4639-881c-ad5c501f6b9d" />

Figure 6: `fact_orders` table

<img width="3264" height="1616" alt="image" src="https://github.com/user-attachments/assets/c11e67fa-7d27-4ffa-8922-bddfb11e5abd" />

Figure 7: `fact_order_items` table

<img width="3266" height="1622" alt="image" src="https://github.com/user-attachments/assets/17a3c369-a6b2-4745-ac23-14ac5149eaff" />

Figure 8: `fact_payments` table

<img width="3244" height="1611" alt="image" src="https://github.com/user-attachments/assets/1aabec72-f24d-441a-868a-67b60a324cbb" />

Figure 9: `fact_cart_events` table

<img width="3237" height="1616" alt="image" src="https://github.com/user-attachments/assets/9771037d-9f87-47e4-b43c-fe9f030e607c" />

Figure 10: `fact_bank_transactions` table

> Insert screenshot of the `fact_bank_transactions` table here.

### 8.3 Reporting Views

| View | Purpose |
| --- | --- |
| `vw_customer_journey` | Customer-level touchpoint and purchase path analysis |
| `vw_customer_journey_sankey` | Pre-shaped edge data for Sankey visualizations |
| `vw_cashflow_daily` | Daily revenue, payment, and bank cashflow rollup |
| `vw_payment_status` | Payment status, outstanding amount, and collection delay tracking |

<img width="2496" height="1530" alt="image" src="https://github.com/user-attachments/assets/2ffcf6d6-0b17-469e-869e-35f0499b8672" />

Figure 11: `vw_customer_journey` view

<img width="2492" height="1508" alt="image" src="https://github.com/user-attachments/assets/ecb87725-24fc-480c-9909-00b5e0a8532d" />

Figure 12: `vw_cashflow_daily` view

<img width="2499" height="1504" alt="image" src="https://github.com/user-attachments/assets/de3dff70-4889-4572-9741-0004a80cb40b" />

Figure 13: `vw_payment_status` view

## 9. Repository Structure

```text
.
├── config/
├── extractors/
│   ├── base_extractor.py
│   ├── customers_extractor.py
│   ├── online_orders_extractor.py
│   ├── payment_extractor.py
│   ├── products_extractor.py
│   ├── sapo_extractor.py
│   ├── shopify_extractor.py
│   ├── tracking_extractor.py
│   └── __init__.py
├── loaders/
│   ├── bigquery_loader.py
│   └── __init__.py
├── logs/
├── orchestration/
│   ├── pipeline_orchestrator.py
│   └── __init__.py
├── tests/
│   ├── fixtures/
│   ├── test_base_transformer.py
│   ├── test_bigquery_loader.py
│   ├── test_customers_extractor.py
│   ├── test_dimension_transformer.py
│   ├── test_fact_transformer.py
│   ├── test_payment_extractor.py
│   ├── test_pipeline_integration.py
│   ├── test_pipeline_orchestrator.py
│   ├── test_sapo_extractor.py
│   └── test_shopify_extractor.py
├── transformers/
│   ├── base_transformer.py
│   ├── dimension_transformer.py
│   ├── fact_transformer.py
│   └── __init__.py
├── utils/
│   ├── gcs_helper.py
│   ├── logger.py
│   └── __init__.py
├── .env
├── main.py
├── README.md
└── requirements.txt
```

## 10. Getting Started

### 10.1 Prerequisites

- Python 3
- A Google Cloud project
- Access to:
  - Google Cloud Storage bucket with source files
  - BigQuery dataset permissions
  - a service account JSON key

### 10.2 Install Dependencies

```powershell
python -m pip install -r requirements.txt
```

### 10.3 Environment Variables

Create a `.env` file in the project root:

```env
GOOGLE_APPLICATION_CREDENTIALS="config/service-account.json"
GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
BIGQUERY_LOCATION="US"
GCS_BUCKET_NAME="your-source-bucket"
BIGQUERY_DATASET="Ancestry"

# Optional
PIPELINE_START_DATE="2025-01-01T00:00:00Z"
PIPELINE_END_DATE="2025-12-31T23:59:59Z"
PIPELINE_HOLIDAYS="2025-01-01,2025-04-30,2025-09-02"
```

Notes:

- `PIPELINE_START_DATE` and `PIPELINE_END_DATE` are optional. If omitted, the pipeline can infer date coverage from fact data.
- The default write disposition is `WRITE_TRUNCATE`.

## 11. How to Run

### 11.1 Run With `.env` Defaults

```powershell
python main.py
```

### 11.2 Run With Explicit Parameters

```powershell
python main.py `
  --project-id your-gcp-project-id `
  --dataset-id Ancestry `
  --location US `
  --bucket-name your-source-bucket
```

### 11.3 Optional Flags

```powershell
python main.py `
  --start-date 2025-01-01T00:00:00Z `
  --end-date 2025-12-31T23:59:59Z `
  --holiday 2025-01-01 `
  --holiday 2025-09-02 `
  --write-disposition WRITE_TRUNCATE
```


## 12. Tech Stack

- Python
- pandas
- NumPy
- Google Cloud Storage
- Google BigQuery
- Power BI
- dotenv-based configuration

---
