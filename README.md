---

#  Simple Data Warehouse

This project is a **simple, end-to-end ELT pipeline** that demonstrates how to:

* **Extract** data from a production (OLTP) database
* **Load** it into a PostgreSQL data warehouse (OLAP)
* **Transform** it into analytics-ready tables using dbt
* **Orchestrate** everything using Apache Airflow

---

##  System Design

![Production Design](docs/prod_design_simple.png)

---

##  Technologies Used

* **Docker & Docker Compose** — containerize and orchestrate services
* **PostgreSQL** — used for both the OLTP (production) and OLAP (warehouse) layers
* **Python** — handles the Extract & Load (EL) phase

  * `psycopg2-binary` for PostgreSQL connections
  * `pandas` for data manipulation
* **dbt (Data Build Tool)** — manages data transformations (T)
* **Apache Airflow** — orchestrates the entire ELT pipeline

---

##  Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── orchestrator.py           # Airflow DAG for pipeline orchestration
│   └── Dockerfile                    # (Optional) custom Dockerfile for Airflow
│
├── docs/
│   ├── prod_design_simple.png        # Simplified production DB schema diagram
│   └── thinking.txt                  # Notes on design decisions
│
├── scripts/
│   ├── prod/
│   │   ├── schema_simple.sql         # Defines the production (OLTP) schema
│   │   └── seed_simple.py            # Seeds production DB with sample data
│   └── dw/
│       ├── el.py                     # Extract & Load script (Python)
│       ├── init_wh_simple.sql        # Initializes data warehouse schemas
│       └── xyz_store_analytics/      # dbt project for transformations
│
├── docker-compose.yaml               # Defines all services (DBs + Airflow)
└── pyproject.toml                    # Python dependencies
```

---

##  Getting Started

### 1️ Prerequisites

Make sure you have:

* Docker
* Docker Compose

---

### 2️ Setup and Run

**Step 1: Clone the repository**

```bash
git clone <repository-url>
cd simple-datawarehouse
```

**Step 2: Build and start all services**

```bash
docker-compose up --build
```

This command starts:

* `production_db` — OLTP source database
* `dwh` — OLAP data warehouse
* `airflow_orchestrator` — Airflow for orchestration

---

### 3️ Seed the Production Database

Once containers are running, initialize and seed the production database:

```bash
docker exec -it production_db psql -U admin -d xyz_store < scripts/prod/schema_simple.sql
python scripts/prod/seed_simple.py
```

---

##  Pipeline Overview

This project implements an **ELT** pipeline:

1. **Extract & Load (EL)** — `scripts/dw/el.py`

   * Connects to the production DB
   * Extracts new or updated records (incrementally using high-watermark logic)
   * Loads them into the **bronze** layer of the data warehouse

2. **Transform (T)** — dbt

   * Transforms data from `bronze` → `silver` → `gold`
   * Creates cleaned, conformed, and aggregated tables

![Pipeline Graph](docs/wh-elt-pipeline-graph%20\(2\).png)

---

##  Data Modeling

The data warehouse follows the **Medallion Architecture**:

| Layer      | Purpose                                        | Example Tables                                            |
| ---------- | ---------------------------------------------- | --------------------------------------------------------- |
| **Bronze** | Raw, uncleaned data from the source system     | `raw_orders`, `raw_customers`                             |
| **Silver** | Cleaned and conformed data                     | `clean_orders`, `clean_customers`                         |
| **Gold**   | Aggregated, analytics-ready data (Star Schema) | `fact_sales`, `dim_customers`, `dim_products`, `dim_date` |

---

##  Orchestration (Airflow)

The entire pipeline is managed by **Apache Airflow**, via the DAG defined in:

```
airflow/dags/orchestrator.py
```

### DAG Tasks:

1. **`extract_and_load_bronze`** — runs `el.py` to perform extraction and loading
2. **`dbt_run_models`** — executes dbt transformations
3. **`dbt_test_models`** — runs dbt tests for data quality
4. **`dbt_return_sample`** — (optional) returns a preview of transformed data

Access the Airflow UI at:
👉 **[http://localhost:8000](http://localhost:8000)**

---

##  Future Enhancements

* Add advanced dbt transformations and business metrics
* Implement data quality checks and tests in dbt
* Add detailed logging and monitoring for EL and T processes
* Automate scheduling via Airflow

---

##  Summary

| Stage              | Tool                          | Purpose                              |
| ------------------ | ----------------------------- | ------------------------------------ |
| **Extract & Load** | Python (`psycopg2`, `pandas`) | Pull data from OLTP and load into DW |
| **Transform**      | dbt                           | Clean, model, and aggregate          |
| **Orchestrate**    | Airflow                       | Automate the full ELT process        |
| **Store**          | PostgreSQL                    | Persist both OLTP and OLAP data      |

---

