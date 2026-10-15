# Enterprise Financial Pipeline — Medallion Architecture

A production-ready data pipeline built with **PySpark** and **Databricks Asset Bundles (DABs)** using **Databricks Serverless Compute**. The pipeline orchestrates financial data through a classic **Bronze → Silver → Gold** (Medallion) ETL/ELT framework.

---

## 🏛 Architecture Overview

The pipeline follows the Medallion Data Architecture to isolate data quality levels at each stage:

```text
[ Raw Financial Data ] 
       │
       ▼
   ┌──────┐  Raw ingestion / Schema enforcement
   │Bronze│  Delta Lake storage
   └──┬───┘
      │
      ▼
   ┌──────┐  Data cleaning, deduplication & normalization
   │Silver│  Business rules enforcement
   └──┬───┘
      │
      ▼
   ┌──────┐  Aggregated metrics & financial KPIs
   │ Gold │  Power BI / Analytics ready
   └──────┘
```

* **Bronze Layer (`src/bronze_ingestion.py`)**: Ingests raw source financial records into append-only Delta Lake tables with schema validation.
* **Silver Layer (`src/silver_transform.py`)**: Cleanses, deduplicates, filters invalid transactions, and applies business normalization.
* **Gold Layer (`src/gold_aggregation.py`)**: Computes executive financial metrics, daily/monthly summaries, and consolidated data marts.

---

## 🛠 Tech Stack & Tools

* **Core Engine:** PySpark, Delta Lake
* **Compute:** Databricks Serverless Compute (`environment_version: "5"`)
* **Orchestration & IaC:** Databricks Asset Bundles (DABs)
* **IDE & Control:** VS Code, Git / GitHub
* **OS Environment:** macOS

---

## 📁 Repository Structure

```text
.
├── databricks.yml                      # Main Asset Bundle configuration & targets
├── resources/
│   └── financial_pipeline_job.yml      # Workflow/Job definition & task dependencies
├── src/
│   ├── bronze_ingestion.py             # Layer 1: Ingestion logic
│   ├── silver_transform.py             # Layer 2: Transformation logic
│   └── gold_aggregation.py             # Layer 3: Aggregation logic
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

1. **Databricks CLI**: Ensure `databricks` CLI v0.200.0+ is installed and configured.
   ```bash
   databricks --version
   ```
2. **Databricks Authentication**: Configure workspace credentials.
   ```bash
   databricks auth login --host <your-databricks-workspace-url>
   ```

---

## ⚙️ Deployment & Execution Workflow

### 1. Validate the Asset Bundle
Verify the YAML structure and syntax before deploying:
```bash
databricks bundle validate
```

### 2. Deploy to Databricks
Package and upload the code and workflow definition to your Databricks Workspace:
```bash
databricks bundle deploy
```

### 3. Run the Pipeline
Trigger a manual execution of the deployed Serverless Job:
```bash
databricks bundle run enterprise_financial_pipeline_job
```

---

## 🔄 CI/CD & Version Control

To sync updates with your remote repository:

```bash
git add .
git commit -m "feat: updated medallion pipeline configuration"
git push origin main
```