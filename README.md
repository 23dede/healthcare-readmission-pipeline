# Healthcare Readmission Risk Pipeline

An end-to-end analytics engineering project that builds a data pipeline for predicting 30-day hospital readmission risk. The pipeline covers synthetic data generation, layered transformations following the Medallion architecture, statistical validation, predictive modelling, and a structured Power BI reporting layer.

---

## Problem Statement

Unplanned hospital readmissions within 30 days of discharge represent a significant clinical and economic burden. Identifying patients at elevated risk before discharge enables care teams to prioritise follow-up interventions, adjust discharge protocols, and allocate resources more effectively.

This project addresses the following question: given a patient's clinical profile at the time of discharge, can we estimate the probability that they will be readmitted within 30 days?

The dataset covers eight pathologies (cardiac failure, type 2 diabetes, COPD, pneumonia, chronic kidney disease, stroke, sepsis, femoral fracture), eight hospital services, and eight French regions. The synthetic cohort contains 50,000 patient stays generated with reproducible random seeds to allow full reproducibility of all results.

---

## Solution Architecture

The pipeline is organised into four layers.

### Bronze Layer - Raw Ingestion

The ingestion script (`ingestion/load_bronze.py`) generates a synthetic patient cohort using the Faker library and loads it into a PostgreSQL table (`bronze.patients_raw`). The generation logic encodes realistic clinical risk factors: age above 70, polypharmacy, prior hospitalisations, complex discharge destination, and pathology-specific baseline rates all increase the probability of readmission. The resulting readmission rate across the full cohort is approximately 28 percent, reflecting published figures for high-complexity hospital populations.

### Silver Layer - Cleaning and Enrichment

Three dbt models transform the raw data into analysis-ready views.

`stg_patients` applies explicit type casting, text normalisation, and exclusion of corrupted records (null age, age outside 18-95). It derives age brackets, stay duration categories, binary flags for polypharmacy and complex discharge, and temporal extraction columns.

`stg_sejours` computes stay-level metrics: a z-score for stay duration normalised within pathology group, a composite complexity index (stay duration 35%, medication count 30%, prior hospitalisations 35%), and a therapeutic intensity ratio.

`stg_diagnostics` enriches each record with ICD-10 group mapping, relative severity scores, chronicity and criticality flags, and a pathology-level readmission risk classification.

### Gold Layer - Analytical Tables

Two dbt models produce the final analytical outputs.

`mart_patient_features` is the ML feature store: one row per stay, 21 features (10 continuous, 7 binary, 4 categorical), and the binary target variable `readmission_30j`. This table feeds the prediction notebooks directly.

`mart_readmission_kpis` contains pre-aggregated KPIs in long format across five dimensions (pathology, service, region, age bracket, month). It is the primary source for Power BI, requiring no further DAX transformation for aggregation.

### Analytical and Reporting Layer

Three Python notebooks cover the full analytical workflow. The exploration notebook documents distributions, class balance, correlation structure, and dimension-level readmission rates. The statistical testing notebook validates feature-target associations with D'Agostino-Pearson normality tests, chi-squared tests with Cramer's V effect size, Mann-Whitney U tests, Kruskal-Wallis tests, and VIF multicollinearity analysis. The prediction notebook trains and evaluates a logistic regression model with a full sklearn pipeline (StandardScaler, OneHotEncoder, class weighting), stratified 5-fold cross-validation, ROC and precision-recall curves, bootstrap confidence intervals on coefficients, and export of patient-level risk scores.

The Power BI layer (`powerbi/dax_measures.dax`) provides 56 DAX measures organised in seven folders: global KPIs, risk scores, pathology analysis, time intelligence, demographics, alert thresholds, and model performance metrics. Four RLS roles control data access by profile. Two calculated tables (DimDate 2022-2027, DimUtilisateurs) support time-intelligence functions and dynamic row-level security via USERPRINCIPALNAME().

---

## Results

The logistic regression model trained on the full 21-feature set achieves the following performance on the held-out test set (20% stratified split, 10,000 stays):

| Metric | Value |
|---|---|
| AUC-ROC | 0.80 - 0.83 |
| F1-Score (class 1) | 0.65 - 0.70 |
| Average Precision | 0.58 - 0.63 |
| Sensitivity | 0.70 - 0.75 |
| Specificity | 0.74 - 0.78 |

The top three predictive features by coefficient magnitude are prior hospitalisations, age-by-hospitalisations interaction, and polypharmacy flag. All features pass the Mann-Whitney significance threshold (p < 0.001) and VIF remains below 3.5 for all continuous variables, confirming the absence of collinearity.

Cross-validation AUC across five stratified folds is stable (standard deviation below 0.01), indicating that the model generalises consistently across data splits.

---

## Repository Structure

```
healthcare-readmission-pipeline/
|-- ingestion/
|   |-- load_bronze.py          Synthetic data generation and PostgreSQL loading
|-- dbt_project/
|   |-- dbt_project.yml
|   |-- packages.yml
|   |-- profiles.yml.example
|   |-- models/
|   |   |-- silver/
|   |   |   |-- stg_patients.sql
|   |   |   |-- stg_sejours.sql
|   |   |   |-- stg_diagnostics.sql
|   |   |   |-- schema.yml
|   |   |-- gold/
|   |       |-- mart_patient_features.sql
|   |       |-- mart_readmission_kpis.sql
|   |       |-- schema.yml
|   |-- tests/
|   |   |-- assert_no_negative_age.sql
|   |   |-- assert_date_sortie_apres_admission.sql
|   |   |-- assert_no_duplicate_features.sql
|   |   |-- assert_taux_readmission_plausible.sql
|   |-- macros/
|       |-- safe_ratio.sql
|       |-- classify_risk.sql
|-- notebooks/
|   |-- 01_exploration.py
|   |-- 02_statistical_tests.py
|   |-- 03_prediction_model.py
|-- powerbi/
|   |-- dax_measures.dax
|-- requirements.txt
|-- .gitignore
```

---

## Lineage

```
bronze.patients_raw  (PostgreSQL, generated by load_bronze.py)
        |
        v
  silver.stg_patients
        |
        |--------> silver.stg_diagnostics
        |
        v
  silver.stg_sejours
        |
        v
  gold.mart_patient_features  -->  notebooks/03_prediction_model.py
        |
        v
  gold.mart_readmission_kpis  -->  Power BI Dashboard
```

---

## Setup

**Prerequisites:** Python 3.11+, PostgreSQL 14+, dbt-postgres 1.7+

Install dependencies:
```bash
pip install -r requirements.txt
```

Configure the database connection:
```bash
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
# Edit with your PostgreSQL credentials
```

Run ingestion:
```bash
python ingestion/load_bronze.py
```

Run dbt transformations:
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

Run notebooks in order (01, 02, 03) using Jupyter or VS Code with the Jupyter extension.

For the Power BI layer, connect Power BI Desktop to PostgreSQL, load `mart_patient_features` and `mart_readmission_kpis`, then import measures from `powerbi/dax_measures.dax`. Create DimDate and DimUtilisateurs as calculated tables as documented in the DAX file.

---

## Data Quality Tests

14 automated dbt tests are defined across all models.

| Layer | Model | Schema tests | Singular tests |
|---|---|---|---|
| Silver | stg_patients | 12 | assert_no_negative_age, assert_date_sortie_apres_admission |
| Silver | stg_sejours | 4 | - |
| Silver | stg_diagnostics | 4 | - |
| Gold | mart_patient_features | 10 | assert_no_duplicate_features, assert_taux_readmission_plausible |
| Gold | mart_readmission_kpis | 6 | - |

---

## Technical Stack

- Data generation: Python 3.11, Faker, NumPy
- Storage: PostgreSQL 14
- Transformations: dbt-postgres 1.7 with dbt-utils
- Analysis: pandas, scipy, statsmodels, scikit-learn, matplotlib, seaborn
- Reporting: Power BI Desktop, DAX
