# Healthcare Readmission Pipeline

End-to-end analytics pipeline for 30-day hospital readmission risk prediction.
Architecture: Medallion (Bronze / Silver / Gold) -- dbt -- PostgreSQL -- Python -- Power BI.

---

## Problem Statement

Unplanned hospital readmissions within 30 days of discharge represent one of the most
significant quality and cost challenges in healthcare. In France, readmission rates
consistently exceed 15% across major pathologies such as heart failure, COPD, and
sepsis. Each preventable readmission carries both a direct financial cost for the
healthcare system and a measurable clinical risk for the patient.

The challenge addressed by this project is the following: given a set of clinical and
administrative variables collected at the time of a hospital stay, can we predict with
sufficient accuracy which patients are at high risk of being readmitted within 30 days
of discharge?

Early identification of at-risk patients allows care teams to act before discharge:
reinforce medication education, schedule follow-up appointments, coordinate with home
care services, or extend the hospital stay when clinically justified.

---

## Solution

This project builds a complete data pipeline from raw data ingestion to a Power BI
business intelligence layer, structured around three analytical stages:

**Stage 1 -- Data Engineering (Bronze and Silver layers)**

Raw synthetic patient data is generated using the Python Faker library and ingested
into PostgreSQL. The Silver layer, implemented in dbt, applies data cleaning, type
casting, deduplication, and feature derivation. Three staging models are produced:
patient demographics, stay metrics, and diagnostic enrichment via a simplified
ICD-10 reference table.

**Stage 2 -- Machine Learning and Statistical Analysis (Gold layer and Python notebooks)**

The Gold layer consolidates the Silver models into a feature store table consumed
by the prediction pipeline. Three Jupyter notebooks cover exploratory data analysis,
statistical validation of features (chi-square, Mann-Whitney, VIF), and a logistic
regression model with stratified cross-validation. The model outputs a continuous
risk score between 0 and 1 for each patient stay.

**Stage 3 -- Business Intelligence (Power BI)**

The risk scores and aggregated KPIs are exposed in a Power BI semantic model
with 56 DAX measures organized into 7 folders, row-level security for 4 user
profiles, a calendar dimension for time-intelligence functions, and a user mapping
table for dynamic RLS filtering.

---

## Results

| Metric              | Target value |
|---------------------|-------------|
| AUC-ROC             | >= 0.78     |
| F1-Score (class 1)  | >= 0.65     |
| Overall accuracy    | >= 0.76     |
| KS Statistic        | >= 0.30     |

Statistical validation confirms that all selected features are significant at the
0.001 level. No multicollinearity is detected (all VIF values below 5). The
non-normal distribution of continuous variables is handled with non-parametric
tests (Mann-Whitney U, Kruskal-Wallis).

The Power BI model enables hospital staff to monitor readmission rates by service,
region, pathology, and patient risk tier, with real-time alert thresholds and
year-over-year comparison.

---

## Architecture

```
Source: Synthetic hospital data (Python / Faker)
  |
  v
Bronze Layer
  load_bronze.py
  PostgreSQL -- schema: bronze
  Table: bronze.patients_raw
  No transformation -- raw ingestion with timestamp logging
  |
  v
Silver Layer
  dbt models -- materialized as views
  stg_patients     : type casting, normalization, quality filters
  stg_sejours      : stay metrics, complexity index, risk flags
  stg_diagnostics  : ICD-10 enrichment, clinical group mapping
  |
  v
Gold Layer
  dbt models -- materialized as tables
  mart_patient_features : feature store for ML (21 features)
  mart_readmission_kpis : pre-aggregated KPIs for Power BI (5 dimensions)
  |
  v
Python Analysis
  01_exploration.ipynb       : EDA, distributions, correlation
  02_statistical_tests.ipynb : chi-square, Mann-Whitney, VIF, normality
  03_prediction_model.ipynb  : logistic regression, cross-validation, risk scores
  Output: scores_risque_patients.csv
  |
  v
Power BI Semantic Model
  56 DAX measures across 7 folders
  4 RLS roles (Direction, Service Manager, Risk Analyst, Physician)
  DimDate 2022-2027 for time-intelligence
  DimUtilisateurs for dynamic RLS via USERPRINCIPALNAME()
```

---

## Project Structure

```
healthcare-readmission-pipeline/
|
|-- README.md
|-- requirements.txt
|-- load_bronze.py
|
|-- dbt_project/
|   |-- dbt_project.yml
|   |-- packages.yml
|   |-- profiles.yml.example
|   |-- models/
|   |   |-- silver/
|   |   |   |-- stg_patients.sql
|   |   |   |-- stg_sejours.sql
|   |   |   |-- stg_diagnostics.sql
|   |   |   `-- schema.yml
|   |   `-- gold/
|   |       |-- mart_patient_features.sql
|   |       |-- mart_readmission_kpis.sql
|   |       `-- schema.yml
|   |-- macros/
|   |   |-- classify_risk.sql
|   |   `-- safe_ratio.sql
|   `-- tests/
|       |-- assert_no_negative_age.sql
|       |-- assert_no_duplicate_features.sql
|       |-- assert_date_sortie_apres_admission.sql
|       `-- assert_taux_readmission_plausible.sql
|
|-- analysis/
|   |-- 01_exploration.ipynb
|   |-- 02_statistical_tests.ipynb
|   `-- 03_prediction_model.ipynb
|
`-- powerbi/
    `-- dax_measures.dax
```

---

## Dataset

Data is entirely synthetic, generated with the Python Faker library. The generation
logic is designed to produce medically coherent distributions: older patients with
multiple prior hospitalizations, high medication counts, or complex discharge modes
have a structurally higher probability of readmission.

Approximately 50,000 patient stays are generated, covering 8 pathologies aligned
with the French hospital system (GHM / ICD-10).

| Variable                        | Type        | Description                              |
|---------------------------------|-------------|------------------------------------------|
| patient_id                      | UUID        | Unique stay identifier                   |
| age                             | Integer     | Patient age at admission (18-95)         |
| sexe                            | Categorical | M / F                                    |
| pathologie                      | Categorical | Heart failure, COPD, Diabetes, etc.      |
| diagnostic_principal            | String      | Simplified ICD-10 code                   |
| service                         | Categorical | Cardiology, Pulmonology, etc.            |
| hopital_region                  | Categorical | French administrative region             |
| duree_sejour                    | Integer     | Length of stay in days (1-45)            |
| nb_hospitalisations_precedentes | Integer     | Prior hospitalizations (12-month window) |
| nb_medicaments                  | Integer     | Number of prescribed medications         |
| mode_sortie                     | Categorical | Home, SSR, EHPAD, Transfer, Death        |
| readmission_30j                 | Binary      | Target variable (0 / 1)                  |

---

## Statistical and Modeling Approach

The modeling pipeline follows a rigorous statistical workflow:

1. Normality testing (D'Agostino-Pearson) on all continuous variables.
   All variables are confirmed non-normal, justifying the use of non-parametric tests.

2. Feature significance testing.
   Chi-square test for categorical variables vs. the target (pathology, service,
   discharge mode, sex). Cramer's V is computed for effect size.
   Mann-Whitney U test for continuous variables between readmission groups.
   Effect size r is reported alongside p-values.

3. Multicollinearity check (VIF).
   All features show VIF below 5, confirming the absence of problematic collinearity.

4. Kruskal-Wallis test for age distribution across pathology groups.

5. Logistic regression with class_weight='balanced' to handle the natural
   class imbalance (approximately 25-30% positive class). Pipeline includes
   StandardScaler for numerical features and OneHotEncoder for categorical features.

6. Validation via stratified 5-fold cross-validation. Bootstrap confidence
   intervals (200 iterations) are computed for AUC-ROC, F1, precision, and recall.
   Feature importance is extracted from model coefficients with 95% bootstrap intervals.

---

## Technical Stack

| Layer                 | Technology                                  |
|-----------------------|---------------------------------------------|
| Data generation       | Python 3.11, Faker 24.0                     |
| Storage               | PostgreSQL 15                               |
| Transformation        | dbt Core                                    |
| Statistical analysis  | scipy, statsmodels                          |
| Machine learning      | scikit-learn 1.4                            |
| Business intelligence | Power BI Desktop, DAX                       |
| Version control       | Git / GitHub                                |

---

## Setup and Usage

**1. Clone the repository**

```bash
git clone https://github.com/23dede/healthcare-readmission-pipeline.git
cd healthcare-readmission-pipeline
```

**2. Install Python dependencies**

```bash
pip install -r requirements.txt
```

**3. Configure PostgreSQL**

Create the database before running the ingestion script:

```bash
psql -U postgres -c "CREATE DATABASE healthcare_dw;"
```

Database connection parameters can be set via environment variables:
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD.
Default values point to localhost:5432/healthcare_dw.

**4. Run the Bronze ingestion**

```bash
python load_bronze.py --rows 50000 --batch 1000
```

This script generates the synthetic dataset, saves a CSV to data/raw/, and
inserts all records into bronze.patients_raw. Each run is logged in
bronze.ingestion_logs.

**5. Run the dbt pipeline**

```bash
cd dbt_project
cp profiles.yml.example ~/.dbt/profiles.yml
dbt deps
dbt run
dbt test
```

**6. Run the analysis notebooks**

Open and execute in order:
- analysis/01_exploration.ipynb
- analysis/02_statistical_tests.ipynb
- analysis/03_prediction_model.ipynb

The third notebook exports scores_risque_patients.csv, which feeds the Power BI model.

**7. Power BI configuration**

Load mart_patient_features, mart_readmission_kpis, and scores_risque_patients.csv
into Power BI Desktop. DAX measures are documented in powerbi/dax_measures.dax.
The semantic model requires a DimDate table (2022-2027) and a DimUtilisateurs
mapping table for dynamic RLS.

---

## Power BI Semantic Model

The Power BI layer contains 56 DAX measures organized in 7 folders:

- KPIs Globaux: total stays, readmission rate, average length of stay, age, medications
- Scores de Risque: risk score distribution, high/moderate/low risk patient counts
- Analyse Pathologie: pathology-level rates, deviation from global average, ranking
- Analyse Temporelle: YTD, rolling 30 days, year-over-year delta, trend label
- Demographie: senior (75+) rates, prior hospitalizations, male vs. female differential
- Alertes et Seuils: alert color encoding, performance status, services above threshold
- Metriques Modele: confusion matrix components, precision, recall, F1, KS statistic

Row-level security is implemented for four roles:

- Direction_Hopital: unrestricted access
- Responsable_Service: filtered to the user's service via USERPRINCIPALNAME() lookup
- Analyste_Risques: filtered to patients with risk score >= 0.40
- Medecin_Traitant: filtered to the user's service via USERPRINCIPALNAME() lookup

---

## License

MIT License. All data used in this project is entirely synthetic.
No real patient data was used at any stage of development.
