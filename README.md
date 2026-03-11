# Healthcare Readmission Pipeline

End-to-end analytics pipeline for 30-day hospital readmission risk prediction.
Architecture: Medallion (Bronze / Silver / Gold) — dbt — PostgreSQL — Python — Power BI.

---

## Power BI Report

> **[Download project_medica_version.pbix](https://github.com/23dede/healthcare-readmission-pipeline/raw/main/project_medica_version.pbix)**
> 56 DAX measures — 7 folders — 4 RLS roles — DimDate 2022–2027

The report connects directly to the Gold layer output and provides two analytical dashboards:
a general readmission overview with KPIs and time trends, and a deep-dive analysis by pathology
and hospital service.

---

## Dashboard 1 — General Readmission Overview

![General Readmission Overview](Vue%20G%C3%A9n%C3%A9rale%20des%20Readmissions.png)

This dashboard provides a high-level view of hospital readmission activity across the entire patient population.
It is designed for hospital directors and risk analysts who need a quick, reliable summary of the current
readmission situation.

### Chart — Total Readmissions by Admission Date (line chart, top left)

The line chart plots the total number of readmissions on the vertical axis against admission dates on the
horizontal axis, covering a period from early July to late July. Each data point on the curve represents
the number of patients readmitted on a given date. The line shows a marked decline from around 9–10
readmissions per day in early July down to approximately 7–8 in mid-July, followed by a sharp recovery
peaking at 10 around July 17, before dropping again toward the end of the month. This temporal view is
critical for detecting daily or weekly spikes that may signal capacity stress, staffing shortfalls, or
the failure of a discharge prevention protocol. A sustained upward trend across several consecutive days
would trigger a management review.

### Chart — Total Stays by Risk Category (donut chart, top center)

The donut chart breaks down all recorded hospital stays into three risk tiers. The dominant segment (in
blue) represents the **Low risk** category, which accounts for the vast majority of stays. The **Very
low risk** (orange) and **Moderate risk** (purple) segments are smaller slices. Exact percentages are
displayed on each arc: Very low 11.58%, Moderate 10.83%, with the remainder being Low. This distribution
confirms that the model correctly identifies most stays as low risk while isolating a meaningful subset
of higher-risk patients who warrant targeted intervention before discharge.

### Chart — Readmission Rate by Pathology (horizontal bar chart, bottom left)

This chart ranks two pathologies — **Hip fracture** (Fracture col femur) and **Pneumonia** (Pneumonie)
— by their respective readmission rates as a percentage. Each bar has a small error band visible around
it, indicating that the rate estimate carries some variability. Hip fracture shows a slightly higher
readmission rate than pneumonia. For hospital managers, this means that orthopedic and geriatric
pathways require more intensive post-discharge follow-up than respiratory ones, based on current data.

### Chart — Total Stays by Admission Season (bar chart, bottom center)

Four vertical bars represent the four seasons: Summer (Été), Spring (Printemps), Autumn (Automne),
and Winter (Hiver). All bars reach approximately 300 stays, indicating that admission volume is
relatively uniform across seasons. The summer bar appears marginally taller in darker shading,
possibly reflecting a slightly higher case load. The absence of a strong seasonal effect simplifies
capacity planning, though month-level filtering is still recommended to detect sub-seasonal peaks.

### KPI Cards (right panel)

| KPI card | Value | Meaning |
|---|---|---|
| Total Stays | 301 | Total number of hospital stays recorded in the dataset |
| Total Readmissions | 64 | Number of patients readmitted within 30 days of discharge |
| Readmission Rate | 21.3% | Proportion of stays followed by a readmission within 30 days |
| Average Length of Stay | 14.9 days | Mean duration of a hospital stay across all records |

A **21.3% readmission rate** is above the French national average benchmark of ~15%, indicating that
the synthetic patient population modeled here represents a high-risk cohort, consistent with the
pathology mix used in data generation.

---

## Dashboard 2 — Analysis by Pathology and Service

![Analysis by Pathology and Service](Analyse%20par%20Pathologie%20%26%20Service.png)

This dashboard provides a clinical decomposition of the readmission risk profile across pathologies
and hospital services. It is intended for department heads, service managers, and clinical risk analysts
who need to understand which patient groups drive the most risk and how resources should be allocated.

### Chart — Average Risk Score by Pathology (horizontal bar chart, top left)

Each horizontal bar represents one of eight pathologies, ranked by average risk score from highest
to lowest. The pathologies covered are: Hip fracture (Fracture col femur), Pneumonia, Ischemic stroke
(AVC ischemique), Heart failure (Insuffisance cardiaque), Type 2 diabetes (Diabete de type 2), COPD
(BPCO), Sepsis, and Chronic kidney disease (IRC). Hip fracture and Pneumonia display the longest bars,
indicating the highest average predicted risk scores. This ranking directly informs which patient
cohorts should be prioritized by discharge prevention teams. A high average risk score means that, on
average, patients admitted with that condition are predicted to have a significant probability of
returning within 30 days.

### Chart — Average Age and Average Complexity Index by Pathology (scatter plot, top center)

Each dot on the scatter plot represents one pathology. The horizontal axis shows average patient age
at admission, ranging from approximately 55.5 to 57.5 years. The vertical axis shows the average
complexity index, ranging from roughly 3 to 5. The complexity index is a composite score that reflects
the clinical difficulty of a stay, derived from the number of prescribed medications, number of prior
hospitalizations, and discharge mode. Dots positioned in the upper-right quadrant represent pathologies
with both older patients and higher complexity — those are the highest-priority segments for care
coordination. The scatter plot reveals whether age alone or overall complexity is the primary driver
of risk, guiding both clinical and administrative decisions.

### Chart — Average Prior Hospitalizations by Clinical Group (pie chart, top right)

The pie chart divides patients into clinical groups: TRAUMA, NEURO, METABOL, PULMO, CARDIO, INFECTIO,
NEPHRO. Each slice shows the average number of prior hospitalizations (in the last 12 months) for
patients in that group. The labels display the average value alongside the percentage of the total.
CARDIO and INFECTIO show the largest average prior hospitalization counts, confirming that cardiac and
infectious disease patients carry a heavier history of repeated admissions. Prior hospitalization count
is one of the strongest predictors in the logistic regression model, so clinical groups with high values
here warrant dedicated readmission prevention protocols.

### Chart — Average Medications by Pathology (bar chart, bottom left)

This vertical bar chart plots the average number of medications prescribed per stay for each pathology.
COPD (BPCO), Ischemic stroke (AVC isc.), and Heart failure (Insuffis.) show the highest bars, indicating
that patients with these conditions receive more complex medication regimens. A higher medication count
is associated with increased polypharmacy risk, adherence difficulties post-discharge, and higher
readmission probability. This chart helps pharmacists and discharge coordinators focus their
reconciliation efforts on the most medication-intensive pathologies.

### Chart — Average Complexity Index by Service (treemap, bottom left)

The treemap divides the hospital into services — Neurologie, Geriatrie, Cardiologie, Diabetologie,
Reanimation, Pneumologie, Chirurgie ortho — and sizes each tile proportionally to its average
complexity index. Color coding adds a second layer of differentiation. Larger, more intensely colored
tiles represent services where admitted patients tend to have more complex stays. Reanimation and
Neurologie tiles are among the largest, confirming that intensive care and neurology departments handle
the most complex patient profiles. This directly informs staffing ratios and discharge planning resources.

### Chart — Total Stays by Discharge Mode (donut chart, bottom center)

The donut chart shows how patients leave the hospital, split into five discharge modes:
EHPAD (nursing home), Home (Domicile), SSR (rehabilitation center), Transfer, and Death (Deces).
Each slice is roughly equal in size at approximately 20% each, indicating that no single discharge
pathway dominates. The balance between home discharge and institutional discharge (EHPAD, SSR,
Transfer) is clinically significant: patients discharged to SSR or EHPAD have different readmission
risk profiles than those sent home, and the model captures this through the discharge mode feature.

### KPI Cards (right panel)

| KPI card | Value | Meaning |
|---|---|---|
| Critical Alert Patients | 96 | Patients whose risk score exceeds the critical alert threshold |
| Complex Discharge Rate | 80.0% | Proportion of stays with a non-home discharge mode |
| Moderate Risk Patients | 96 | Patients classified in the moderate risk tier |

The **80% complex discharge rate** is notably high and reflects the synthetic patient population's
design: older, multi-morbid patients with high prior hospitalization counts tend to require structured
post-acute care rather than direct home discharge.

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

**Stage 1 — Data Engineering (Bronze and Silver layers)**

Raw synthetic patient data is generated using the Python Faker library and ingested
into PostgreSQL. The Silver layer, implemented in dbt, applies data cleaning, type
casting, deduplication, and feature derivation. Three staging models are produced:
patient demographics, stay metrics, and diagnostic enrichment via a simplified
ICD-10 reference table.

**Stage 2 — Machine Learning and Statistical Analysis (Gold layer and Python notebooks)**

The Gold layer consolidates the Silver models into a feature store table consumed
by the prediction pipeline. Three Jupyter notebooks cover exploratory data analysis,
statistical validation of features (chi-square, Mann-Whitney, VIF), and a logistic
regression model with stratified cross-validation. The model outputs a continuous
risk score between 0 and 1 for each patient stay.

**Stage 3 — Business Intelligence (Power BI)**

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
  PostgreSQL — schema: bronze
  Table: bronze.patients_raw
  No transformation — raw ingestion with timestamp logging
  |
  v
Silver Layer
  dbt models — materialized as views
  stg_patients     : type casting, normalization, quality filters
  stg_sejours      : stay metrics, complexity index, risk flags
  stg_diagnostics  : ICD-10 enrichment, clinical group mapping
  |
  v
Gold Layer
  dbt models — materialized as tables
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
|-- project_medica_version.pbix     <- Power BI report (download above)
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
| age                             | Integer     | Patient age at admission (18–95)         |
| sexe                            | Categorical | M / F                                    |
| pathologie                      | Categorical | Heart failure, COPD, Diabetes, etc.      |
| diagnostic_principal            | String      | Simplified ICD-10 code                   |
| service                         | Categorical | Cardiology, Pulmonology, etc.            |
| hopital_region                  | Categorical | French administrative region             |
| duree_sejour                    | Integer     | Length of stay in days (1–45)            |
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
   class imbalance (approximately 25–30% positive class). Pipeline includes
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
The semantic model requires a DimDate table (2022–2027) and a DimUtilisateurs
mapping table for dynamic RLS.

---

## Power BI Semantic Model

The Power BI layer contains 56 DAX measures organized in 7 folders:

- Global KPIs: total stays, readmission rate, average length of stay, age, medications
- Risk Scores: risk score distribution, high/moderate/low risk patient counts
- Pathology Analysis: pathology-level rates, deviation from global average, ranking
- Time Analysis: YTD, rolling 30 days, year-over-year delta, trend label
- Demographics: senior (75+) rates, prior hospitalizations, male vs. female differential
- Alerts and Thresholds: alert color encoding, performance status, services above threshold
- Model Metrics: confusion matrix components, precision, recall, F1, KS statistic

Row-level security is implemented for four roles:

- Direction_Hopital (Hospital Management): unrestricted access to all data
- Responsable_Service (Department Manager): filtered to the user's service via USERPRINCIPALNAME()
- Analyste_Risques (Risk Analyst): filtered to patients with risk score >= 0.40
- Medecin_Traitant (Attending Physician): filtered to the user's service via USERPRINCIPALNAME()

---

## Glossary — French to English Reference

This section translates all French labels used in the Power BI dashboards and data pipeline.
Use this as a reference guide when reading charts, axis labels, and field names.

### Dashboard Labels

| French label | English translation | Context |
|---|---|---|
| Réhospitalisation | Readmission | A patient returning to hospital within 30 days of discharge |
| Séjour | Hospital stay | One recorded hospital admission episode |
| Pathologie | Pathology / Medical condition | The primary diagnosis driving the admission |
| Service | Hospital department / Ward | The clinical unit where the patient was treated |
| Date d'admission | Admission date | The date the patient entered the hospital |
| Mode de sortie | Discharge mode | How the patient left the hospital |
| Taux de réhospitalisation | Readmission rate | Percentage of stays followed by readmission within 30 days |
| Durée moyenne de séjour | Average length of stay | Mean number of days spent in hospital |
| Score de risque | Risk score | Predicted probability of readmission (0 to 1) |
| Indice de complexité | Complexity index | Composite score of clinical difficulty for a stay |
| Hospitalisations précédentes | Prior hospitalizations | Number of admissions in the 12 months before current stay |
| Médicaments | Medications | Number of drugs prescribed during the stay |
| Groupe clinique | Clinical group | Broader category grouping related pathologies |
| Catégorie de risque | Risk category | Tier classification: Low, Very Low, Moderate |
| Saison d'admission | Admission season | Season in which the stay was recorded |
| Patients alerte critique | Critical alert patients | Patients whose risk score exceeds the critical threshold |
| Taux sortie complexe | Complex discharge rate | Share of stays with a non-home discharge mode |
| Patients modérés | Moderate risk patients | Patients classified in the moderate risk tier |
| Âge moyen | Average age | Mean patient age across a group |

### Pathology Names

| French name | English name | Clinical description |
|---|---|---|
| Fracture col fémur | Hip fracture | Fracture of the femoral neck, common in elderly patients |
| Pneumonie | Pneumonia | Lung infection causing fluid accumulation in air sacs |
| AVC ischémique | Ischemic stroke | Brain blood supply blocked by a clot |
| Insuffisance cardiaque | Heart failure | Heart unable to pump sufficient blood to meet body demands |
| Diabète de type 2 | Type 2 diabetes | Chronic metabolic disorder with elevated blood glucose |
| BPCO | COPD (Chronic Obstructive Pulmonary Disease) | Progressive lung disease causing airflow obstruction |
| Sepsis | Sepsis | Life-threatening immune response to infection |
| IRC | CKD (Chronic Kidney Disease) | Progressive loss of kidney function over time |

### Clinical Groups (groupe_clinique)

| French code | English meaning |
|---|---|
| TRAUMA | Trauma (fractures, orthopedic injuries) |
| NEURO | Neurology (stroke, neurological conditions) |
| METABOL | Metabolic (diabetes, endocrine disorders) |
| PULMO | Pulmonology (respiratory diseases, COPD, pneumonia) |
| CARDIO | Cardiology (heart failure, cardiac conditions) |
| INFECTIO | Infectiology (sepsis, infectious diseases) |
| NEPHRO | Nephrology (chronic kidney disease, renal conditions) |

### Hospital Services (service)

| French name | English name |
|---|---|
| Cardiologie | Cardiology |
| Chirurgie ortho | Orthopedic surgery |
| Diabétologie | Diabetology / Endocrinology |
| Gériatrie | Geriatrics |
| Neurologie | Neurology |
| Pneumologie | Pulmonology |
| Réanimation | Intensive Care Unit (ICU) |

### Discharge Modes (mode_sortie)

| French name | English name | Description |
|---|---|---|
| Domicile | Home | Patient discharged directly to their home |
| EHPAD | Nursing home | Residential care facility for dependent elderly patients |
| SSR | Rehabilitation center | Specialized post-acute rehabilitation unit |
| Transfert | Transfer | Patient moved to another hospital or unit |
| Décès | Death | Patient died during the stay |

### Risk Category Labels (catégorie_risque)

| French label | English label | Meaning |
|---|---|---|
| Faible | Low risk | Predicted readmission probability below the low threshold |
| Très faible | Very low risk | Predicted readmission probability at the minimum range |
| Modéré | Moderate risk | Predicted readmission probability in the intermediate range |

### Seasons (saison_admission)

| French | English |
|---|---|
| Été | Summer |
| Printemps | Spring |
| Automne | Autumn |
| Hiver | Winter |

---

## License

MIT License. All data used in this project is entirely synthetic.
No real patient data was used at any stage of development.
