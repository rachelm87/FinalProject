# AstraWatch: Monitoring Global Space & Space-Security Trends

## 📌 Project Overview

**AstraWatch** is a data analytics capstone project that monitors, structures, and analyzes recent global space-related developments, with a specific focus on **space security**.  
The project consolidates unstructured news from multiple sources and transforms it into a structured, event-level dataset to support **situational awareness**, trend analysis, and risk-contextualized insights.

This project was completed as the **final project for a Data Analytics bootcamp** and demonstrates an end-to-end analytics workflow using real-world, unstructured data.

---
## 🔗 Project Links

- 📊 **[Live Dashboard (Tableau Public)](https://public.tableau.com/app/profile/rachelm/viz/AstraWatch/Dashboard)**
- 💻 **[GitHub Repository](https://github.com/rachelm87/FinalProject)**
- 📹 **[Video Demonstration](https://www.loom.com/share/9ae62fcd999f4dfd825a8451ed65925e)**


## 🎯 Objectives

- Collect space- and space-security–related news from multiple sources  
- Clean, deduplicate, and structure unstructured text data  
- Categorize events (launches, satellites, infrastructure, policy, security incidents)  
- Tag events by **countries** and **entities**  
- Contextualize activity using country-level risk indicators  
- Generate KPIs and trends using SQL  
- Visualize insights through an interactive Tableau dashboard  

---

## 🌍 Scope of Analysis

The project focuses on:
- Space launches and satellite deployments  
- Ground and space infrastructure developments  
- Space policy and defense-related announcements  
- Security-relevant incidents (jamming, interference, counterspace activity)  
- Weekly and short-term changes in space-security attention  

---

## 🗂 Data Sources

- **Spaceflight News API** – primary source for structured space industry reporting  
- **GDELT DOC 2.0 API** – global news monitoring for space and security coverage  
- **Google News RSS (pygooglenews)** – supplemental recent reporting  
- **Vision of Humanity – Global Peace Index (GPI)** – country-level risk baseline  

---

## 🛠 Tech Stack

### Data Collection & Processing
- Python (requests, pandas, numpy, regex)
- API ingestion (JSON)
- Text cleaning and normalization
- Keyword-based filtering and categorization
- Deduplication and incremental updates

### Database & SQL
- PostgreSQL
- Event-level storage (`space_records`)
- SQL views for aggregation, trends, and co-occurrence analysis
- Joins, lateral unnesting, and aggregates

### Machine Learning (Exploratory)
- Google Colab
- scikit-learn
- Unsupervised clustering for weekly headline summarization

### Visualization
- Tableau
- Interactive dashboard with filters and cross-highlighting
- Maps, treemaps, bar charts, rankings, and trend comparisons

---

## 🗃 Repository Structure
FinalProject/
│
├── data/
│ ├── space_records.csv
│ ├── weekly_headlines.csv
│ ├── tableau_data.xlsx
│
├── notebooks/
│ └── ML_Weekly_Highlights.ipynb
│
├── sql/
│ ├── table.sql
│ └── views.sql
│
├── src/
│ ├── oss.py
│ └── connection.py
│
├── dashboard/
│ └── Visualizations.twb
│
├── requirements.txt
└── README.md

---

## 🧮 Database Views (Highlights)

- `v_baseline_filter` – security-related events baseline  
- `v_baseline_space_security` – strict space-security events  
- `v_security_events_by_country`  
- `v_security_events_by_entity`  
- `v_weekly_country_trends`  
- `v_weekly_entity_trends`  
- `v_country_cooccurrence`  
- `v_entity_cooccurrence`  
- `v_country_entity_shared`  
- `v_space_headlines_period` – ML-generated headline summaries  

These views support KPI creation and Tableau visualization.

---

## 📊 Tableau Dashboard

The interactive dashboard includes:
- Global map of space-security activity  
- Industry stakeholders most frequently mentioned  
- Event type breakdowns  
- Space security mentions vs country risk exposure  
- Week-over-week changes in security-related attention  

Filters allow users to select countries, entities, and time periods, dynamically updating all charts.

---

## 🔑 Key Insights (Examples)

- Space-security activity is concentrated among a small number of countries and entities  
- Security-related space developments cluster around higher-risk geopolitical regions  
- Commercial actors increasingly appear alongside government institutions  
- Week-over-week analysis highlights rapid shifts following launches or incidents  

These insights are analytical demonstrations rather than definitive intelligence assessments.
---

## ⚠️ Data Availability, Assumptions & Limitations
This project is designed as a **near-real-time monitoring and analytical tool**, rather than a comprehensive historical archive. The following limitations and assumptions apply:

- **Temporal scope**  
The dataset covers **recent months only**, reflecting the period in which the data collection pipeline is executed. The Tableau dashboard updates **only when the Python pipeline is manually run** and does not refresh automatically.

- **Source coverage and bias**  
Data is collected exclusively from **open-source, English-language media and APIs**. As a result, the dataset is biased toward **Western reporting sources** and may underrepresent developments reported in non-English or restricted media environments.

- **Event completeness**  
The project does not capture all global space or space-security developments. It reflects only those events that are publicly reported and accessible through the selected APIs and RSS feeds.

- **Geographic attribution**  
Countries and regions are inferred through **text-based mention detection** within articles. While effective for large-scale analysis, this approach may miss implicit geographic references or over-represent frequently cited countries.

- **Event classification**  
Event categorization is based on **analyst-defined, keyword-based rules**. This method prioritizes transparency and reproducibility but may:
- Miss relevant events that do not match predefined keywords  
- Include marginal or ambiguous cases that require human interpretation

- **Risk baseline**  
The **Global Peace Index (GPI)** is used as a **proxy indicator** for national risk exposure. It is not a space-specific threat index and does not directly measure counterspace capabilities, cyber operations, or space-security posture. A combined crime or terrorism-specific threat score could not be implemented within the project timeframe due to data availability constraints.

These limitations are consistent with open-source intelligence (OSINT)–driven analysis and are documented to ensure transparent interpretation of the results.

---

## 🔮 Future Improvements

If extended beyond the scope of this course, AstraWatch could be enhanced in several realistic and analytically valuable ways:

- **Automation**  
Implement scheduled API runs (e.g., daily or weekly) to enable **automatic dashboard updates** and reduce reliance on manual execution.

- **Advanced natural language processing**  
Replace or supplement keyword-based tagging with **Named Entity Recognition (NER)** or topic modeling to improve detection of countries, organizations, and event types.

- **Expanded risk indicators**  
Integrate **space-specific and security-relevant risk metrics**, such as counterspace doctrine, sanctions regimes, ASAT testing history, or cyber capability indicators, to complement or replace the Global Peace Index baseline.

- **Crime and terrorism cross-referencing**  
Incorporate structured datasets of **designated criminal, terrorist, or extremist organizations** to cross-reference entity mentions and identify potential overlaps with space-sector activity.

- **Network and financial risk analysis (exploratory)** 
Extend co-occurrence analysis to support **network-based exploration of indirect linkages**, including the potential tracing of illicit finance pathways into emerging space or dual-use technology companies via jurisdictions, partnerships, or intermediaries.

- **Analyst validation layer**  
Introduce manual review or flagging mechanisms for **high-risk or high-impac**
---

## ▶️ How to Run

### 1. Clone the repository  
```bash
git clone <https://github.com/rachelm87/FinalProject>
cd FinalProject 
```

### 2. Create and activate a virtual environment (recommended)
```bash
python -m venv venv
source venv/bin/activate   # macOS / Linux
```

### 3. Install dependencies
```bash 
pip install -r requirements.txt
```

### 4. Configure Postgres connection
- Create a PostgreSQL database named astrawatch
- Update credentials in:
```bash src/connection.py```
or via a .env file (if used)

### 5. Run the data pipeline
This will: 
- Collect data from APIs
- Clean and categorize records
- Insert new records into PostgreSQL
```bash python src/oss.py ```

### 6. Generate analysis views
Run the following SQL files in pgAdmin:
- sql/tableau.sql
- sql/views.sql
These create the analytical views used for visualization.

### 7. Tableau visualization
- Open Visualizations.twb in Tableau
- Or connect Tableau to data/tableau_data.xlsx
- Interact with filters and dashboard