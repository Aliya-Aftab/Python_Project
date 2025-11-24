# 🚨 Smart Traffic Violation Analysis Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=for-the-badge&logo=apachespark)
![Streamlit](https://img.shields.io/badge/Streamlit-1.30-red?style=for-the-badge&logo=streamlit)
![Pandas](https://img.shields.io/badge/Pandas-2.0-purple?style=for-the-badge&logo=pandas)

### [View the Live Interactive Dashboard Here](https://pythonproject-nwabg4qijypjvz6vzglwkj.streamlit.app/)

This repository contains the full end-to-end data engineering pipeline built for the Infosys Springboard internship. This project solves a core business problem: **How can traffic authorities allocate limited resources effectively?**

By ingesting, cleaning, and analyzing simulated traffic data, this pipeline transforms raw, messy data into an interactive dashboard that provides actionable insights to help identify *when* and *where* violation hotspots occur.

---

## 📸 Dashboard Preview
<img width="1889" height="838" alt="Screenshot 2025-11-14 234656" src="https://github.com/user-attachments/assets/7d7207bf-a393-45e7-9863-a76900c85b4c" />

---

## 🏛️ Project Architecture: A Decoupled Two-Tier System

This project is built using a professional "heavy-to-light" or "decoupled" architecture. This design ensures that the user-facing dashboard is fast and responsive, while the heavy data processing is handled separately.

### Tier 1: The "Heavy" ETL Backend (PySpark)
* **Purpose:** To process data at a scale that would crash normal tools. This backend is engineered to handle billions of rows, not just the 500 in the simulation.
* **Process:** A **PySpark** script (`clean_data.py`) ingests the raw, inconsistent CSV files. It performs all heavy transformations:
    * Handles `NULL` and `N/A` values.
    * Standardizes inconsistent text (`car` vs. `Car`).
    * Parses and validates multiple, complex timestamp formats.
* **Output:** A single, clean, optimized **Parquet file** that serves as our "data warehouse."

### Tier 2: The "Light" Analysis Frontend (Streamlit + Pandas)
* **Purpose:** To provide a fast, interactive, and user-friendly analysis tool.
* **Process:** The **Streamlit** dashboard loads the *single, clean Parquet file* from Tier 1 into **Pandas** at startup.
* **Benefit:** All filtering, grouping, and chart generation happen "on-the-fly" in memory. This architecture is far more efficient than running a new Spark job for every user click, making the dashboard feel instant.

---

## 💡 Key Features & Technical Highlights

* **End-to-End ETL Pipeline:** The project includes the full data lifecycle: from raw data simulation (`generate_data.py`) to a clean, queryable data warehouse (`cleaned_traffic_data.parquet`).
* **Dynamic "On-the-Fly" Analysis:** This is not a static report. All charts and KPIs are re-calculated in real-time based on user filters. The app performs its own `groupBy`, `value_counts`, and `crosstab` (pivot table) operations live.
* **Advanced "Hotspot" Visualization:** Includes a **Plotly Heatmap** (Violation Type vs. Hour) and an interactive **Donut Chart** to help users instantly identify complex patterns.
* **Robust & User-Friendly:** The app gracefully handles empty filter states, preventing crashes and displaying helpful "No data" messages to the user.
* **Data Export:** A "Download as CSV" feature allows non-technical users to export the filtered data they are currently viewing for their own reports.

---

## 🛠️ Tech Stack & Libraries

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Processing (ETL)** | **PySpark** | Scalable, large-scale data cleaning and transformation. |
| **Data Analysis (App)** | **Pandas** | In-memory data manipulation and filtering. |
| | **NumPy** | Core numerical operations for analysis. |
| **Web Dashboard** | **Streamlit** | The core framework for building the interactive web app. |
| **Data Visualization** | **Plotly Express** | Creating interactive bar charts, pie/donut charts, and heatmaps. |
| **Core Language** | **Python** | The language for all scripts. |
| **Local Environment** | **Hadoop Winutils** | Required Windows binaries to allow PySpark to write files locally. |

---

```bash
git clone https://github.com/Aliya-Aftab/Python_Project.git
cd Python_Project
