# NYC Real Estate Sales Dashboard

A complete data engineering project that ingests, cleans, models, and visualizes NYC property sales data to identify market trends.

**View the Live Interactive Dashboard Here**
[https://giant-swim.metabaseapp.com/public/dashboard/1311632e-f703-4193-8fb1-191104e922b0](https://giant-swim.metabaseapp.com)


<img width="1089" height="704" alt="Screenshot from 2025-10-18 20-57-54" src="https://github.com/user-attachments/assets/b741e374-0a32-49c2-9258-17698fb79ef8" />
<img width="1081" height="415" alt="Screenshot from 2025-10-18 21-37-04" src="https://github.com/user-attachments/assets/7c8630fc-c33f-4e05-815c-e81e80515d02" />


---

## 1. Project Objective

The goal of this project is to help realtors/agencies understand property price trends (average sale price, monthly volume) for better market insights across New York City.
The key business insight is to build standardized data views that reveal which neighborhoods are "hot" or potentially undervalued.

---

## 2. Tech Stack

This project was built as an end-to-end cloud data solution, migrating from a local environment to a fully deployed cloud application.

* 🐘 **Database:** **PostgreSQL** (Local and Hosted on **Render**)
* 🐍 🐚 **Data Ingestion and cleansing:** **Bash**, **Python**, **[NYC GeoSearch API]**
* ☁️ **Data Migration:** **Python** (Pandas, SQLAlchemy)
* 📈 **BI & Visualization:** **Metabase** (Hosted on **Metabase Cloud**)
* 💻 **Tools:** **DBeaver**, **VScode**, **Docker**

### Project Architecture

The architecture is as follows:

1.  **Local Environment:** Data was ingested from NYC.gov, cleaned, and modeled locally using Python, SQL, Bash(SHELL Scripting), and a local Postgres instance.
2.  **Cloud Database:** The finalized data model was migrated to a free-tier Postgres database hosted on **Render**.
3.  **Cloud Application:** A **Metabase Cloud** instance was deployed to connect to the **Render** database.

---

## 3. Data Model

The data is modeled as a **Star Schema**.

* `fact_property_sales`: A single table containing one row for every valid market sale, with keys to the dimensions and the `sale_price` measure.
* `dim_property`: A table holding descriptive details about each unique property (address, building type, borough, ZIP).
* `dim_date`: A date dimension table that breaks down the `sale_date` into useful attributes (month, year, quarter, day of week).


<img width="1019" height="540" alt="Screenshot from 2025-10-18 20-55-52" src="https://github.com/user-attachments/assets/e79f892d-3fe2-4005-9c39-bd83bcf5639d" />


---

## 4. Project Highlights

* **Advanced Data Cleansing:** Standardized inconsistent raw property addresses by using the **[NYC GeoSearch API]**.
* **End-to-End Deployment:** Successfully managed the full project lifecycle from local development to a live, public cloud dashboard.

---

## 5. Future Work

The next step for this project is to add automated orchestration.

* **Automated Ingestion:** Airflow DAG for yearly refresh of property sales.
* **Incremental Loads:** The script would fetch the yearly sales data, transform it, and append it to the `fact_property_sales` table, ensuring the dashboard is always up-to-date.

---

## 6. License

This project is licensed under the MIT License. See the `LICENSE` file for details.
