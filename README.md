# News Trending Tracker 📊

**⚠️ Under Development ⚠️**

A real-time data pipeline that scrapes news websites, extracts trending words, and visualizes trends over time.

## 🚀 Features

- Scrapes news headlines from multiple sources
- Data pipeline includes **Kafka** and **Apache Spark**
- Stores data in **Google BigQuery**
- **dbt (Data Build Tool)** for business logic and transformations
- Provides a REST API using Java + Spring Boot
- Visualizes trends with **custom UI**

## 🛠️ Tech Stack

- **Scraping:** Python (Scrapy)
- **Data Pipeline:** Kafka & Apache Spark (Structured Streaming)
- **Data Transformation:** dbt
- **Database:** Google BigQuery
- **Infrastructure:** Terraform
- **Backend API:** Java + Spring Boot
- **Frontend:** React, Typescript, TailwindCSS

## 🔧 Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/SiCarlsson/news-trending-tracker.git
   cd news-trending-tracker

   ```

2. **Set Up Google Cloud**

- Create a **Google Cloud** project and enable **BigQuery API**.
- Create a **Service Account** and generate a **Service Account Key** with **BigQuery Data Editor** and **BigQuery Job User** roles for the backend services.
- Download the keys file and place it in the project directory.
- Update the `Terraform variables` by duplicating `./terraform/terraform.tfvars.example` and change the varaibales to suit your project (if wanting to override variables).
- BigQuery Dataset and Tables are automatically created when deploying the program.

## 📊 Data Model

The system uses BigQuery with three datasets:

- **raw** - Raw data from scrapers (websites, articles, words, occurrences)
- **staging** - Temporary staging tables for deduplication
- **aggregation** - Time-series analytics (10-minute word trends)

Detailed schema definitions with field descriptions are available in `terraform/main.tf`.
