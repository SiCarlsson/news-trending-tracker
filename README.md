# News Trending Tracker 📊  
**⚠️ Under Development**  

A real-time data pipeline that scrapes news websites, extracts trending words, and visualizes trends over time.  

## 🚀 Features  
- Scrapes news headlines from multiple sources  
- Extracts and ranks trending words  
- Stores data in **Google BigQuery**  
- Provides a REST API using Java + Spring Boot  
- Visualizes trends with **Looker**  

## 🛠️ Tech Stack  
- **Scraping:** Python (Scrapy, BeautifulSoup)  
- **Data Pipeline:** Kafka, Apache Spark  
- **Storage:** Google BigQuery  
- **Backend API:** Java + Spring Boot  
- **Visualization:** Looker  

## 🔧 Setup  
1. **Clone the repository**  
   ```bash
   git clone https://github.com/yourusername/news-trending-tracker.git  
   cd news-trending-tracker

2. **Set Up Google Cloud**
- Create a **Google Cloud** project and enable **BigQuery API**.  
- Generate a **Service Account Key** with **BigQuery Data Editor** and **BigQuery Job User** roles.  
- Download the key file and place it in the project directory.  
- Update the `__init__` method in the `BigQueryPipeline` class within the `Pipelines.py` file to reference the key.