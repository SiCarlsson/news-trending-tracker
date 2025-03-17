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
- Create a **BigQuery dataset** named `scraper_data` with the following tables:

#### Websites Table
| Field Name   | Type   | Mode     | Description |
|--------------|--------|----------|-------------|
| website_id   | STRING | REQUIRED | Primary key. Unique identifier for each news website. 
| website_name | STRING | REQUIRED | The display name of the news website (e.g., 'SVT', 'Expressen', 'Aftonbladet'). 
| website_url  | STRING | REQUIRED | The base URL of the news website (e.g., 'https://www.svt.se'). 

#### Articles Table
| Field Name    | Type   | Mode     | Description |
|---------------|--------|----------|-------------|
| article_id    | STRING | REQUIRED | Unique identifier for each article. 
| website_id    | STRING | REQUIRED | Foreign key referencing websites.website_id. 
| article_title | STRING | REQUIRED | The title of the article. 
| article_url   | STRING | REQUIRED | The URL of the article. (e.g., '/nyheter/ekonomi/sparare-ratar-usa-fonder-valjer-svenskt')

#### Words Table
| Field Name  | Type   | Mode     | Description |
|-------------|--------|----------|-------------|
| word_id     | STRING | REQUIRED | Primary key. Unique identifier for each word.
| word_text   | STRING | REQUIRED | The actual word. 

#### Occurrences Table
| Field Name    | Type   | Mode     | Description |
|---------------|--------|----------|-------------|
| occurrence_id | STRING | REQUIRED | Primary key. Unique identifier for each word occurrence.
| word_id       | STRING | REQUIRED | Foreign key referencing words.word_id. Identifies which word appeared.
| website_id    | STRING | REQUIRED | Foreign key referencing websites.website_id. Identifies which website the word appeared on.
| article_id    | STRING | REQUIRED | Foreign key referencing articles.article_id. Links to the specific article where the word appeared. 
| timestamp     | STRING | REQUIRED | The exact date and time when the word was scraped/recorded. 