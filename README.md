**Real-time Financial News Sentiment Analysis Dashboard**

---
### You can locate all the Databricks files featured in the presentation within the final-project directory in HTML format or final-project-ipynb in notebook format. Simply download those files containing data and then upload them onto Databricks to run those.



### Project Overview

This project aims to create a real-time financial news sentiment analysis dashboard. The dashboard will leverage various technologies such as Kafka for streaming data, Spark Streaming for processing, machine learning models for sentiment analysis, PostgreSQL for data storage, and a frontend dashboard for visualization.

### Project Structure

The project is structured into several components:

1. **Data Ingestion**: The financial news data will be fetched from a news API (e.g., NewsAPI) using Kafka as a messaging system for real-time data streaming.

2. **Data Processing**: Spark Streaming will be employed to process the incoming data streams, performing necessary transformations and preparing the data for sentiment analysis.

3. **Sentiment Analysis**: Machine learning models will be utilized for sentiment analysis of the financial news articles. This step will determine the sentiment (positive, negative, or neutral) of each article.

4. **Data Storage**: The results of sentiment analysis along with relevant metadata will be stored in a PostgreSQL database for further analysis and retrieval.

5. **Frontend Dashboard**: A frontend dashboard will be developed using suitable technologies (e.g., React.js, Flask) to visualize the sentiment analysis results in real-time.

### Setup Instructions

To set up and run the project, follow these steps:

1. **Clone the Repository**: Clone this repository to your local machine.

   ```bash
   git clone https://github.com/your-username/financial-news-sentiment-analysis.git
   ```

2. **Install Dependencies**: Navigate to the project directory and install the required dependencies.

   ```bash
   cd financial-news-sentiment-analysis
   pip install -r requirements.txt
   ```

3. **Configure Kafka and PostgreSQL**: Set up Kafka and PostgreSQL instances and configure the necessary connection details in the project.

4. **Run Data Streaming**: Start the Kafka producer to stream data from the news API.

   ```bash
   python kafka_producer.py
   ```

5. **Run Spark Streaming Job**: Execute the Spark Streaming job to process the incoming data streams.

   ```bash
   spark-submit spark_streaming_job.py
   ```

6. **Perform Sentiment Analysis**: Train or load pre-trained machine learning models for sentiment analysis and run the sentiment analysis process.

   ```bash
   python sentiment_analysis.py
   ```

7. **Store Results in PostgreSQL**: Configure PostgreSQL connection details and store the sentiment analysis results in the database.

8. **Launch Frontend Dashboard**: Set up the frontend dashboard using suitable technologies and launch it to visualize the sentiment analysis results in real-time.

### Contributors

- Yesha Desai
- Dinh Tho Tran
- Tejas Sathe
