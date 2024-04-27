from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamProcessing") \
    .getOrCreate()

def save_to_postgresql(iter):
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        database="your_database",
        user="your_username",
        password="your_password"
    )

    cur = conn.cursor()

    for record in iter:
        processed_text = record[0]
        cur.execute("INSERT INTO your_table (processed_text) VALUES (%s)", (processed_text,))

    conn.commit()
    cur.close()
    conn.close()

ssc = StreamingContext(spark.sparkContext, 5)

kafka_params = {
    "bootstrap.servers": "kafka:9092",
    "auto.offset.reset": "largest",
    "group.id": "spark-streaming-consumer-group"
}
kafka_topics = ["raw_news"]
kafka_stream = KafkaUtils.createDirectStream(ssc, kafka_topics, kafka_params)
articles = kafka_stream.map(lambda x: json.loads(x[1])['title'])

# Add your preprocessing logic here

articles.foreachRDD(lambda rdd: rdd.foreachPartition(save_to_postgresql))

ssc.start()
ssc.awaitTermination()
