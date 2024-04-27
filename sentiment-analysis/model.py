from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
from tqdm import tqdm

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Sentiment Analysis") \
    .getOrCreate()

# Define schema for input dataset
schema = StructType([
    StructField("sentiment", StringType(), True),
    StructField("text", StringType(), True)
])

# Load input dataset
filename = "file:///path/to/all-data.csv"
df = spark.read.csv(filename, schema=schema, header=False)

# Define function for generating prompts
def generate_prompt(text):
    return f"""generate_prompt
            Analyze the sentiment of the news headline enclosed in square brackets, 
            determine if it is positive, neutral, or negative, and return the answer as 
            the corresponding sentiment label "positive" or "neutral" or "negative"

            [{text}] = 
            """.strip()

generate_prompt_udf = udf(generate_prompt, StringType())

# Apply generate_prompt function to DataFrame
df = df.withColumn("prompt", generate_prompt_udf(df["text"]))

# Load pre-trained model and tokenizer
model = AutoModelForCausalLM.from_pretrained("/kaggle/input/gemma/transformers/7b-it/1")
tokenizer = AutoTokenizer.from_pretrained("/kaggle/input/gemma/transformers/7b-it/1")

# Define function for predicting sentiment
def predict_sentiment(text):
    input_ids = tokenizer(text, return_tensors="pt").to("cuda")
    outputs = model.generate(**input_ids, max_new_tokens=1, temperature=0.0)
    result = tokenizer.decode(outputs[0])
    answer = result.split("=")[-1].strip().lower()
    if "positive" in answer:
        return "positive"
    elif "negative" in answer:
        return "negative"
    elif "neutral" in answer:
        return "neutral"
    else:
        return "none"

predict_sentiment_udf = udf(predict_sentiment, StringType())

# Apply predict_sentiment function to DataFrame
df = df.withColumn("predicted_sentiment", predict_sentiment_udf(df["prompt"]))

# Define function for evaluating accuracy
def evaluate_accuracy(y_true, y_pred):
    total_count = y_true.count()
    correct_count = y_true.filter(y_true == y_pred).count()
    accuracy = correct_count / total_count
    print("Accuracy:", accuracy)

# Evaluate accuracy
evaluate_accuracy(df.select("sentiment"), df.select("predicted_sentiment"))

# Save the DataFrame with predictions
df.write.mode("overwrite").parquet("file:///path/to/predictions.parquet")

# Stop SparkSession
spark.stop()
