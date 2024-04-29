FROM jupyter/pyspark-notebook

# Copy the notebook and data file into the container
COPY FinancialNewsSentiment.ipynb /home/jovyan/work/
COPY FinancialNewsSentiment.csv /home/jovyan/work/

# Set the working directory
WORKDIR /home/jovyan/work/
