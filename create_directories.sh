mkdir news-analysis
cd news-analysis

mkdir -p data-source data-ingestion stream-processing sentiment-analysis database-management frontend

touch data-source/Dockerfile data-source/data_source.py data-source/requirements.txt
touch data-ingestion/Dockerfile
touch stream-processing/Dockerfile
touch sentiment-analysis/Dockerfile
touch database-management/Dockerfile
touch frontend/index.html frontend/style.css frontend/script.js

touch docker-compose.yml

touch README.md

echo "Directory structure created successfully."
