FROM python:3.9

COPY analyze_sentiment.py /
COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

CMD ["python", "/analyze_sentiment.py"]
