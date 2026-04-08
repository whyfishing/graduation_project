FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY streaming_rank_job.py .
CMD ["python", "streaming_rank_job.py"]
