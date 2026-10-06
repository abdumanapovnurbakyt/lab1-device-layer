FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY device_sim.py .

CMD ["python", "-u", "device_sim.py", "--live", "mosquitto", "--fmt", "json", "--interval", "1"]