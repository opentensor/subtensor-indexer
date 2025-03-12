FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_validators /app/shovel_validators

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_validators/main.py"]
