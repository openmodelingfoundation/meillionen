FROM python:3.9

COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY model.py /app/model.py

ENTRYPOINT ["./model.py"]
