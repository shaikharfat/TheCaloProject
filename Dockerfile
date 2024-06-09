FROM python:3.8

COPY raw_data.zip .

ADD main.py .

RUN pip install pandas

COPY . .

CMD ["python", "./main.py"]