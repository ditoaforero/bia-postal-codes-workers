FROM ditoaforero/base-bia:latest 

ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_ACCESS_KEY_ID=1111111
ENV AWS_SECRET_ACCESS_KEY=secretito

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt
