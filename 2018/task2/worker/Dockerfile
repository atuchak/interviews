FROM python:3.6-alpine

WORKDIR /usr/src/app

RUN apk update \
  && apk add --virtual build-deps gcc python3-dev musl-dev \
  && apk add  bash \
  && apk add postgresql-dev \
  && pip install psycopg2 \
  && apk del build-deps

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]