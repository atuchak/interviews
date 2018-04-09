#!/bin/sh


/usr/src/app/wait-for-it.sh postgres:5432 -- echo "postgresql is up"
/usr/src/app/wait-for-it.sh rabbitmq:5672 -- echo "rabbitmq is up"

pip install -r requirements.txt
python manage.py migrate
python manage.py runserver 0.0.0.0:8080
