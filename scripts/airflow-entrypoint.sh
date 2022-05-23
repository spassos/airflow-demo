#!/usr/bin/env bash
airflow db reset
airflow db init
airflow db upgrade
airflow users create -r Admin -u admin -e airflow@example.com -f admin -l admin -p admin
airflow scheduler &
airflow webserver