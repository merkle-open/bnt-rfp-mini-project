#!/bin/bash -e


# Create a runtime Airflow user
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org \
    --password 123

# Launch our services (airflow, jupyter)
supervisord -n -c bnt_rfp_mini_project/config/supervisord.conf
