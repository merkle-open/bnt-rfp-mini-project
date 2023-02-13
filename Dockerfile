FROM python:3.9

WORKDIR /work
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt



# Copy Airflow's configuration file
COPY bnt_rfp_mini_project/config/airflow.cfg /root/airflow/airflow.cfg
# Init Apache Airflow
RUN airflow db init


# Copy business data
COPY bnt_rfp_mini_project bnt_rfp_mini_project
COPY README.MD README.MD
COPY data data
COPY notebooks notebooks


EXPOSE 8888/tcp
EXPOSE 8080/tcp

COPY entrypoint.sh entrypoint.sh
ENTRYPOINT ["/work/entrypoint.sh"]
