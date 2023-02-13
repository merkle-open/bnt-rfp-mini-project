# bnt-rfp-miniproject

Mini-project assignment for the RFP tender.

## Description

Example repository of a Data Lake design pattern for advanced scalable analytics.

For demonstration purposes the data engineering best practices have been implemented to perform a simple ETL task. Best practices allows:
1. Extensibility - enabled by a custom ETL framework developed in Python and Airflow orchestration.
2. Governance - Basic data validation enabled by the [Pandera](https://pandera.readthedocs.io/en/stable/index.html) framework.
2. Monitoring - Enabled by Airflow and custom logging framework

This repository demonstrates all these principles with analysis of public Covid-19 datasets
which contain information about Covid-19 variants evolution and country indicators.

Analysis outputs are:
1. ___Early hit countries___
      * For each continent, which are the five countries that are statistically hit earliest by new variants?
2. ___Predictor countries___
      * For these countries which are the five countries on and the five off the respective continent, that serves as predictors for incoming variants?

Outputs of the analysis are presented in the Jupyter notebook.

## Storage Layer

Folder `data` serves as a storage layer of our ETL system. It helps organizing original data as well as processed and output data.

1) `raw`

   * Source datasets ingested into storage in its raw form.
   * No schema required for storing data, can store any type of data.
   * Serves as a landing layer for all ingestion processes.

2) `cleansed`
   
   * Cleansing layer for any data which needs to be cleansed before processing,
    as well as storing data in big-data ready structure and formats.
   * Typical process is detecting and correcting corrupt or invalid records.
   * Applies defined data quality rules on selected datasets to ensure
    its correctness and if repair is not possible, expose such data in the `rejected` layer.

3) `rejected`
   
   * Any data which cannot be processed properly by cleansing transformation.
   * Corrupted data will land in this storage layer together with data quality report.
   * Notification system can be set up to track any datasets which appears in 
    this layer and proactively trigger source systems.

4) `processed`

   * Output of transformation process from cleansed or raw layer.
   * Data here are ready to be analyzed, joined together, or used in any other way by analysts.
   * Data stored as native big-data structure and file formats.

5) `output`

   * Output layer contains output of analytics and data science tasks which are 
   ready to be consumed by a presentation layer or downstream systems.
   * Analytics reports can be produced here as well.
   * Data can be stored in any format requested by consuming system.

## Presentation Layer

For presentation purposes there is JupyterLab and interactive notebooks. 
These notebooks access data stored in `output` or `processed` layer and they
visualize them for clearer understandability.

### More Details

Individual subpages dedicated to specific topics.

* [Storage Layers documentation](data/README.md)
* [Analytics notebooks](notebooks/README.md)
* [ETL framework](bnt_rfp_mini_project/README.md)


## Getting Started

### Dependencies
* Docker, version 23.0.0
* internet access
* AWS CLI credentials for access to underlying dataset Covid-19 Variants Evolution and Country Indicators.
  * Needs to be filled in `bnt_rfp_mini_project/config/credentials.ini`

#### Docker Image

The Docker image integrates all the requirements needed to run the data processing pipeline, namely:
- JupyterLab with notebooks
- Apache Airflow for ETL pipelines
- supervisor in order to launch both services above

Both services are exposed on the following TCP ports:
- 8888 (Jupyter notebooks)
- 8080 (Apache Airflow)

## Installation

There are two installation options. First, just download and run the image from Docker Hub. Second 
option is to clone repository, and build the image locally.

### 1. Build and Run Docker Image
#### Option A - From Docker Hub
Link to Docker Hub image: [`petrsvec/bnt-rfp-mini-project`](https://hub.docker.com/r/petrsvec/bnt-rfp-mini-project)

Pull published image:
```bash
$ docker pull petrsvec/bnt-rfp-mini-project
```

Create and run container from downloaded image:
```bash
$ docker run -d --name mini-lake -p 8080:8080/tcp -p 8888:8888/tcp petrsvec/bnt-rfp-mini-project:latest
```

#### Option B - From Cloned Repository

First step is to clone this repository.

Then, the easiest way to build and run Docker image is with `docker compose`.
You can build and launch the following image by typing:

```bash
$ docker compose up --build
```

The command will hang during the time that the container is running. You will see the logs related to it

### 2. Start JupyterLab and Airflow

There are two exposed ports allowing access to this running container. You can thus reach the services at the following URLs:

| **Service** | **Address**           | **Credentials**                 |
| ----------- | --------------------- | ------------------------------- |
| Jupyter     | http://localhost:8888 | None                            |
| Airflow     | http://localhost:8080 | Login: `admin`, Password: `123` |

As a demonstration package, Airflow's credentials are hardcoded.

### 3. Set Up `credentials.ini` File

In order to download and visualize datasets, you have to first set up `credentials.ini` configuration file.

1. Create `credentials.ini` file inside [bnt_rfp_mini_project/config](../bnt_rfp_mini_project/config) 
   using [`../bnt_rfp_mini_project/config/credentials.ini.template`](../bnt_rfp_mini_project/config/credentials.ini.template) as a template

More details can be found in [notebooks/README.md](notebooks/README.md).

### Docker image specifics

You may have noted that this image runs many services. We chose this exceptional approach as it enables us to bundle the whole dependencies into a single image.
On a production setup, these services would be separated into several images, as that would bring several benefits (easier deployment, scalability, logging, …).

## Authors
Merkle DACH

## Version History

## License
MIT
