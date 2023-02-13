# bnt-rfp-mini-project

Python framework for operating mini ETL platform which handles data storage, data optimization,
data cleansing, and transformation.

It is build around Data Lake design pattern and it implements storage as separated layers. For 
more details, see [Storage Layers](../data/README.md).

For process orchestration there is Airflow as part of Docker container. All ETL processes are
defined as Airflow DAGs.

## Configurable Project

Basic project configuration can be found inside `config` folder. Inside this folder
there needs to be defined `credentials.ini` file connection details to S3 bucket with source data.

`conventions.ini` file contains basic static conventions like layer names which are then reused 
across ETL framework. More information inside [config/README.md](config/README.md) or inside [`execute_etl_pipeline.ipynb`](../notebooks/execute_etl_pipeline.ipynb).

There is also Airflow instance configuration, which can be altered if needed.

## Package Structure

```
├── DAG
├── README.md
├── __init__.py
├── config
├── conventions.py
├── ingestion
├── schemas
└── transformation
```

### `DAG`

Airflow DAGs which can be orchestrated via Airflow. Folder contains complete ETL pipeline including
ingestion and all transformations.

### `config`

Contains framework configuration files as well as Airflow configuration file.

### `conventions.py`

Python file loading conventions from config and making them easily accessible for other parts of Python 
framework.

### `schemas`

Module where individual schema files are being defined. [Pandera](https://pandera.readthedocs.io/en/stable/index.html) data quality checks and validation
is supported and implemented.

This framework is designed to provide easy schema validation of pandas DataFrames. It is highly
customizable and it allows having hard/soft checks in place.

Hard checks are the ones which stops transformation process immediately, because data is unrepairable.
Soft checks on the other hand just raise a warning together with schema validation report, but 
transformation process can proceed with cleansing.

### `ingestion`

Module having AWS S3 connector which is used during ingestion process. All ingestion processes are 
defined inside this folder. It outputs all files into `raw` storage layer.

### `transformation`

Module containing framework for writing layer-to-layer transformations. All individual data 
manipulations between layers are stored here as Python scripts.

```
transformation/
├── cleansed2processed
│   └── ...
├── etl_pipeline.py
├── processed2output
│   └── ...
├── raw2cleansed
│   └── ...
└── utils
    ├── __init__.py
    ├── cleansed2processed.py
    ├── create_logger.py
    ├── layer2layer.py
    ├── processed2output.py
    ├── raw2cleansed.py
    └── raw2rejected.py
```

#### `utils`

Module contains Python framework for quick build of layer-to-layer transformations. All 
transformations are based on parent class `Layer2Layer` in `layer2layer.py` file and they implement
their own custom layer logic.

When new transformation is being written, take appropriate layer transformation, inherit the class
and build custom layer transformation around it.


#### `raw2cleansed`

Individual transformations between `raw` and `cleansed` layer. Schema validation and data quality 
checks takes place here.

#### `cleansed2processed`

Individual transformations between `cleansed` and `processed` layer. Custom transformations like 
joins can happen here.

#### `processed2output`

Individual transformations between `processed` and `output` layer. Data in output layer are ready 
to be consumed by any target system.

#### `etl_pipeline.py`

Python file contains complete ETL pipeline as an alternative for Airflow DAG.

## More Details

* [Storage Layers documentation](../data/README.md)
* [Analytics notebooks](../notebooks/README.md)
* [ETL framework](../bnt_rfp_mini_project/README.md)

