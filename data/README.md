# Storage Layer
Folder `data` serves as a storage layer of our ETL system. It helps organizing original data as well as processed and output data.

Layer implements design pattern for a Data Lake. This brings several benefits, some of which are:

  1. scalability
  2. storing structured, semi-structured, and unstructured data in same system
  3. advanced analytics including machine learning

## Layer Structure

1) `raw`

   * Original datasets ingested into our process in its original form
   * No schema required for storing data, can store any type of data
   * Serves as a landing port for all ingestion processes

2) `cleansed`
   
   * Cleansing layer for any data which needs to be cleansed before processing,
    as well as storing data in big-data ready structure and formats
   * Typical is process of detecting and correcting corrupt or invalid records
   * Applies defined data quality measurements on selected dataset to ensure
    its correctness and if repair is not possible, store data in `rejected` layer

3) `rejected`
   
   * Any data which cannot be processed properly by cleansing transformation
   * Corrupted data will land in this storage layer together with data quality report
   * Notification system can be set up to track any datasets which appears in 
    this layer and proactively trigger source systems.

4) `processed`

   * Output of transformation process from cleansed or raw layer
   * Data here are ready to be analyzed, joined together, or used in any other way by analysts
   * Data stored as native big-data structure and file formats

5) `output`

   * Data in this layer is output of analytics and data science tasks which are 
   ready to be consumed by other connected systems
   * Analytics reports can be produced here as well
   * Data can be stored in any format requested by consuming system

## More Details

* [Storage Layers documentation](../data/README.md)
* [Analytics notebooks](../notebooks/README.md)
* [ETL framework](../bnt_rfp_mini_project/README.md)

