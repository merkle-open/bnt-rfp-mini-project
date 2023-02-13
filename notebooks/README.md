# Analytics
For analytic use cases we implement Jupyter notebooks, which all are being stored in this folder.

For any new use case, which needs some type of development, this is the place to start developing it.

## Use Cases

Currently implemented use case is analysis of Covid-19 variants across the all countries.

### Covid-19 Variants

1. ___Early hit countries___
      * For each continent, which are the five countries that are statistically hit earliest by new variants?
2. ___Predictor countries___
      * For these countries which are the five countries on and the five off the respective continent, that serves as predictors for incoming variants?

**For output visualization, open and run `presentation.ipynb`.**

#### **Prerequisites**

For proper analysis of Covid-19 data, the dataset has to be downloaded from source AWS S3 bucket and processed by our ETL.

1. Create `credentials.ini` file inside [bnt_rfp_mini_project/config](../bnt_rfp_mini_project/config) 
   using [`../bnt_rfp_mini_project/config/credentials.ini.template`](../bnt_rfp_mini_project/config/credentials.ini.template) as a template
   
   You will need:
   * S3 bucket name
     * `my-bucket-with-data`
   * AWS access key 
     * `AAAAAAAAAAAAAAAAAAAAAAAA`
   * AWS access secret
     * `secretsecretsecretsecretsecretsecretsecretsecretsecret`
   * Hostname to S3 service in region where target S3 bucket is located
     * `s3.eu-central-1.amazonaws.com`

When all prerequisites are set up, start all cells in this notebook.

Once done, start the pipeline. When data is stored inside storage layers, open `presentation.ipynb` 
and visualize the results.

## More Details

* [Storage Layers documentation](../data/README.md)
* [Analytics notebooks](../notebooks/README.md)
* [ETL framework](../bnt_rfp_mini_project/README.md)

