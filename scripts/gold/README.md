# Gold Layer ETL Scripts

This folder contains AWS Glue ETL scripts designed to create analytics-ready datasets for sales optimization.

## Business Analytics

The Gold layer is intended to support analysis such as:

* Sales and revenue trends
* Monthly sales performance
* Product performance
* Order and payment analysis
* Customer purchasing patterns

## Processing

The Gold ETL jobs read cleaned datasets from the Silver layer, apply business transformations and aggregations, and write results to the Gold layer in Amazon S3.

## Validation

Gold outputs should be checked for correct schemas, expected records, and reliable business metrics before being used for reporting.

## Note

The actual Gold ETL scripts will be added after the AWS Glue jobs have been validated successfully.
