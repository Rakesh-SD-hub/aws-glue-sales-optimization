## Gold Layer Validation

The Gold sales ETL job completed successfully using AWS Glue. The output is stored as Parquet data in Amazon S3 and cataloged as `sales_optimization_db.sales`.

### Validation Results

* **Total sales item rows:** 112,650
* **Distinct orders:** 98,666
* **Possible duplicate order-item keys:** 0 detected
* **Rows with negative item sales values:** 0 detected
* **Rows missing price or freight:** 0 detected
* **Athena query validation:** Successful

The Gold table was queried using Amazon Athena. Aggregation checks also returned total item sales of approximately 15.84 million, total freight of approximately 2.25 million, and an average review score of approximately 4.03.

These checks provide an initial validation of the Gold dataset. Further reconciliation against the Silver layer is recommended to verify business calculations and completeness.
