## Silver-to-Gold Reconciliation

The Gold sales table was reconciled against the Silver order-item table using Amazon Athena.

| Validation                         |        Result |
| ---------------------------------- | ------------: |
| Silver order-item rows             |       112,650 |
| Gold sales rows                    |       112,650 |
| Distinct orders in Silver          |        98,666 |
| Distinct orders in Gold            |        98,666 |
| Silver freight total               |  2,251,909.54 |
| Gold freight total                 |  2,251,909.54 |
| Gold item sales total              | 15,843,553.24 |
| Recalculated item sales total      | 15,843,553.24 |
| Row-level sales formula mismatches |             0 |

The reconciliation confirmed that `item_sales_value` equals `price + freight_value` within a tolerance of 0.01 for every Gold row. Silver and Gold row counts, distinct-order counts, freight totals, and item sales totals also matched in the tested queries.

These results validate the tested metrics. Additional checks may be needed for business rules, join completeness, and other columns.

## Project Documentation

Detailed documentation covering the project setup, AWS Glue ETL pipeline, data lake layers, and validation results is available below.

* [Download the Project Documentation (Word)](docs/AWS_Glue_Sales_Optimization_Project_Documentation.docx)
* [View the Project Documentation (PDF)](docs/AWS_Glue_Sales_Optimization_Project_Documentation%281%29.pdf)
