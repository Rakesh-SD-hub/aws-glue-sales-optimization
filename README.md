# AWS Data Lake Automation with AWS Glue for Sales Optimization

## Project Overview

This project demonstrates the development of a cloud-based data lake using Amazon S3 and AWS Glue to process Brazilian e-commerce data from the Olist dataset.

The goal is to organize raw data, clean and transform datasets, and prepare structured data for sales analysis and business reporting.

## Architecture

The project follows a layered data lake design:

* **Raw Layer:** Stores the original Olist CSV datasets in Amazon S3.
* **Silver Layer:** Cleans and transforms the raw datasets using AWS Glue ETL jobs.
* **Gold Layer:** Produces analytics-ready datasets for business reporting.

## AWS Services and Tools

* Amazon S3
* AWS Glue ETL
* AWS Glue Crawlers
* AWS Glue Data Catalog
* AWS IAM
* GitHub

Amazon Athena can be added to the workflow if query testing has been completed.

## Dataset

The project uses the public Olist Brazilian E-Commerce dataset, covering orders, customers, order items, payments, reviews, products, sellers, geolocation, and product category translations.

## Workflow

1. Upload the source CSV datasets to Amazon S3.
2. Configure IAM permissions for AWS Glue.
3. Catalog the raw datasets using AWS Glue Crawlers.
4. Build Silver-layer ETL jobs to clean and transform the datasets.
5. Catalog the Silver-layer outputs.
6. Build Gold-layer ETL jobs for business-focused analysis.
7. Validate the processed datasets and prepare them for reporting.

## Project Documentation

* [Read the project documentation (PDF)](docs/AWS_Glue_Sales_Optimization_Project_Documentation%281%29.pdf)
* [Read the project documentation (Word)](docs/AWS_Glue_Sales_Optimization_Project_Documentation.docx)

## Business Objectives

* Organize e-commerce data into structured data lake layers.
* Prepare reliable datasets for sales analysis.
* Support analysis of orders, payments, products, and customer activity.
* Demonstrate practical cloud data engineering skills.

## Project Status

Raw and Silver layer processing has been implemented. Gold-layer processing and final validation are in progress.

## Author

Data Engineering Portfolio Project
