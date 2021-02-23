# sample_data_pipeline_beam

## Overview

- Create Beam app locally using DirectRunner
- Run on FlinkRunner via Docker
- Deploy job to EMR via Scripts
- (Not implemented) Detach from local machine by utilising EMR and Lambda to run job on a schedule
- (Not implemented) Tests to validate that the inputs and outputs align
- (Not implemented) Cloudwatch monitoring system

## Running the Flink container

The pipeline can be tested locally on Flink docker. See the aws_emr scripts for local and cloud based execution.

## Source Data

http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv

## Field Data Types

| Field Name                        | Field Type |
|-----------------------------------|------------|
| Transaction unique identifier     | text       |
| Price                             | numeric    |
| Date of Transfer                  | date       |
| Postcode                          | text       |
| Property Type                     | text       |
| Old/New                           | text       |
| Duration                          | text       |
| PAON                              | text       |
| SAON                              | text       |
| Street                            | text       |
| Locality                          | text       |
| Town/City                         | text       |
| District                          | text       |
| County                            | text       |
| PPDCategory Type                  | text       |
| Record Status - monthly file only | text       |

## Alternative ideas

Just run it on GCP as they have a managed BEAM runner. That will handle all this for us.

Wanted to try AWS though...