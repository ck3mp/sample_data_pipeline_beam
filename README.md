# sample_data_pipeline_beam

## Overview

- Create Beam app locally using DirectRunner
- Package up using Maven so it can be ran on AWS ECS under a Hadoop cluster

## Running the Maven container

For ease of development I chose to build my Maven app inside Docker

	docker run -it \                                            
	    -v /Users/chris/Development/sample_data_pipeline_beam/app:/app \
	    -w /app \
	    maven:3.6.0-jdk-8 /bin/bash

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