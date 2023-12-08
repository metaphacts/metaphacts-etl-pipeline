# metaphacts ETL pipeline

The Extract-Transform-Load (ETL) pipeline provides a means to convert structured data to RDF, perform post-processing steps, and ingest it into a graph database.

The pipeline follows the principles described in the [Blueprint](docs/Blueprint.md) and is based on an opinionated selection of components and tools:

* [Amazon Web Services (AWS)](https://aws.amazon.com/) as cloud environment
* a selection of AWS services such as S3, CloudFormation, StepFunctions, Lambda, EC2, etc. for various parts
* [RDF Mapping Language (RML)](https://rml.io/specs/rml/) as declarative mapping language with [Carml](https://github.com/carml/carml) as mapping engine
* [Ontotext GraphDB](https://graphdb.ontotext.com/) as RDF database

## Features

The ETL pipeline has the following features:

* read source files from a S3 bucket
* convert source files to RDF using [RML](https://rml.io/specs/rml/) mappings
* supported formats are CSV, XML, JSON, JSONL, also in compressed (gzipped) form
* the RDF files are written to an S3 bucket, one RDF file per source file
* the RDF files are ingested into a graph using the [GraphDB Preload](https://graphdb.ontotext.com/documentation/10.4/loading-data-using-importrdf.html#load-vs-preload) tool
* adding new files into the source bucket after the initial ingestion will add them as incremental updates

## Setup and Operation

See [ETL Pipeline Setup](docs/Setup.md) for how to set up and run the pipeline.

## Architecture

The following diagram shows the architecture of the ETL pipeline:

<img src="docs/etl-pipeline-architecture.drawio.svg">

See [Architecture](docs/Architecture.md) for a detailed description.

## Copyright

All content in this repository is (c) 2023 by metaphacts.
