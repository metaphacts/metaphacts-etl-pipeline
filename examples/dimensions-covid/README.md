# Dimensions Covid Dataset

## Overview

The [Dimensions Covid Dataset](https://www.dimensions.ai/covid19/) provides information on publications (academic papers), research grants, datasets, and clinical trials. The zipped dataset can be downloaded from [Figshare](https://dimensions.figshare.com/articles/dataset/Dimensions_COVID-19_publications_datasets_and_clinical_trials/11961063) (total download size: ca. 1GB, uncompressed ca. 1.1GB).

## Setup

Follow these steps to use this dataset:

* ensure that the ETL pipeline is set up as described int the [setup guide](../../docs/Setup.md). 
* copy the provided RML mappings from the example `mappings` folder into the main mappings folder in the root directory of this pipeline definition or directly into the S3 bucket containing the mappings. 
* download and extract the source files from [Figshare](https://dimensions.figshare.com/articles/dataset/Dimensions_COVID-19_publications_datasets_and_clinical_trials/11961063) and put them into the source data S3 bucket.
* split long CSV files into smaller files of max. 5000 lines. Please note that the CSV files may contain multi-line values.
* copy the static RDF files (ontology, dataset description) from the example `source-files` folder into the source data S3 bucket.
* trigger the ETL workflow from the [AWS StepFunctions console](https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/statemachines) or wait for the time-based trigger to start the workflow at the set time.

Preparing all source files including downloading the dataset, splitting the CSV files, and adding the RDF files with ontologies, etc. can be performed using the provided script `prepare-dataset.sh`. The script uses `curl`, `unzip`, and `python3` to prepare the source files in folder `source-files/covid` in the project root directory (i.e. not the `source-files` folder within this Dimensions Covid example). It can be invoked like this:

```
prepare-dataset.sh
```

Note: when copying mappings into the local folders, `cdk deploy` must be re-run to upload the files to the respective S3 buckets.

## Local testing

To test RDF conversion of this example dataset locally, follow these steps:

* launch the lambda locally from folder `etl-pipeline/lambda-convert-to-rdf/` using the command `./gradlew quarkusDev`.
This will also build the Lamdba on first invocation.
* download and extract the source files from [Figshare](https://dimensions.figshare.com/articles/dataset/Dimensions_COVID-19_publications_datasets_and_clinical_trials/11961063) and put them into the source data S3 bucket.
Please ensure that the four CSV files are contained directly int the source folder without further sub folders! The file names should match those listed in `examples/dimensions-covid/dimensions-covid-request.json`.
* copy the static RDF files (ontology, dataset description) from the example `source-files` folder into the source data S3 bucket.
* trigger the conversion by running the provided script `examples/dimensions-covid/invoke.sh`. 

The script sends a message to the locally running Lambda implementation launched in the first step with a list of source files to process. The folder locations are defined in the `dev` configuration in file `etl-pipeline/lambda-convert-to-rdf/src/main/resources/application.properties`, the names of the source files to be processed are listed in `examples/dimensions-covid/dimensions-covid-request.json`. The results of the conversion process can be found in folder `output-files`.
