# User Files

This folder contains files that can be supplied by the user of the ETL pipeline. The files may be copied to the ingestion instance or used as lambda implementation.

## Supported Files

The following sections describe the supported files in this folder.

### GraphDB license

Add a valid GraphDB license as file `graphdb.license` to enable enterprise features of GraphDB. If omitted, GraphDB will run in "free" mode which limits performance and feature set. See the [GraphDB Licensing Guide](https://graphdb.ontotext.com/documentation/10.4/set-up-your-license.html) for details.
