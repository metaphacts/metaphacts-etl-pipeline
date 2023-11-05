# Welcome to the ETL Pipeline setup

## Overview

### Prerequisites

* [AWS account](https://repost.aws/knowledge-center/create-and-activate-aws-account)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) installed and [configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
* [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) ([TypeScript](https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-typescript.html)) installed
* [Docker](https://docs.docker.com/get-docker/) for running and building local images

## Preparation

### Amazon Web Services (AWS)

The ETL pipeline is implemented for the Amazon Web Services (AWS) public cloud.

#### AWS Account and Credentials

The deployment of the ETL Pipeline as CloudFormation stack will use the AWS credentials provided during the `cdk` tool invocation.

By default, this will use the locally configured credentials (account, AWS access key and secret, region) set up in the [AWS CLI configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
Use `aws configure` to set up access to AWS and the target region.

Please make sure that the user account used to set up the ETL pipeline has access to the AWS Systems Manager (SSM) parameter store, e.g. by granting the managed role `AmazonSSMReadOnlyAccess`.

##### Additional Hints

* When using multiple AWS accounts or users, setting the `AWS_PROFILE` environment variable in the executing shell to the target account/user will automatically use this account for all operations. Alternatively, the active profile can be selected using `cdk --profile=myprofile` command line switch when invoking a CDK command.
* When running this within an EC2 instance configured with a IAM role, the permissions of the role associated with the EC2 instance will be used

#### Test AWS account

Run the following comment in a terminal within the folder containing the ETL pipeline package:

```bash
aws s3 list
```

This should provide a list of S3 buckets in the account and region.

#### Install and Bootstrap AWS CDK and Related Tools

##### Install AWS CDK and Related Tools

The AWS Cloud Development Kit (AWS CDK) is used to define all infrastructure components, synthesize AWS CloudFormation templates and apply them to create a CloudFormation stack in the AWS account of the user.

The AWS CDK including the `cdk` command line tool and `nodejs` and `npm` tools need to be installed as described in the [Working with the AWS CDK
](https://docs.aws.amazon.com/cdk/v2/guide/work-with.html#work-with-prerequisites) guide.

##### Bootstrap CDK

These resources include an Amazon S3 bucket for storing files and IAM roles that grant permissions needed to perform deployments See the [CDK Bootstrap](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html#bootstrapping-howto) guide for details.

The required resources are defined in an AWS CloudFormation stack, called the _bootstrap stack_, which is usually named `CDKToolkit`. Like any AWS CloudFormation stack, it appears in the AWS CloudFormation console once it has been deployed.

In contrast to the regular pipeline deployment, the bootstrapping needs to be done only once per account and region. Also, it needs to be done with admin permissions in that account!
Note: **bootstrapping will create IAM policies etc. with all relevant permissions**, etc. to deploy the ETL pipeline and related infrastructure! Please make sure to review it (find the roles and policies in the AWS CloudFormation console in the `CDKToolkit` stack).

Run this command to bootstrap CDK:

```bash
cdk bootstrap
```

## Pipeline Setup

### Verify Permissions

The bootstrapping (described above) needs to be run as an user with admin permissions.
In order to deploy the pipeline, no admin permissions are required.

However, for the user deploying the pipeline, at least permissions to read AWS Systems Manager (SSM) parameters are required. This can be achieved by granting the managed role `AmazonSSMReadOnlyAccess` to the executing user.
Availability of this minimal set of permissions can be verified by running this command (which determines the version of the bootstrap data):

```bash
aws ssm get-parameters-by-path --path  "/cdk-bootstrap/" --recursive
```

This should return something like this:

```json
{
    "Parameters": [
        {
            "Name": "/cdk-bootstrap/hnb659fds/version",
            "Type": "String",
            "Value": "19",
            "Version": 2,
            "LastModifiedDate": "2023-11-15T13:51:59.126000+01:00",
            "ARN": "arn:aws:ssm:us-east-1:123456789012:parameter/cdk-bootstrap/hnb659fds/version",
            "DataType": "text"
        }
    ]
}
```

### Provide Source Files

The source files to be converted to RDF can be provided either in the `source-files` folder, in which case they will be automatically be uploaded to the respective S3 bucket.

Alternatively, they can also be uploaded the the `source-files` bucket, which is created during pipeline creation.

Source files are accepted in the following formats:

* JSON: `.json` or `.json.gz`
* JSONL (a text file with one JSON document per file): `.jsonl` or `.jsonl.gz`
* XML: `.xml` or `.xml.gz`
* CSV (with header line for column names!): `.csv` or `.csv.gz`

For all variants, the files may also be compressed using GZIP (`.gz`).

### Provide RDF Mappings

RDF Mappings are need to be provided in the `mappings` folder and follow the [RDF Mapping Language (RML)](https://rml.io/specs/rml/) format.

The mapping files should contain a logical source with an empty `rml::source` which during the actual RML mapping process will be replaced with one reading from `stdin`:

```turtle
<my-mapping>
  rml:logicalSource [
    rml:source [] ;
    rml:referenceFormulation ql:JSONPath ;
    rml:iterator "$.root[*]" 
  ] ;
```

Mapping files should be provided as RDF files in Turtle format (`.ttl`).

### Deployment

The deployment can be triggered using the `cdk deploy` command. See the section _Parameter Reference_ below for details on all available parameters!

```bash
cdk deploy --parameters "instanceType=t3.xlarge" --parameters "sshPubKey=ssh-ed25519 AAAA...XXX myuser@example.com"
```

After synthesizing the CloudFormation templates, the CDK tool will ask once more for confirmation and display all created IAM roles and permissions.

The actual deployment will be triggered after confirming this be entering `y` (yes).

The deployment can be watched in the CloudFormation console for the stack `EtlPipeline`. All created resource are listed in the `Resources` tab and all activities are listed in the `Events` tab.

### Cleanup

To delete the ETL pipeline, the CloudFormation stack can be torn down with the following command:

```bash
cdk destroy
```

Please note that some resources like the `output` S3 bucket are intentionally not deleted to avoid accidental data loss. These need to be deleted manually.

## Parameter Reference

The ETL Pipeline can be parameterized with the following configuration values:

### SSH Public Key

The SSH public key to store on the EC2 instance for ingestion. This can be used to connect to the EC2 instance (the command is specified in the output after the ETL pipeline deployment) using SSH.

The key is `sshPubKey`, the value is the full public key. The public key can be typically found in the `~/.ssh/` folder, e.g. in the file `~/.ssh/id_ed25519.pub`.

This parameter is required.

Example:

```bash
cdk deploy --parameters "sshPubKey=ssh-ed25519 AAAA...XXX myuser@example.com"
```

### EC2 Instance Type

The ETL pipeline creates an EC2 instance on which the RDF ingestion is performed. The instance needs to have enough resources to process all RDF files and create a GraphDB data journal using the [GraphDB Preload tool](https://graphdb.ontotext.com/documentation/10.4/loading-data-using-importrdf.html).
See the [GraphDB Sizing](https://graphdb.ontotext.com/documentation/10.4/requirements.html#hardware-sizing) guide for requirements when selecting an [EC2 instance type](https://aws.amazon.com/ec2/instance-types/).

The key is `instanceType`, the value is the canonical name of the instance type, e.g. `t3.xlarge`.

This parameter is optional, the default value when unset is `t3.xlarge`.

```bash
cdk deploy --parameters "instanceType=t3.xlarge"
```
