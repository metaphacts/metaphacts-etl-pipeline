# Welcome to the ETL Pipeline setup

## Overview

The ETL Pipeline is implemented using an AWS CloudFormation stack defined using a AWS CDK application.


### Prerequisites

* AWS account
* AWS CLI installed and configured
* AWS CDK (TypeScript) installed
* Docker for running and building local images

### AWS Account and Credentials

Deployment of the ETL Pipeline as CloudFormation stack will use the AWS credentials provided during the `cdk` tool invocation.

By default, this will use the locally configured credentials (account, AWS access key and secret, region) set up in the AWS CLI configuration. 
Use `aws configure` to set up access to AWS and the intended region.

* When using multiple AWS accounts or users, setting the `AWS_PROFILE` environment variable in the executing shell to the target account/user will automatically use this account for all operations.
* When running this within an EC2 instance configured with a IAM role, the permissions of the role associated with the EC2 instance will be used

### Useful commands

* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template
* `cdk destroy`     delete all resources created in the pipeline

## Implementation Details

This is a CDK project with TypeScript based on the [CDK Workshop](https://cdkworkshop.com/20-typescript/).

The `cdk.json` file tells the CDK Toolkit how to execute your app.

### Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template
