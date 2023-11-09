# Setup of RDF conversion Lambda


## Create the execution role

See [Create the execution role](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-awscli.html#with-userapp-walkthrough-custom-events-create-iam-role) in the AWS Lambda docs 
and [Creating an S3 Batch Operations IAM role](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html#batch-ops-iam-role-policies-create) in the S3 Batch Operations docs.

```
aws iam create-role --role-name lambda-execution --assume-role-policy-document file://execution-role.json
```


## Notes

Output of role creation above:

```
export LAMBDA_ROLE_ARN="arn:aws:iam::123456789012:role/lambda-execution"
```