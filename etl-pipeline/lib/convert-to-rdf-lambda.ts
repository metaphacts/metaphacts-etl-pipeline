import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import {
  Role,
  ServicePrincipal,
  ManagedPolicy,
  PolicyDocument,
  PolicyStatement,
} from 'aws-cdk-lib/aws-iam';
import { RemovalPolicy, Duration, Stack, CfnOutput } from 'aws-cdk-lib';
import { Bucket, ObjectOwnership } from 'aws-cdk-lib/aws-s3';
import { Source, BucketDeployment } from 'aws-cdk-lib/aws-s3-deployment';

export interface ConvertToRDFLambdaProps {
  /** bucket containing source files */
  sourceBucket: s3.Bucket,
  /** bucket containing RML mappings */
  mappingsBucket: s3.Bucket,
  /** bucket in which to place output files */
  outputBucket: s3.Bucket,
  /** path within the bucket containing RML mappings */
  mappingsPath?: string,
}

export class ConvertToRDFLambda extends Construct {
  constructor(scope: Construct, id: string, props: ConvertToRDFLambdaProps) {
    super(scope, id);


    // Create a role for the EC2 instance to assume.  This role will allow the instance to put log events to CloudWatch Logs
    const lambdaRole = new Role(this, 'lambdaRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: {
        ['RetentionPolicy']: new PolicyDocument({
          statements: [
            new PolicyStatement({
              resources: ['*'],
              actions: ['logs:PutRetentionPolicy'],
            }),
          ],
        }),
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ManagedPolicy.fromAwsManagedPolicyName('AWSLambda_ReadOnlyAccess'),
      ],
    });

    const conversionLambda = new lambda.Function(this, 'RDFConversionLambda', {
      runtime: lambda.Runtime.JAVA_21,    // execution environment
      code: lambda.Code.fromAsset('lambda-convert-to-rdf', {
        bundling: {
          // build with Java 17
          image: lambda.Runtime.JAVA_17.bundlingImage,
          command: [
            "/bin/bash",
            "-c",
            "./gradlew build -x test"
              + " && cp build/function.zip /asset-output/"
          ]
        }
      }),  // code loaded from "lambda-convert-to-rdf" directory
      // TODO refactor package name
      handler: 'com.metaphacts.etl.lambda.ConvertToRDFLambda::handleRequest',
      memorySize: 1024,
      timeout: cdk.Duration.minutes(15),
      environment: {
        "UPLOAD_BUCKET": `${props.outputBucket.bucketName}`,
        "PROCESS_COLDSTART": "false",
        "PROCESS_DETECT_LASTUPDATE": "false"
      }
    });

    // Function ARN
    new CfnOutput(this, 'RDFConversionLambda.functionArn', {
      value: `${conversionLambda.functionArn}`,
    });

    // Function ARN
    new CfnOutput(this, 'RDFConversionLambda.functionName', {
      value: `${conversionLambda.functionName}`,
    });

    // allow access to the bucket for this instance
    props.sourceBucket.grantRead(lambdaRole);
    props.mappingsBucket.grantRead(lambdaRole);
    props.outputBucket.grantWrite(lambdaRole);
  }
}
