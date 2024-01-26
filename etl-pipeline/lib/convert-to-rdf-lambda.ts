import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import {
  CompositePrincipal,
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
  /** amount of memory (MB), default 512 MB, max 10GB. Note: this influences costs. */
  lambdaMemoryMB?: number,
  /** amount of ephemeral disk storage (MB), default 512 MB, max 10GB. Note: this influences costs. */
  lambdaStorageMB?: cdk.Size,
  /** timeout for conversion (minutes), default 5 minutes, max 15 minutes. */
  lambdaTimeoutMinutes?: cdk.Duration,
  /** bucket containing source files */
  sourceBucket: s3.Bucket,
  /** bucket containing RML mappings */
  mappingsBucket: s3.Bucket,
  /** bucket in which to place output files */
  outputBucket: s3.Bucket,
  /** bucket in which to place report files and intermediate state */
  runtimeBucket: s3.Bucket,
  /** path within the bucket containing RML mappings */
  mappingsPath?: string,
}

export class ConvertToRDFLambda extends Construct {
  readonly conversionLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: ConvertToRDFLambdaProps) {
    super(scope, id);


    // Create a role for the conversion lambda to assume.  This role will allow the instance to put log events to CloudWatch Logs
    const lambdaRole = new Role(this, 'lambdaRole', {
      assumedBy: new CompositePrincipal(new ServicePrincipal('lambda.amazonaws.com'), new ServicePrincipal('batchoperations.s3.amazonaws.com')),
      inlinePolicies: {
        ['RetentionPolicy']: new PolicyDocument({
          statements: [
            new PolicyStatement({
              resources: ['*'],
              actions: ['logs:PutRetentionPolicy'],
            }),
            new PolicyStatement({
              effect: cdk.aws_iam.Effect.ALLOW,
              actions: [ 'lambda:InvokeFunction' ],
              resources: [ '*' ]
            }),
          ],
        }),
      },
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ManagedPolicy.fromAwsManagedPolicyName('AWSLambda_ReadOnlyAccess'),
      ],
    });


    const mappingsDir = `s3://${props.mappingsBucket.bucketName}/${props.mappingsPath || ''}`;
    const memorySize = props.lambdaMemoryMB ?? 512;
    const ephemeralStorageSize = props.lambdaStorageMB ?? cdk.Size.mebibytes(512);
    const timeout = props.lambdaTimeoutMinutes ?? cdk.Duration.minutes(5);

    this.conversionLambda = new lambda.Function(this, 'RDFConversionLambda', {
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
      handler: 'io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler::handleRequest',
      memorySize: memorySize,
      ephemeralStorageSize: ephemeralStorageSize,
      timeout: timeout,
      environment: {
        "UPLOAD_BUCKET": `${props.outputBucket.bucketName}`,
        "MAPPINGS_DIR": mappingsDir,
        "PROCESS_COLDSTART": "false",
        "PROCESS_DETECT_LASTUPDATE": "false"
      },
      role: lambdaRole
    });
    
    // Function ARN
    new CfnOutput(this, 'RDFConversionLambda.functionArn', {
      value: `${this.conversionLambda.functionArn}`,
    });

    // Function ARN
    new CfnOutput(this, 'RDFConversionLambda.functionName', {
      value: `${this.conversionLambda.functionName}`,
    });

    // allow access to the bucket for this instance
    props.sourceBucket.grantRead(lambdaRole);
    props.mappingsBucket.grantRead(lambdaRole);
    props.outputBucket.grantWrite(lambdaRole);
    props.runtimeBucket.grantReadWrite(lambdaRole)
  }
}
