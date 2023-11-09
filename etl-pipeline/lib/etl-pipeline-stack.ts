import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RemovalPolicy, Duration, Stack, CfnParameter } from 'aws-cdk-lib';
import { Bucket, ObjectOwnership } from 'aws-cdk-lib/aws-s3';
import { Source, BucketDeployment } from 'aws-cdk-lib/aws-s3-deployment';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { ETLInstance } from './ingestion-instance';
import { RDFConversionLambda } from './rdf-conversion-lambda';
import { LoggingFormat } from 'aws-cdk-lib/aws-appmesh';

export class EtlPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // SNS topic which will receive pipeline events
    const pipelineEventsTopic = new sns.Topic(this, 'EtlPipelineTopic');


    const assetBucket = new Bucket(this, 'assetBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });
    
    new BucketDeployment(this, 'assetBucketDeployment', {
      sources: [Source.asset('../assets')],
      destinationBucket: assetBucket,
      retainOnDelete: false,
      include: ['ec2-instance/**'],
      exclude: ['**/node_modules/**', '**/dist/**'],
      memoryLimit: 512,
    });

    const runtimeBucket = new Bucket(this, 'runtimeBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });

    const mappingsBucket = new Bucket(this, 'mappingsBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });
    
    new BucketDeployment(this, 'mappingsBucketDeployment', {
      sources: [Source.asset('../mappings')],
      destinationBucket: mappingsBucket,
      retainOnDelete: false,
      include: ['**'],
      exclude: ['.gitignore'],
      memoryLimit: 512,
    });

    const sourceBucket = new Bucket(this, 'sourceBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });

    const outputBucket = new Bucket(this, 'outputBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: false,
    });

    // hello Lambda
    const hello = new lambda.Function(this, 'HelloLambda', {
      runtime: lambda.Runtime.NODEJS_16_X,    // execution environment
      code: lambda.Code.fromAsset('hello-lambda'),  // code loaded from "hello-lambda" directory
      handler: 'hello.handler'                // file is "hello", function is "handler"
    });

    // RDF conversion lambda
    const rdfConversionLambda = new RDFConversionLambda(this, 'RDFConversionLambda', {
      sourceBucket: sourceBucket,
      mappingsBucket: mappingsBucket,
      outputBucket: outputBucket,
    });
    
    // EC2 instance for ingestion

    const logLevel = new CfnParameter(this, 'logLevel', {
      default: process.env.LOG_LEVEL || 'INFO'
    });
    const sshPubKey = new CfnParameter(this, 'sshPubKey', {
      default: process.env.SSH_PUB_KEY || undefined
    });
    const instanceType = new CfnParameter(this, 'instanceType', {
      default: process.env.INSTANCE_TYPE || 't3.xlarge'
    });

    console.log('Environment:');
    //console.log(process.env);
    console.log('log level:      ' + logLevel.valueAsString);
    console.log('SSH public key: ' + sshPubKey.valueAsString);
    console.log('instance type:  ' + instanceType.valueAsString);
    
    const etlInstanceProps = {
      logLevel: logLevel.valueAsString,
      sshPubKey: sshPubKey.valueAsString || ' ',
      instanceType : instanceType.valueAsString,
    };

    const etlInstance = new ETLInstance(this, 'ingestion', {
      ...etlInstanceProps,
      assetBucket: assetBucket
    });
  }
}
