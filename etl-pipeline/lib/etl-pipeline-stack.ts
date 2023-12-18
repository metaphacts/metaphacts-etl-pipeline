import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RemovalPolicy, Duration, Stack, CfnParameter } from 'aws-cdk-lib';
import { Bucket, ObjectOwnership } from 'aws-cdk-lib/aws-s3';
import { Source, BucketDeployment } from 'aws-cdk-lib/aws-s3-deployment';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { ETLInstance } from './ingestion-instance';
import { ConvertToRDFLambda } from './convert-to-rdf-lambda';
import { IngestionWorkflow } from './ingestion-workflow';
import { EtlPipelineConfigProps } from './etl-pipeline-config';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';


/** pipeline props including configuration */
type EtlPipelineProps = cdk.StackProps & {
  config: Readonly<EtlPipelineConfigProps>;
};

export class EtlPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: EtlPipelineProps) {
    super(scope, id, props);

    // SNS topic which will receive pipeline events
    const pipelineEventsTopic = new sns.Topic(this, 'EtlPipelineTopic', {
      displayName: 'ETL Pipeline notifications'
    });

    // optionally subscribe the provided email address to the topics
    const emailAddress = props?.config.NOTIFICATION_EMAIL || undefined;
    if (emailAddress) {
      console.log("Creating SNS subscription to topic for address " + emailAddress);
      pipelineEventsTopic.addSubscription(new EmailSubscription(emailAddress, {
        json: false
      }));
    }
    else {
      console.log("Skipping creation of SNS subscription to topic: no email address provided");
    }

    const assetBucket = new Bucket(this, 'assetBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });
    
    // deploy files from ../assets into the bucket
    new BucketDeployment(this, 'assetBucketDeployment', {
      sources: [Source.asset('../assets')],
      destinationBucket: assetBucket,
      retainOnDelete: false,
      include: ['ec2-instance/**'],
      exclude: ['**/node_modules/**', '**/dist/**', '.DS_Store', '.gitignore', '.gitkeep'],
      memoryLimit: 512,
    });
    // deploy some files from ../user-files into the bucket
    new BucketDeployment(this, 'assetBucketDeploymentUserFiles', {
      sources: [Source.asset('../user-files')],
      destinationBucket: assetBucket,
      destinationKeyPrefix: 'user-files/',
      retainOnDelete: false,
      include: ['graphdb.license'],
      exclude: ['README*', '.gitignore', '.gitkeep'],
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
    
    // deploy files from ../mappings into the bucket
    new BucketDeployment(this, 'mappingsBucketDeployment', {
      sources: [Source.asset('../mappings')],
      destinationBucket: mappingsBucket,
      retainOnDelete: false,
      include: ['**'],
      exclude: ['.gitignore', '.gitkeep'],
      memoryLimit: 512,
    });

    const sourceBucket = new Bucket(this, 'sourceBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: true,
    });

    // note: there is no auto-deploy of files from ../source-files 
    // into the bucket, as they are to big for this method

    const outputBucket = new Bucket(this, 'outputBucket', {
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      autoDeleteObjects: false,
    });

    // RDF conversion lambda
    const rdfConversionLambda = new ConvertToRDFLambda(this, 'ConvertToRDFLambda', {
      sourceBucket: sourceBucket,
      mappingsBucket: mappingsBucket,
      mappingsPath: props?.config.MAPPINGS_PREFIX || '',
      outputBucket: outputBucket,
    });
    
    // EC2 instance for ingestion

    const logLevel = new CfnParameter(this, 'logLevel', {
      default: process.env.LOG_LEVEL || 'INFO'
    });
    const sshPubKey = new CfnParameter(this, 'sshPubKey', {
      default: props?.config.SSH_PUB_KEY || undefined
    });
    const instanceType = new CfnParameter(this, 'instanceType', {
      default: props?.config.INSTANCE_TYPE || 't3.xlarge'
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

    // Ingestion workflow
    const ingestionWorkflow = new IngestionWorkflow(this, 'IngestionWorkflow', {
      notificationTopic: pipelineEventsTopic,
      sourceBucket: sourceBucket,
      mappingsBucket: mappingsBucket,
      runtimeBucket: runtimeBucket,
      outputBucket: outputBucket,
      //mappingsPath?: string,
    });
  }
}
