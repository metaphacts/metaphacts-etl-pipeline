import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as fs from 'fs'
import { Construct } from 'constructs';
import {
  AccountRootPrincipal,
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
import { IntegrationPattern, JsonPath, TaskInput } from 'aws-cdk-lib/aws-stepfunctions';

export interface IngestionWorkflowProps {
  /** SNS topic to which to send notifications */
  notificationTopic: sns.Topic,
  /** bucket containing source files */
  sourceBucket: s3.Bucket,
  /** source prefix (e.g. folder) within source bucket. When unset, all files will be considered */
  sourcePrefix?: string,
  /** 
   * source regex pattern for files within source bucket. When unset, all files will be considered.
   * Note: the pattern needs to match the full key name, i.e. inclusing any prefix defined using SOURCE_PREFIX.
   */
  sourcePattern?: string,

  /** bucket containing RML mappings */
  mappingsBucket: s3.Bucket,
  /** path within the bucket containing RML mappings */
  mappingsPath?: string,

  /** bucket containing runtime data */
  runtimeBucket: s3.Bucket,
  /** bucket in which to place output files */
  outputBucket: s3.Bucket,
  /** lambda function to process each source file  */
  processingLambda: lambda.Function,

  /** schedule execution when configured
   * To run every day at 6PM UTC, use the expression '0 18 ? * MON-FRI'
   * See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
   */
  executionSchedule?: string,
}

export class IngestionWorkflow extends Construct {
  constructor(scope: Construct, id: string, props: IngestionWorkflowProps) {
    super(scope, id);

  /** ------------------ Lambda Handlers Definition ------------------ */

  // Create a role for the conversion lambda to assume.  This role will allow the instance to put log events to CloudWatch Logs
  const lambdaRole = new Role(this, 'lambdaRole', {
    assumedBy: new CompositePrincipal(new ServicePrincipal('lambda.amazonaws.com'), new ServicePrincipal('batchoperations.s3.amazonaws.com')),
    inlinePolicies: {
      ['RetentionPolicy']: new PolicyDocument({
        statements: [
          new PolicyStatement({
            resources: ['*'],
            actions: [
              'logs:PutRetentionPolicy',
              'states:SendTask*',
            ],
          }),
        ],
      }),
      
    },
  });

  const getStatusLambda = new lambda.Function(this, 'CheckLambda', {
    code: new lambda.InlineCode(fs.readFileSync('lambda-check-status/check_status.py', { encoding: 'utf-8' })),
    handler: 'index.main',
    timeout: cdk.Duration.seconds(30),
    runtime: lambda.Runtime.PYTHON_3_9,
    role: lambdaRole
  });

  const submitLambda = new lambda.Function(this, 'SubmitLambda', {
    code: new lambda.InlineCode(fs.readFileSync('lambda-submit/submit.py', { encoding: 'utf-8' })),
    handler: 'index.main',
    timeout: cdk.Duration.seconds(30),
    runtime: lambda.Runtime.PYTHON_3_9,
    role: lambdaRole
  });

  const createS3BatchManifestLambda = new lambda.Function(this, 'CreateS3BatchManifest', {
    //code: new lambda.InlineCode(fs.readFileSync('lambda-create-s3batch-manifest/submit.py', { encoding: 'utf-8' })),
    code: lambda.Code.fromAsset('lambda-create-s3batch-manifest', {
      bundling: {
        image: lambda.Runtime.PYTHON_3_9.bundlingImage,
        command: [
          'bash',
          '-c',
          'pip install -r requirements.txt -t /asset-output && cp -au . /asset-output',
        ],
      },
    }),
    handler: 'create-s3batch-manifest.lambda_handler',
    timeout: cdk.Duration.minutes(15),
    runtime: lambda.Runtime.PYTHON_3_9,
    environment: {
      "SOURCE_BUCKET": `${props.sourceBucket.bucketName}`,
      "SOURCE_PREFIX": `${props.sourcePrefix  || ''}`,
      "SOURCE_PATTERN": `${props.sourcePattern || '.*'}`,
      "RUNTIME_BUCKET": `${props.runtimeBucket.bucketName}`,
    },
    role: lambdaRole
  });
  props.runtimeBucket.grantReadWrite(createS3BatchManifestLambda)
  props.sourceBucket.grantRead(createS3BatchManifestLambda)

  /** ------------------ Step functions Definition ------------------ */

  const publishStartWorkflowMessage = new tasks.SnsPublish(this, 'StartWorkflow', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromObject({
      message: 'Launching ETL Workflow'
    }),
    subject: 'ETL: Launching Workflow'
  });

  const publishEndWorkflowMessage = new tasks.SnsPublish(this, 'EndWorkflow', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromJsonPathAt('$.Payload'),
    resultPath: '$.sns',
    subject: 'ETL: Workflow is finished'
  });


  const publishStartRDFConversionMessage = new tasks.SnsPublish(this, 'StartRDFConversion', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromObject({
      message: 'Starting RDF conversion'
    }),
    subject: 'ETL: Starting RDF conversion'
  });

  const publishEndRDFConversionMessage = new tasks.SnsPublish(this, 'EndRDFConversion', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromObject({
      message: 'RDF conversion is finished'
    }),
    subject: 'ETL: RDF conversion is finished'
  });

  const publishStartIngestionMessage = new tasks.SnsPublish(this, 'StartIngestion', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromObject({
      message: 'Starting Ingestion'
    }),
    subject: 'ETL: Starting Ingestion'
  });

  const publishEndIngestionMessage = new tasks.SnsPublish(this, 'EndIngestion', {
    topic: props?.notificationTopic,
    message: sfn.TaskInput.fromObject({
      message: 'Ingestion is finished'
    }),
    subject: 'ETL: Ingestion is finished'
  });

  const waitX = new sfn.Wait(this, 'Wait X Seconds', {
    /**
     *  You can also implement with the path stored in the state like:
     *  sfn.WaitTime.secondsPath('$.waitSeconds')
     */
    time: sfn.WaitTime.duration(cdk.Duration.seconds(30)),
  });
  const getStatus = new tasks.LambdaInvoke(this, 'Get Job Status', {
    lambdaFunction: getStatusLambda,
    outputPath: '$.Payload',
  });

  const createS3BatchOperationsManifest = new tasks.LambdaInvoke(this, 'Create S3 Batch Manifest', {
    lambdaFunction: createS3BatchManifestLambda,
    integrationPattern: IntegrationPattern.WAIT_FOR_TASK_TOKEN,
    payload: TaskInput.fromObject({
      taskToken: JsonPath.taskToken,
      runtimeBucket: props.runtimeBucket.bucketName,
      sourceBucket: props.sourceBucket.bucketName,
      sourcePrefix: props.sourcePrefix,
      sourcePattern: props.sourcePattern,
    }),
    outputPath: '$.Payload',
  });

  const accountId = Stack.of(this).account;

  const triggerS3BatchOperations = new tasks.CallAwsService(this, 'Trigger S3 BatchOperations', {
    service: 's3control',
    action: 'createJob',
    resultPath: '$.CreateJob',
    parameters: {
      AccountId: accountId,
      ClientRequestToken: JsonPath.uuid(),
      Priority: 2,
      Operation: {
        LambdaInvoke: {
          FunctionArn: props.processingLambda.functionArn,
        }
      },
      Report: {
        Bucket: JsonPath.format('arn:aws:s3:::{}', JsonPath.stringAt("$.ManifestBucket")),
        Format: 'Report_CSV_20180820',
        Enabled: true,
        Prefix: 'reports',
        ReportScope: 'AllTasks'
      },
      RoleArn: props.processingLambda.role?.roleArn,
      Manifest: {
        Spec: {
          Format: 'S3BatchOperations_CSV_20180820',
          Fields: [
            'Bucket',
            'Key'
          ]
        },
        Location: {
          "ObjectArn": JsonPath.format('arn:aws:s3:::{}/{}', JsonPath.stringAt("$.ManifestBucket"),JsonPath.stringAt("$.ManifestKey")),
          "ETag": sfn.JsonPath.stringAt("$.ManifestEtag")
        }
      },
      ConfirmationRequired: false
    },
    iamResources: [ props.processingLambda.functionArn ],
    additionalIamStatements: [ 
      new PolicyStatement({

        actions: [
          'lambda:InvokeFunction'
        ],
        effect: cdk.aws_iam.Effect.ALLOW,
        resources: [ 
          props.processingLambda.functionArn
        ]
      })],
  });

  const getS3BatchOperationsStatus = new tasks.CallAwsService(this, 'Get S3 BatchOperations Status', {
    service: 's3control',
    action: 'describeJob',
    resultPath: '$.DescribeJob',
    parameters: {
      AccountId: accountId,
      JobId: sfn.JsonPath.stringAt('$.CreateJob.JobId'),
    },
    iamResources: [ '*' ],
  });

  const waitForRDFConversion = new sfn.Wait(this, 'Wait for RDF Conversion', {
    /**
     *  You can also implement with the path stored in the state like:
     *  sfn.WaitTime.secondsPath('$.waitSeconds')
     */
    time: sfn.WaitTime.duration(cdk.Duration.seconds(30)),
  });

  const jobFailed = new sfn.Fail(this, 'Job Failed', {
    cause: 'AWS Batch Job Failed',
    error: 'DescribeJob returned FAILED',
  });

  const finalStatus = new tasks.LambdaInvoke(this, 'Get Final Job Status', {
    lambdaFunction: getStatusLambda,
    outputPath: '$.Payload',
  });

  // Create chain
  const definition = publishStartWorkflowMessage
    .next(publishStartRDFConversionMessage)
    .next(createS3BatchOperationsManifest)
    .next(triggerS3BatchOperations)
    .next(waitForRDFConversion)
    .next(getS3BatchOperationsStatus)
    .next(new sfn.Choice(this, 'RDF Conversion Complete?')
      // Look at the "status" field
      .when(sfn.Condition.stringEquals('$.DescribeJob.Job.Status', 'New'), waitForRDFConversion)
      .when(sfn.Condition.stringEquals('$.DescribeJob.Job.Status', 'Active'), waitForRDFConversion)
      .when(sfn.Condition.stringEquals('$.DescribeJob.Job.Status', 'Completing'), waitForRDFConversion)
      .when(sfn.Condition.stringEquals('$.DescribeJob.Job.Status', 'Complete'), publishEndRDFConversionMessage)
      .when(sfn.Condition.stringEquals('$.DescribeJob.Job.Status', 'Failed'), finalStatus)
      .otherwise(waitForRDFConversion));

  publishEndRDFConversionMessage
    .next(finalStatus);

  finalStatus
    // TODO check %error and %succeeded
    .next(publishStartIngestionMessage)
    // TODO trigger ingestion
    .next(publishEndIngestionMessage)
    .next(publishEndWorkflowMessage);

  // Create state machine
  const stateMachine = new sfn.StateMachine(this, 'ETLStateMachine', {
    definitionBody: sfn.DefinitionBody.fromChainable(definition),
    comment: 'ETL Pipeline',
    timeout: cdk.Duration.minutes(5),
  });

  // Grant lambda execution roles
  submitLambda.grantInvoke(stateMachine.role);
  getStatusLambda.grantInvoke(stateMachine.role);
  createS3BatchManifestLambda.grantInvoke(stateMachine.role);
  
  props.notificationTopic.grantPublish(stateMachine);
  
  props.mappingsBucket.grantRead(stateMachine.role);
  props.sourceBucket.grantRead(stateMachine.role);
  props.outputBucket.grantWrite(stateMachine.role);

  // grant permissions fro s3control (S3 Batch Operations service)
  stateMachine.role.addToPrincipalPolicy(new PolicyStatement({
    // see https://aws.permissions.cloud/api/s3#S3Control_CreateJob for required permissions
    actions: ['s3control:*', 's3:CreateJob', 's3:DescribeJob', 'iam:PassRole'],
    effect: cdk.aws_iam.Effect.ALLOW,
    resources: ['*'],
    //principals: [new AccountRootPrincipal()],
  }));
  

  /** ------------------ Events Rule Definition ------------------ */

  // schedule execution when configured
  /**
   * Run every day at 6PM UTC: '0 18 ? * MON-FRI'
   * See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
   */
  const schedule = props?.executionSchedule || undefined;
  if (schedule) {
    const cronExpression = 'cron(' + schedule + ')';
    /**
     *  Run every day at 6PM UTC
     * See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
     */
    const rule = new events.Rule(this, 'Rule', {
      schedule: events.Schedule.expression(cronExpression)
    });
    rule.addTarget(new targets.SfnStateMachine(stateMachine));
  }

  }
}