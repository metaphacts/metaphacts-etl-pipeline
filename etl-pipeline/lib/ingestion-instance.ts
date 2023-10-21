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

export interface IngestionInstanceProps {
  /** bucket containing assets to download into the instance */
  assetBucket: s3.Bucket;
}

export class IngestionInstance extends Construct {
  constructor(scope: Construct, id: string, props: IngestionInstanceProps) {
    super(scope, id);


    // Create a role for the EC2 instance to assume.  This role will allow the instance to put log events to CloudWatch Logs
    const serverRole = new Role(this, 'serverEc2Role', {
      assumedBy: new ServicePrincipal('ec2.amazonaws.com'),
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
        ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        ManagedPolicy.fromAwsManagedPolicyName('CloudWatchAgentServerPolicy'),
      ],
    });

    // allow access to the bucket for this instance
    props.assetBucket.grantRead(serverRole);
  }
}