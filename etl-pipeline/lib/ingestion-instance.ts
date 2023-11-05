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
import {
  Peer,
  Port,
  SubnetType,
  Vpc,
  SecurityGroup,
  Instance,
  InstanceType,
  InstanceClass,
  InstanceSize,
  CloudFormationInit,
  InitConfig,
  InitFile,
  InitCommand,
  UserData,
  MachineImage,
  AmazonLinuxCpuType,
} from 'aws-cdk-lib/aws-ec2';
import { Bucket, ObjectOwnership } from 'aws-cdk-lib/aws-s3';
import { Source, BucketDeployment } from 'aws-cdk-lib/aws-s3-deployment';

export interface ETLInstanceProps {
  /** instance type for ETL EC2 instance, e.g. 't3.medium' */
  instanceType : string;
  /** SSH public key */
  sshPubKey: string;
  /** bucket containing assets to download into the instance */
  assetBucket: s3.Bucket;
}

export class ETLInstance extends Construct {
  constructor(scope: Construct, id: string, props: ETLInstanceProps) {
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
        ManagedPolicy.fromAwsManagedPolicyName('EC2InstanceConnect'),
      ],
    });

    // use default VPC
    const vpc = Vpc.fromLookup(this, 'ImportVPC', { isDefault: true } );

    const cpuType = AmazonLinuxCpuType.X86_64;


    // Create a security group for SSH
    const ec2InstanceSecurityGroup = new SecurityGroup(this, 'SSHSecurityGroup', {
      vpc: vpc,
      description: 'Security Group for SSH and HTTPS',
      allowAllOutbound: true,
    });

    // Allow SSH inbound traffic on TCP port 22
    ec2InstanceSecurityGroup.addIngressRule(Peer.anyIpv4(), Port.tcp(22));
    // Allow HTTPS inbound traffic on TCP port 443
    ec2InstanceSecurityGroup.addIngressRule(Peer.anyIpv4(), Port.tcp(443));

    const userData = UserData.forLinux();

    // Add user data that is used to configure the EC2 instance
    userData.addCommands(
      'yum update -y',
      'curl -sL https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo',
      'curl -sL https://rpm.nodesource.com/setup_18.x | sudo -E bash - ',
      'yum install -y amazon-cloudwatch-agent nodejs python3-pip zip unzip docker yarn',
      'sudo systemctl enable docker',
      'sudo systemctl start docker',
      'aws s3 cp s3://' +
      props.assetBucket.bucketName +
        '/ec2-instance /home/ec2-user/ec2-instance --recursive',
    );
    
    const machineImage = MachineImage.latestAmazonLinux2023({
        cachedInContext: false,
        cpuType: cpuType,
      });

    // Create the EC2 instance
    const etlInstance = new Instance(this, 'Instance', {
      vpc: vpc,
      instanceType: new InstanceType(props.instanceType),
      machineImage: machineImage,
      userData: userData,
      securityGroup: ec2InstanceSecurityGroup,
      init: CloudFormationInit.fromConfigSets({
        configSets: {
          default: ['config'],
        },
        configs: {
          config: new InitConfig([
            InitFile.fromObject('/etc/config.json', {
              // Use CloudformationInit to create an object on the EC2 instance
              STACK_ID: Stack.of(this).artifactId,
            }),
            InitFile.fromFileInline(
              // Use CloudformationInit to copy a file to the EC2 instance
              '/tmp/amazon-cloudwatch-agent.json',
              '../assets/ec2-instance/config/amazon-cloudwatch-agent.json',
            ),
            InitFile.fromFileInline(
              '/etc/config.sh',
              '../assets/ec2-instance/config/config.sh',
            ),
            InitFile.fromString(
              // Use CloudformationInit to write a string to the EC2 instance
              '/home/ec2-user/.ssh/authorized_keys',
              props.sshPubKey + '\n',
            ),
            InitCommand.shellCommand('chmod +x /etc/config.sh'), // Use CloudformationInit to run a shell command on the EC2 instance
            InitCommand.shellCommand('/etc/config.sh'),
          ]),
        },
      }),

      initOptions: {
        timeout: Duration.minutes(10),
        includeUrl: true,
        includeRole: true,
        printLog: true,
      },
      role: serverRole,
    });

    // Add the Security Group to the EC2 instance
    etlInstance.addSecurityGroup(ec2InstanceSecurityGroup);

    // SSH Command to connect to the EC2 Instance
    new CfnOutput(this, 'sshCommand', {
      value: `ssh ec2-user@${etlInstance.instancePublicDnsName}`,
    });

    // allow access to the bucket for this instance
    props.assetBucket.grantRead(serverRole);
  }
}
