#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { EtlPipelineStack } from '../lib/etl-pipeline-stack';
import { getConfig } from "../lib/etl-pipeline-config";

// get pipeline config from .env file
const config = getConfig();

console.log("Configuration:");
console.log(config);

const pipelineId = 'EtlPipelineStack' + (config.PIPELINE_ID ? '-' + config.PIPELINE_ID : '');

const app = new cdk.App();
new EtlPipelineStack(app, pipelineId, {
  env: { 
    account: config.AWS_ACCOUNT || process.env.CDK_DEFAULT_ACCOUNT, 
    region: config.AWS_REGION || process.env.CDK_DEFAULT_REGION 
  },
  config
});