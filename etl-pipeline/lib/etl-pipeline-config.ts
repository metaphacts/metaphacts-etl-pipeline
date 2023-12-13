import path = require("path");
import * as dotenv from "dotenv";

// support reading configuration from .env file
// see https://conermurphy.com/blog/making-environment-variables-effortless-aws-cdk-stacks

// configure dotenv to read from our `.env` file
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// pipeline config parameters
// when editing, also add an example entry to .env.template
export type EtlPipelineConfigProps = {
  /** 
   * ID of the pipeline. If provided, it will be appended to the pipeline id 
   * and allows to provision multiple independent instances of the pipeline.
   * 
   * The id should only consist of letters, numbers and dashes, 
   * no whitespace or special characters.
   * */
  PIPELINE_ID?: string;
  /**
   * AWS accountin which to deploy the pipeline. When unset, the current region  
   * * of the profile will be used.
   */
  AWS_ACCOUNT?: string;
  /**
   * AWS region in which to deploy the pipeline. When unset, the current region 
   * of the profile or the default region of the account will be used.
   */
  AWS_REGION?: string;
  /** SSH public key for the EC instance (optional). When unset, no remote access will be possible. */
  SSH_PUB_KEY?: string;
  /** instance type for ETL EC2 instance, e.g. 't3.medium' */
  INSTANCE_TYPE?: string;
  /** 
   * Email address to register for notifications from this pipeline. 
   * Note: The address will receive a subscription confirmation email.
   * */
  NOTIFICATION_EMAIL?: string;
};

export const getConfig = (): EtlPipelineConfigProps => ({
  PIPELINE_ID: process.env.PIPELINE_ID,
  AWS_REGION: process.env.AWS_REGION,
  AWS_ACCOUNT: process.env.AWS_ACCOUNT,
  SSH_PUB_KEY: process.env.SSH_PUB_KEY,
  INSTANCE_TYPE: process.env.INSTANCE_TYPE,
  NOTIFICATION_EMAIL: process.env.NOTIFICATION_EMAIL,
});
