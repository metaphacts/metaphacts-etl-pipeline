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
  /** source prefix (e.g. folder) within source bucket. When unset, all files will be considered */
  SOURCE_PREFIX?: string,
  /** 
   * source regex pattern for files within source bucket. When unset, all files will be considered.
   * Note: the pattern needs to match the full key name, i.e. inclusing any prefix defined using SOURCE_PREFIX.
   */
  SOURCE_PATTERN?: string,
  /** mappings prefix (e.g. folder) within mappings bucket. When unset, all files will be considered */
  MAPPINGS_PREFIX?: string,

  /** RDF conversion: amount of memory (MB), default 512 MB. Note: this influences costs. */
  RDFCONVERT_MEMORY?: string,
  /** RDF conversion: amount of ephemeral disk storage (MB), default 512 MB. Note: this influences costs. */
  RDFCONVERT_STORAGE?: string,
  /** RDF conversion: timeout for conversion (minutes), default 5 minutes. */
  RDFCONVERT_TIMEOUT?: string,

  /** schedule execution when configured
   * To run every day at 6PM UTC, use the expression '0 18 ? * MON-FRI'
   * See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
   */
  EXECUTION_SCHEDULE?: string,
};

export const getConfig = (): EtlPipelineConfigProps => ({
  PIPELINE_ID: process.env.PIPELINE_ID,
  AWS_REGION: process.env.AWS_REGION,
  AWS_ACCOUNT: process.env.AWS_ACCOUNT,
  SSH_PUB_KEY: process.env.SSH_PUB_KEY,
  INSTANCE_TYPE: process.env.INSTANCE_TYPE,
  NOTIFICATION_EMAIL: process.env.NOTIFICATION_EMAIL,
  SOURCE_PREFIX: process.env.SOURCE_PREFIX,
  SOURCE_PATTERN: process.env.SOURCE_PATTERN,
  MAPPINGS_PREFIX: process.env.MAPPINGS_PREFIX,
  RDFCONVERT_MEMORY: process.env.RDFCONVERT_MEMORY,
  RDFCONVERT_STORAGE: process.env.RDFCONVERT_STORAGE,
  RDFCONVERT_TIMEOUT: process.env.RDFCONVERT_TIMEOUT,
  EXECUTION_SCHEDULE: process.env.EXECUTION_SCHEDULE,
});
