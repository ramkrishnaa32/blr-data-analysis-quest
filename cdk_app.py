#!/usr/bin/env python3
"""
Part 4: AWS CDK Infrastructure
Creates the complete data pipeline infrastructure
"""

import os
from aws_cdk import (
    App,
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    aws_logs as logs,
    CfnOutput
)
from constructs import Construct

class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create S3 bucket for data storage
        data_bucket = s3.Bucket(
            self, "DataBucket",
            bucket_name=f"rearc-data-quest-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    noncurrent_version_expiration=Duration.days(30)
                )
            ]
        )
        
        # Create SQS queue for processing notifications
        processing_queue = sqs.Queue(
            self, "ProcessingQueue",
            queue_name="data-pipeline-processing-queue",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(7),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=sqs.Queue(
                    self, "ProcessingDLQ",
                    queue_name="data-pipeline-processing-dlq"
                )
            )
        )
        
        # Create Lambda layer for dependencies
        dependencies_layer = lambda_.LayerVersion(
            self, "DependenciesLayer",
            code=lambda_.Code.from_asset("lambda_layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
            description="Dependencies for data pipeline lambdas"
        )
        
        # Lambda function for data sync (Part 1 & 2)
        data_sync_lambda = lambda_.Function(
            self, "DataSyncLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="data_sync_handler.handler",
            code=lambda_.Code.from_asset("lambda_functions"),
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name,
                "QUEUE_URL": processing_queue.queue_url
            },
            layers=[dependencies_layer],
            log_retention=logs.RetentionDays.ONE_WEEK
        )
        
        # Grant permissions to data sync lambda
        data_bucket.grant_read_write(data_sync_lambda)
        processing_queue.grant_send_messages(data_sync_lambda)
        
        # Lambda function for data analytics (Part 3)
        analytics_lambda = lambda_.Function(
            self, "AnalyticsLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="analytics_handler.handler",
            code=lambda_.Code.from_asset("lambda_functions"),
            timeout=Duration.minutes(5),
            memory_size=2048,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name
            },
            layers=[dependencies_layer],
            log_retention=logs.RetentionDays.ONE_WEEK
        )
        
        # Grant permissions to analytics lambda
        data_bucket.grant_read(analytics_lambda)
        
        # Create EventBridge rule for daily execution
        daily_rule = events.Rule(
            self, "DailyDataSyncRule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="2",  # Run at 2 AM UTC daily
                month="*",
                week_day="*",
                year="*"
            ),
            description="Trigger data sync lambda daily"
        )
        
        # Add lambda as target for the daily rule
        daily_rule.add_target(targets.LambdaFunction(data_sync_lambda))
        
        # Configure S3 event notification for JSON files
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(processing_queue),
            s3.NotificationKeyFilter(
                prefix="api-data/",
                suffix=".json"
            )
        )
        
        # Configure SQS to trigger analytics lambda
        analytics_lambda.add_event_source_mapping(
            "ProcessingQueueTrigger",
            event_source_arn=processing_queue.queue_arn,
            batch_size=1
        )
        
        # Grant SQS permissions to analytics lambda
        processing_queue.grant_consume_messages(analytics_lambda)
        
        # Outputs
        CfnOutput(
            self, "BucketName",
            value=data_bucket.bucket_name,
            description="S3 bucket for data storage"
        )
        
        CfnOutput(
            self, "QueueUrl",
            value=processing_queue.queue_url,
            description="SQS queue URL for processing"
        )
        
        CfnOutput(
            self, "DataSyncLambdaArn",
            value=data_sync_lambda.function_arn,
            description="Data sync Lambda function ARN"
        )
        
        CfnOutput(
            self, "AnalyticsLambdaArn",
            value=analytics_lambda.function_arn,
            description="Analytics Lambda function ARN"
        )


app = App()
DataPipelineStack(app, "DataPipelineStack")
app.synth()