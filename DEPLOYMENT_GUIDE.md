
## **Part 4: Infrastructure as Code & Data Pipeline with AWS CDK**

## Complete deployment and usage of the automated data pipeline that combines Parts 1, 2, and 3

### **Infrastructure Components**
- **S3 Bucket**: `rearc-data-quest-650653607934` - Data storage for BLS and API data
- **SQS Queue**: `data-pipeline-processing-queue` - Triggers analytics when new data arrives
- **Lambda Functions**:
  - **DataSyncLambda**: Executes Parts 1 & 2 (BLS sync + API fetch)
  - **AnalyticsLambda**: Executes Part 3 analytics when triggered by SQS
- **EventBridge Rule**: Daily trigger at 2 AM UTC for data sync
- **Lambda Layer**: Contains all dependencies (requests, beautifulsoup4, pandas, numpy, boto3)

### **Architecture Flow**
```
EventBridge (Daily 2 AM) → DataSyncLambda → S3 Storage → S3 Notification → SQS → AnalyticsLambda
```

## **Deployment Commands Used**

```bash
# 1. Create virtual environment and install dependencies
source .venv/bin/activate
pip install -r requirements-cdk.txt

# 2. Create lambda layer with dependencies
mkdir -p lambda_layer/python
pip install requests beautifulsoup4 pandas numpy boto3 -t lambda_layer/python/

# 3. Bootstrap CDK environment (first time only)
cdk bootstrap

# 4. Deploy infrastructure
cdk deploy
```

### **Part 1: BLS Data Sync (Automated)**
- **Function**: `DataSyncLambda`
- **Schedule**: Daily at 2 AM UTC
- **Process**: 
  - Scrapes BLS website for files
  - Downloads and uploads to S3
  - Maintains sync metadata
- **S3 Location**: `bls-data/` prefix

### **Part 2: API Data Fetching (Automated)**
- **Function**: `DataSyncLambda` (same function)
- **Schedule**: Daily at 2 AM UTC
- **Process**:
  - Fetches population data from DataUSA API
  - Saves as timestamped JSON files
  - Maintains `latest.json` for analytics
- **S3 Location**: `api-data/` prefix

### **Part 3: Data Analytics (Triggered)**
- **Function**: `AnalyticsLambda`
- **Trigger**: SQS message when new JSON data arrives
- **Process**:
  - Loads BLS and population data from S3
  - Performs all three analyses from Part 3
  - Logs results to CloudWatch


## View Lambda Logs
```bash
# Data Sync Lambda logs
aws logs tail /aws/lambda/DataPipelineStack-DataSyncLambdaE3D45EA6-3aWy3hRd7zG3 --follow

# Analytics Lambda logs
aws logs tail /aws/lambda/DataPipelineStack-AnalyticsLambda406A5DA6-bAYQfOZ4HHvp --follow
```

## Manual Testing
```bash
# Test data sync manually
aws lambda invoke \
  --function-name DataPipelineStack-DataSyncLambdaE3D45EA6-3aWy3hRd7zG3 \
  --payload '{}' \
  response.json

# Check S3 bucket contents
aws s3 ls s3://rearc-data-quest-650653607934/ --recursive

# Check SQS queue
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-1.amazonaws.com/650653607934/data-pipeline-processing-queue \
  --attribute-names All
```






