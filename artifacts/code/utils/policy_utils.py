class POLICY_UTILS():

    def join_permissions(*permission_lists):
        unique_permissions = set()
        for permissions in permission_lists:
            unique_permissions.update(permissions)
        return list(unique_permissions)

    APPFLOW_READ_WRITE_PERMISSIONS = [
        "appflow:TagResource",
        "appflow:DescribeFlow",
        "appflow:DescribeFlowExecutionRecords",
        "appflow:ListFlows",
        "appflow:ListTagsForResource",
        "appflow:CreateFlow",
        "appflow:UpdateFlow",
        "appflow:DeleteFlow",
        "appflow:StartFlow",
        "appflow:UseConnectorProfile",
        "appflow:StopFlow"
    ]

    KMS_FULL_PERMISSIONS = [
        "kms:*"
    ]

    BUCKET_LIST_S3_PERMISSIONS = [
        "s3:ListBucket",
        "s3:ListAllBucket",
        "s3:ListBucketMultipartUploads",
        "s3:AbortMultipartUpload",
        "s3:ListAllMyBuckets",
    ]

    BUCKET_READ_WRITE_S3_PERMISSIONS = [
        "s3:GetBucketAcl",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketPolicy",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket" 
    ]

    S3_READ_PERMISSIONS = [   
        "s3:ListBucket",
        "s3:GetObject",
        "s3:GetBucketPolicy",
        "s3:GetBucketLocation",
        "s3:ListAllMyBuckets",
        "s3:GetBucketAcl"
    ]

    S3_WRITE_PERMISSIONS = [
        "s3:PutObject",
        "s3:ReplicateObject",
        "s3:DeleteObject"
    ]

    DYNAMO_READ_PERMISSIONS = [
        "dynamodb:Scan",
        "dynamodb:Query",
        "dynamodb:GetItem"
    ]

    DYNAMO_WRITE_PERMISSIONS = [
        "dynamodb:UpdateItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem"
    ]

    GLUE_CATALOG_PERMISSIONS = [
        "glue:CreateTable",
        "glue:DeleteTable",
        "glue:GetDatabase",
        "glue:UpdateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition",
        "glue:BatchUpdatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "logs:PutLogEvents",
        "glue:UpdatePartition",
        "glue:UpdatePartitions",
        "glue:BatchGetPartition"
    ]

    LAMBDA_CATALOG_PERMISSIONS = [
        "glue:GetDatabase",
        "glue:CreateDatabase",
        "glue:GetCrawler",
        "glue:CreateCrawler",
        "glue:UpdateCrawler",
        "glue:StartCrawler",
        "iam:PassRole",
        "lakeformation:GetLFTag",
        "lakeformation:AddLFTagsToResource",
        "lakeformation:GetDataAccess",
        "lakeformation:GrantPermissions"
    ]

    PUT_METRICS_PERMISSIONS = [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics"
    ]

    GET_LOG_DETAILS_PERMISSIONS = [
        "glue:GetJobRuns"
    ]

    SNS_PUBLIS_PERMISSIONS = [
        "sns:Publish"
    ]

    LOGS_PERMISSIONS = [
        "logs:*"
    ]

    COST_EXPLORER_PERMISSIONS = [
        "ce:GetCostAndUsage"
    ]

    START_STEP_FUNCTION_PERMISSIONS = [
        "states:StartExecution"
    ]

    REDSHIFT_DATA_PERMISSIONS = [
        "redshift-data:ExecuteStatement",
        "redshift:GetClusterCredentials",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult"
    ]

