# AWS access required

## Airflow runtime identity (writes to S3)

Grant the Airflow runtime identity access only to the project prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListProjectPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET",
      "Condition": {"StringLike": {"s3:prefix": ["YOUR_PREFIX/*"]}}
    },
    {
      "Sid": "WriteProjectObjects",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/*"
    }
  ]
}
```

Add KMS permissions if the bucket uses a customer-managed KMS key instead of SSE-S3.

## Snowflake storage integration role (reads from S3)

The AWS role assumed by Snowflake should have `s3:GetObject` on `YOUR_BUCKET/YOUR_PREFIX/*` and `s3:ListBucket` constrained to `YOUR_PREFIX/*`. Its trust policy must use the AWS IAM user ARN and external ID returned by `DESC INTEGRATION` for the Snowflake storage integration. Do not reuse the Airflow writer credentials for Snowflake.
