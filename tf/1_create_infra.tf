# Raw bucket
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "s3_lambda_trigger_role" {
  name               = "s3_lambda_trigger_role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


resource "aws_lambda_permission" "allow_raw_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_raw_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_iam_role_policy" "lambda_logging_policy" {
  name = "lambda_logging_policy"
  role = aws_iam_role.s3_lambda_trigger_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}


resource "aws_lambda_function" "s3_raw_lambda" {
  filename      = "../s3_triggers/s3_raw_lambda.zip"
  function_name = "s3_raw_lambda"
  role          = aws_iam_role.s3_lambda_trigger_role.arn
  handler       = "s3_raw_lambda.lambda_handler"
  runtime       = "python3.12"
  timeout       = "120"
  
  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12"
  ]
}

resource "aws_s3_bucket" "raw" {
  bucket = "nifty50-raw-1"

  tags = {
    Project = "data-engineering"
  }
}

resource "aws_s3_bucket_notification" "raw_bucket_notification" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_board_meetings.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_bulk_deals_archives.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_monthly_adv_declines.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_nse_ratios.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_short_selling_archives.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_sec_archives.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_block_deals_archives.csv"
  }

  depends_on = [aws_lambda_permission.allow_raw_bucket]
}

# Processed bucket

resource "aws_lambda_function" "s3_processed_lambda" {
  filename      = "../s3_triggers/s3_processed_lambda.zip"
  function_name = "s3_processed_lambda"
  role          = aws_iam_role.s3_lambda_trigger_role.arn
  handler       = "s3_processed_lambda.lambda_handler"
  runtime       = "python3.12"
  timeout       = "120"

  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12"
  ]
}

resource "aws_lambda_permission" "allow_processed_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_processed_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.processed.arn
}

resource "aws_s3_bucket" "processed" {
  bucket = "nifty50-processed-1"

  tags = {
    Project = "data-engineering"
  }
}

resource "aws_s3_bucket_notification" "processed_bucket_notification" {
  bucket = aws_s3_bucket.processed.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_block_deals_archives_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_board_meetings_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_bulk_deals_archives_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_monthly_adv_declines_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_nse_ratios_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_sec_archives_processed.csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_short_selling_archives_processed.csv"
  }

  depends_on = [aws_lambda_permission.allow_processed_bucket]
}


# Analytics bucket
resource "aws_s3_bucket" "analytics" {
  bucket = "nifty50-analytics-1"

  tags = {
    Project = "data-engineering"
  }
}

# IAM USER: arn:aws:iam::819481466467:user/e3t51000-s
# IAM ROLE" arn:aws:iam::853463361791:role/snowflake-s3-access

# Uncomment the following after creating snowflake SQS pipe
# Replace the queue arn with the Snowflake SQS pipe arn 
resource "aws_s3_bucket_notification" "analytics_bucket_notification" {
  bucket = aws_s3_bucket.analytics.id

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_block_deals_archives_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_board_meetings_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_bulk_deals_archives_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_monthly_adv_declines_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_nse_ratios_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_sec_archives_analytics.csv"
  }

  queue {
    queue_arn = "arn:aws:sqs:us-east-1:819481466467:sf-snowpipe-AIDA35THG2ZRTUQA4IHCD-nSKT9gHPAs8K7g9aehjfoA" # replace here
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_short_selling_archives_analytics.csv"
  }

  depends_on = [aws_s3_bucket.analytics]
}
