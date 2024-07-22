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
  name   = "lambda_logging_policy"
  role   = aws_iam_role.s3_lambda_trigger_role.id

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
}

resource "aws_s3_bucket" "raw" {
  bucket = "nifty50-raw"

  tags = {
    Project = "data-engineering"
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_raw_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = "_board_meetings.csv"
  }

  depends_on = [aws_lambda_permission.allow_raw_bucket]
}

# resource "aws_s3_bucket" "processed" {
#   bucket = "nifty50-processed"

#   tags = {
#     Project = "data-engineering"
#   }
# }

# resource "aws_s3_bucket" "analytics" {
#   bucket = "nifty50-analytics"

#   tags = {
#     Project = "data-engineering"
#   }
# }