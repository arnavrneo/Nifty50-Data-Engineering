locals {
  object_source = "../lambda_layer.zip"
}

resource "aws_s3_bucket" "my_lambda_layers_arneo" {
  bucket = "my-lambda-layers-arneo"

  tags = {
    env = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "my_lambda_layers_arneo" {
  bucket = aws_s3_bucket.my_lambda_layers_arneo.id

  block_public_acls   = false
  block_public_policy = false
}

resource "aws_s3_object" "layer_upload" {
  bucket = aws_s3_bucket.my_lambda_layers_arneo.id
  key    = "lambda_layer.zip"
  source = local.object_source
}