locals {
    lambda_function_path = "../lambda_function.zip"
}

resource "aws_lambda_layer_version" "stock_de_lambda_layer" {
  layer_name = "stockde_lambda_layer"
  description = "Lambda Layer for Stock DE Project"
  s3_bucket = aws_s3_bucket.my_lambda_layers_arneo.id
  s3_key = aws_s3_object.layer_upload.key

  compatible_runtimes = ["python3.10"]
}

resource "aws_lambda_function" "stock_de_lambda" {
    function_name = "Stock_DE_script_lambda"
    handler = "lambda_function.lambda_handler"
    filename = local.lambda_function_path
    role = "arn:aws:iam::211125565587:role/StockDELambdaAccessFullAccessS3" # Already created in AWS 
    runtime = "python3.10"
    layers = [
        aws_lambda_layer_version.stock_de_lambda_layer.arn
    ]
  
}