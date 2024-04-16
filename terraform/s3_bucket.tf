resource "aws_s3_bucket" "raw_data_bucket" {
    bucket = "raw-data-bucket"  # Replace with your desired bucket name
    acl    = "private"
}

resource "aws_s3_bucket" "cleaned_data_bucket" {
    bucket = "cleaned-data-bucket"  # Replace with your desired bucket name
    acl    = "private"
}