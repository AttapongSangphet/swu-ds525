import boto3

aws_access_key_id = "aws_access_key"
aws_secret_access_key = "aws_secret_access_key"
aws_session_token = "aws_session_token"

# client = boto3.client(
#     's3',
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key,
#     aws_session_token=aws_session_token
# )
# print(client)

# response = client.list_objects(
#     Bucket='tands525',
#     MaxKeys=2,
# )
# contents = response["Contents"]
# for content in contents:
#     print(content["Key"], content["Size"])

s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)
s3.meta.client.upload_file(
    "file_name",
    "bucket",
    "object_name",
)