import boto3

aws_access_key_id = "ASIA255IGVKUMQXAHV5J"
aws_secret_access_key = "Knxbhfc8myuUxFWehPed/E+PXGE1ZpZcTke66U5E"
aws_session_token = "FwoGZXIvYXdzEBsaDHia1WQKIZUX4lk7lyLOAUPpsXhutoUSEGrAafrDWnfXSBc59yXyZmCToJHEMCcuyfh5qyneL/4+ZmBQtFD5/KLXd3F6J3dJElqU/4+3aUP0s3tiXGwMmYg3j64KvsxQGVmFx3FqrmqVWIh1n7LC98JTBtTedqoqRuKBkzHPesGc2W78dw1Ikkz01eSZWbGcm6mlbKOW5FXTxU23HRMlop0oJgAWEX2sD3tmUa9FfVs8U/MwmDypocsCf3Vo7R9iWNvIiAt0P7T4B+6668VAj5qvGvRVuuKFhVVYN2YGKPjC+5wGMi3K7pPsghchdIlCqNMaPG3fc+TrnxH5FUlnKbylPbmPMz++Rw/mx+M9qi+CahM="

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
    "/workspace/swu-ds525/capstone-project/data/price_paid_records01.csv",
    "tands525",
    "price_paid_records01.csv",
)