import boto3
import csv

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

BUCKET_NAME = 'ezratl-test-bucket'

CSV_FILE = './experiments.csv'
DATA_PATH = './data/'

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
dyndb = None
table = None

try:
    s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
except Exception:
    pass

try:
    s3.Object(BUCKET_NAME, 'test.png').put(Body=open('/home/mydata/test.jpg', 'rb'))
except FileNotFoundError:
    pass

dyndb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            { 'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
            { 'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
            {'AttributeName': 'RowKey', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception:
    table = dyndb.Table('DataTable')

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

urlbase = "https://s3-us-east-2.amazonaws.com/" + BUCKET_NAME + "/"

try:
    with open(CSV_FILE, 'r') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        for item in csvf:
            body = open(DATA_PATH + item[2], 'rb')
            s3.Object(BUCKET_NAME, item[2]).put(Body=body)
            md = s3.Object(BUCKET_NAME, item[2]).Acl().put(ACL='public-read')
            url = urlbase + item[2]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[3], 'url': url}
            table.put_item(Item=metadata_item)
except Exception:
    print('read csv fail')

print('Querying table...')
response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '5'
    }
)
item = response['Item']
print(item)
