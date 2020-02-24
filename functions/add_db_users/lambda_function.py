import boto3
import cfnresponse
import mysql.connector
import random
import socket
import string
import time
import traceback

alnum = string.ascii_uppercase + string.ascii_lowercase + string.digits
client = boto3.client('ec2')


def add_db_user(admin_user, admin_password, endpoint, cluster_id):
    # wait for domain name to propagate
    wait_domain_name(endpoint)
    print("endpoint: {}, id: {}".format(endpoint, cluster_id))
    username = 'dev'
    rdsdb = mysql.connector.connect(host=endpoint, user=admin_user, password=admin_password)
    cursor = rdsdb.cursor(buffered=True)
    if not user_exists(cursor, username):
        statement = """CREATE USER {} IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';""".format(username)
        print(statement)
        cursor.execute(statement)
        if not user_exists(cursor, username):
            raise Exception("Unable to create User '{}'.".format(username))

    # Construct user arn.
    region = client.meta.region_name
    account = boto3.client('sts').get_caller_identity().get('Account')
    rdsdb.close()
    return "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)


def user_exists(cursor, username):
    # Verify user was created.
    statement = """SELECT User FROM mysql.user WHERE User ='{}'""".format(username)
    print(statement)
    cursor.execute(statement)
    print(cursor.rowcount)
    if cursor.rowcount < 1:
        return False
    return True


def wait_domain_name(hostname):
    period = 30
    attempts = 4
    for attempt in range(0, attempts+1):
        try:
            socket.gethostbyname(hostname)
        except socket.gaierror as err:
            print("domain resolution error: {}".format(err))
        else:
            return
        time.sleep(period)
    raise Exception("Domain name did not resolve in {} seconds".format(period*attempts))


def handler(event, context):
    response_code = cfnresponse.SUCCESS
    response_data = {}
    if event['RequestType'] == 'Create':
        phys_id = ''.join(random.choice(alnum) for _ in range(16))
    else:
        phys_id = event['PhysicalResourceId']
    try:
        if event['RequestType'] in ['Create', 'Update']:
            response_data['User'] = add_db_user(
                event['ResourceProperties']['DBUsername'],
                event['ResourceProperties']['DBPassword'],
                event['ResourceProperties']['ClusterEndpoint'],
                event['ResourceProperties']['DBName']
            )
        cfnresponse.send(event, context, response_code, response_data, phys_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data, phys_id, str(e))
