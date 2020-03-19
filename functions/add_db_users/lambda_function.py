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
rds_client = boto3.client('rds')


def add_db_user(master_user, master_password, endpoint, db_region, cluster_id):
    # Required for User ARN.
    region = client.meta.region_name
    account = boto3.client('sts').get_caller_identity().get('Account')
    rds_users = {}
    users = {
            'admin': 'GRANT ALL ON `%`.* TO {} REQUIRE SSL',
            'application': 'GRANT SELECT, INSERT, UPDATE, DELETE ON `%`.* TO {} REQUIRE SSL',
            'reader': 'GRANT SELECT, INSERT, UPDATE, DELETE ON `%`.* TO {} REQUIRE SSL',
            }

    if region != db_region:
        for username in users.keys():
            rds_users[username] = "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)
            print(rds_users[username])
        return rds_users

    # wait for domain name to propagate
    wait_domain_name(endpoint)
    print("endpoint: {}, id: {}".format(endpoint, cluster_id))
    # master user does not have super privileges, cannot grant 'ALL PRIVILEGES ON *.*'.
    rdsdb = mysql.connector.connect(host=endpoint, user=master_user, password=master_password)
    cursor = rdsdb.cursor(buffered=True)
    for username, grant in users.items():
        if not user_exists(cursor, username):
            statement = """CREATE USER {} IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';""".format(username)
            print(statement)
            cursor.execute(statement)
            if not user_exists(cursor, username):
                raise Exception("Unable to create User '{}'.".format(username))
            grant_statement = grant.format(username)
            print(grant_statement)
            cursor.execute(grant_statement)

        rds_users[username] = "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)
        print(rds_users[username])

    rdsdb.close()
    return rds_users


def user_exists(cursor, username):
    # Verify user was created.
    statement = """SELECT User FROM mysql.user WHERE User ='{}'""".format(username)
    print(statement)
    cursor.execute(statement)
    print(cursor.rowcount)
    if cursor.rowcount < 1:
        return False
    return True


# Make a nice list of federated users with access to the DB User.
def format_iam_users(account, ldap_users):
    return ["arn:aws:sts::{}:federated-user/{}".format(account, ldap_user) for ldap_user in ldap_users.split(',') if ldap_users]


def get_cluster_resource_id(cluster):
    clusters = rds_client.describe_db_clusters(DBClusterIdentifier=cluster)
    return clusters['DBClusters'][0]['DbClusterResourceId']


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
            resource_id = get_cluster_resource_id(event['ResourceProperties']['Cluster'])
            rds_users = add_db_user(
                event['ResourceProperties']['DBUsername'],
                event['ResourceProperties']['DBPassword'],
                event['ResourceProperties']['ClusterEndpoint'],
                event['ResourceProperties']['PrimaryRegion'],
                resource_id
            )
            response_data['ApplicationUser'] = rds_users['application']
            response_data['AdminUser'] = rds_users['admin']
            response_data['ReadUser'] = rds_users['reader']
            response_data['AdminUsers'] = format_iam_users(
                event['ResourceProperties']['Account'],
                event['ResourceProperties']['AdminUsers']
            )
            response_data['ReadUsers'] = format_iam_users(
                event['ResourceProperties']['Account'],
                event['ResourceProperties']['ReadUsers']
            )
        cfnresponse.send(event, context, response_code, response_data, phys_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data, phys_id, str(e))
