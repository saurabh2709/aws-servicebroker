import boto3
import cfnresponse
import random
import string
import traceback

from botocore.exceptions import ClientError

alnum = string.ascii_uppercase + string.ascii_lowercase + string.digits


def create_global_cluster(global_cluster_id, source_cluster_arn):
    client = boto3.client('rds')
    print("Global Cluster: {}, Source Cluster: {}".format(global_cluster_id, source_cluster_arn))
    # Check if cluster already exists.
    try:
        global_clusters = client.describe_global_clusters(GlobalClusterIdentifier=global_cluster_id)
        # Return the arn of the source cluster for replicas to use.
        for member in global_clusters['GlobalClusters'][0]['GlobalClusterMembers']:
            if member['IsWriter']:
                return member['DBClusterArn']
    except ClientError as err:
        if err.response['Error']['Code'] == 'GlobalClusterNotFoundFault':
            if source_cluster_arn == "":
                raise Exception(
                        "Global Cluster {} does not exist and no source cluster was passed".format(global_cluster_id)
                        )
            else:
                # Create the global cluster if it does not exist and we are in the primary region.
                global_cluster = client.create_global_cluster(
                        GlobalClusterIdentifier=global_cluster_id,
                        SourceDBClusterIdentifier=source_cluster_arn
                        )
                # Return the arn of the source cluster for replicas to use.
                for member in global_cluster['GlobalCluster']['GlobalClusterMembers']:
                    if member['IsWriter']:
                        return member['DBClusterArn']
        else:
            print(err.response['Error'])
            raise


def get_kms_key(cluster_arn):
    # Ex arn:aws:rds:us-east-1:123456789012:cluster:dev-service, region=us-east-1
    region = cluster_arn.split(':')[3]
    client = boto3.client('rds', region)
    cluster = client.describe_db_clusters(Filters=[{'Name': 'db-cluster-id', 'Values': [cluster_arn]}])
    kms_key_id = cluster['DBClusters'][0].get('KmsKeyId', '')
    print("KMS Key {}".format(kms_key_id))
    return kms_key_id


def handler(event, context):
    response_code = cfnresponse.SUCCESS
    response_data = {}
    if event['RequestType'] == 'Create':
        phys_id = ''.join(random.choice(alnum) for _ in range(16))
    else:
        phys_id = event['PhysicalResourceId']
    try:
        if event['RequestType'] in ['Create', 'Update']:
            response_data['SourceCluster'] = create_global_cluster(
                event['ResourceProperties']['GlobalClusterIdentifier'],
                event['ResourceProperties'].get('DBClusterArn', ''),
            )
            response_data['KmsKeyId'] = get_kms_key(response_data['SourceCluster'])
        cfnresponse.send(event, context, response_code, response_data, phys_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data, phys_id, str(e))
