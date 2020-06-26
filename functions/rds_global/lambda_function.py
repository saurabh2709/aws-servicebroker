import boto3
import cfnresponse
import mysql.connector
import random
import socket
import string
import time
import traceback

from awshelper import AwsHelper

alnum = string.ascii_uppercase + string.ascii_lowercase + string.digits

client = boto3.client('ec2')
rds_client = boto3.client('rds')
region = client.meta.region_name

aws = AwsHelper(region)


def create_rdsglobal(notification):
    """
    :return: CustomResourceResponse object (operation status,
                                            physical resource identifier,
                                            additional resource data)
    """
    result = CustomResourceResponse()
    cfn = notification.get("ResourceProperties", {})
    global_properties = cfn.get("GlobalProperties", {})
    cluster_properties = cfn.get("ClusterProperties", {})
    instance_properties = cfn.get("InstanceProperties", {})

    # does the global cluster already exist?
    response = aws.describe_global_clusters(GlobalClusterIdentifier=global_properties["GlobalClusterIdentifier"])
    if len(response["GlobalClusters"]) > 0:
        # yes...this is the secondary region or retry of primary
        print("Global cluster exists (Secondary region or Primary Retry)")
        result.id = response["GlobalClusters"][0]["GlobalClusterIdentifier"]
        result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
        result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
        cluster_properties["SourceRegion"] = cfn.get("SourceRegion", "")
    else:
        # no...this is the primary region
        print("Global cluster does not exist (Primary region) - creating new global database")
        create_response = aws.create_global_cluster(**global_properties)
        result.id = create_response["GlobalCluster"]["GlobalClusterIdentifier"]
        result.data["Arn"] = create_response["GlobalCluster"]["GlobalClusterArn"]
        result.data["ResourceId"] = create_response["GlobalCluster"]["GlobalClusterResourceId"]

    # Pass back parameter as output.
    result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]

    if not result.id:
        # No Global cluster ID.
        result.status = cfnresponse.FAILED
        return result

    cluster_response = aws.describe_db_clusters(DBClusterIdentifier=cluster_properties["DBClusterIdentifier"])
    if len(cluster_response["DBClusters"]) > 0:
        # This is a retry
        result.data["Endpoint"] = cluster_response["DBClusters"][0]["Endpoint"]
        result.data["ReadEndpoint"] = cluster_response["DBClusters"][0]["ReaderEndpoint"]
        result.data["ClusterResourceId"] = cluster_response["DBClusters"][0]["DbClusterResourceId"]
    else:
        result = create_cluster(cluster_properties,
                                instance_properties,
                                result,
                                int(cfn["Replicas"]),
                                cfn["AvailabilityZones"]
                                )

        print("Waiting 7 minutes to let clusters be available")
        time.sleep(420)

    if cluster_properties.get("EnableIAMDatabaseAuthentication", "false") == "true":
        add_db_user(cluster_properties, result, cfn.get("SourceRegion", ""))
    return result


def update_rdsglobal(notification):
    """
    :return: CustomResourceResponse object (operation status,
                                            physical resource identifier,
                                            additional resource data)
    """
    result = CustomResourceResponse()
    result.id = notification.get("PhysicalResourceId")
    cfn = notification.get("ResourceProperties", {})
    global_properties = cfn.get("GlobalProperties", {})

    # The following can be modified on the DB cluster:
    cluster_keys = ["DBClusterIdentifier", "EngineVersion", "PreferredMaintenanceWindow", "BackupRetentionPeriod"]
    passed_cluster_properties = cfn.get("ClusterProperties", {})
    cluster_properties = {key: passed_cluster_properties.get(key, "") for key in cluster_keys}
    cluster_properties["AllowMajorVersionUpgrade"] = True

    # For some reason the engine version cannot be passed if it is unchanged. No other parameters have this behavior.
    current_db_cluster = aws.describe_db_clusters(DBClusterIdentifier=cluster_properties["DBClusterIdentifier"])
    if current_db_cluster["DBClusters"][0]["EngineVersion"] == cluster_properties["EngineVersion"]:
        del cluster_properties["EngineVersion"]

    # Only AutoMinorVersionUpgrade can be modified on the instance.
    passed_instance_properties = cfn.get("InstanceProperties", {})
    instance_properties = {"AutoMinorVersionUpgrade": passed_instance_properties["AutoMinorVersionUpgrade"]}

    # Fetch return values from global cluster. These cannot be modified.
    response = aws.describe_global_clusters(GlobalClusterIdentifier=global_properties["GlobalClusterIdentifier"])
    result.id = response["GlobalClusters"][0]["GlobalClusterIdentifier"]
    result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
    result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
    result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]

    print("Modifying DB cluster and instances")
    print("DB cluster parameters: {}".format(cluster_properties))
    try:
        cluster_response = aws.modify_db_cluster(**cluster_properties)
        aws.add_tags_to_resource(ResourceName=current_db_cluster["DBClusters"][0]["DBClusterArn"],
                                  Tags=passed_cluster_properties["Tags"])
    except Exception as e:
        print("Cluster Update Failed: {}".format(e))
        result.status = cfnresponse.FAILED
        return result

    result.data["Endpoint"] = cluster_response["DBCluster"]["Endpoint"]
    result.data["ReadEndpoint"] = cluster_response["DBCluster"]["ReaderEndpoint"]
    result.data["ClusterResourceId"] = cluster_response["DBCluster"]["DbClusterResourceId"]
    print("DB instance parameters: {0}".format(instance_properties))
    db_instances = cluster_response["DBCluster"]["DBClusterMembers"]
    for db_instance in db_instances:
        instance_properties["DBInstanceIdentifier"] = db_instance["DBInstanceIdentifier"]
        instance_arn = aws.get_db_instance_arn(DBInstanceIdentifier=db_instance["DBInstanceIdentifier"])
        try:
            aws.modify_db_instance(**instance_properties)
            aws.add_tags_to_resource(ResourceName=instance_arn, Tags=passed_cluster_properties["Tags"])
        except Exception as e:
            print("Instance Update Failed: {}".format(e))
            result.status = cfnresponse.FAILED
            return result

    if passed_cluster_properties.get("EnableIAMDatabaseAuthentication", "false") == "true":
        add_db_user(passed_cluster_properties, result, cfn.get("SourceRegion", ""))

    result.status = cfnresponse.SUCCESS
    return result


def delete_rdsglobal(notification):
    """
    :param notification: notification request
    :return: CustomResourceResponse object (operation status,
                                            physical resource identifier,
                                            additional resource data)
    """
    result = CustomResourceResponse()
    result.id = notification.get("PhysicalResourceId")
    cfn = notification.get("ResourceProperties", {})
    cluster_properties = cfn.get("ClusterProperties", {})

    if cfn["SourceRegion"] == region:
        # delete the global cluster
        delete_global_cluster(result.id)
    else:
        # remove the secondary db cluster
        arn = aws.get_db_cluster_arn(DBClusterIdentifier=cluster_properties["DBClusterIdentifier"])
        response = aws.remove_from_global_cluster(GlobalClusterIdentifier=result.id, DbClusterIdentifier=arn)
        if "GlobalClusterIdentifier" in response["GlobalCluster"]:
            print("'%s' has been removed from '%s'" % (cluster_properties["DBClusterIdentifier"],
                                                       response["GlobalCluster"]["GlobalClusterIdentifier"]))
            # wait for the db cluster to be promoted to standalone
            print("Waiting 2 minutes before deleting old DB cluster")
            time.sleep(120)

    # now delete the leftover db cluster
    result.status = delete_db_cluster(cluster_properties)
    return result


def delete_global_cluster(id):
    """
    Delete a global cluster, with all its trimmings

    :param id: global cluster identifier
    :return: None
    """
    # first, remove any members
    clusters = aws.describe_global_clusters(GlobalClusterIdentifier=id)["GlobalClusters"]
    for cluster in clusters:
        for member in cluster["GlobalClusterMembers"]:
            aws.remove_from_global_cluster(GlobalClusterIdentifier=id,
                                           DbClusterIdentifier=member["DBClusterArn"])
    # now we can delete him
    aws.delete_global_cluster(GlobalClusterIdentifier=id)


def create_cluster(cluster_properties, instance_properties, result, num_replicas, azs):
    """
    Create an RDS cluster

    :param cluster_properties: create_db_cluster() arguments
    :param instance_properties: create_db_instance() arguments
    :param result: CustomResourceResult
    :param num_replicas: Number of DB replicas
    :param azs: AZs to launch instances within
    :return: custom resource notification status (cfnresponse.SUCCESS or cfnresponse.FAILED)
    """
    # this property is only used for deletions
    cluster_properties.pop("SkipFinalSnapshotOnDeletion", None)

    # create db cluster
    print("Creating DB cluster and instances")
    print("DB cluster parameters: {}, replicas: {}".format(cluster_properties, num_replicas))
    try:
        cluster_response = aws.create_db_cluster(**cluster_properties)
    except Exception as e:
        print("Cluster Creation Failed: {}".format(e))
        result.status = cfnresponse.FAILED
        return result
    result.data["Endpoint"] = cluster_response["DBCluster"]["Endpoint"]
    result.data["ReadEndpoint"] = cluster_response["DBCluster"]["ReaderEndpoint"]
    result.data["ClusterResourceId"] = cluster_response["DBCluster"]["DbClusterResourceId"]

    # create db instances
    print("DB instance parameters: {0}".format(instance_properties))
    instance_identifier = instance_properties["DBClusterIdentifier"]
    for i in range(num_replicas):
        instance_properties["DBInstanceIdentifier"] = instance_identifier + str(i)
        instance_properties["AvailabilityZone"] = azs[i]
        try:
            aws.create_db_instance(**instance_properties)
        except Exception as e:
            print("Instance Creation Failed: {}".format(e))
            result.status = cfnresponse.FAILED
            return result
    result.status = cfnresponse.SUCCESS
    return result


def delete_db_cluster(cluster_properties):
    """
    Delete an RDS cluster

    :param cluster_properties: create_db_cluster() arguments
    :return: custom resource notification status (always cfnresponse.SUCCESS)
    """
    id = cluster_properties["DBClusterIdentifier"]
    skip = cluster_properties["SkipFinalSnapshotOnDeletion"]
    clusters = aws.describe_db_clusters(DBClusterIdentifier=id)["DBClusters"]
    if len(clusters) > 0:
        members = clusters[0]["DBClusterMembers"]
        try:
            writers = []
            for instance in members:
                if instance["IsClusterWriter"]:
                    # writer instance will be deleted at the end
                    writers.append(instance["DBInstanceIdentifier"])
                else:
                    # delete reader instances
                    aws.delete_db_instance(DBInstanceIdentifier=instance["DBInstanceIdentifier"])
                    print("Deleted %s" % instance["DBInstanceIdentifier"])
            # now delete writer instances
            for writer in writers:
                aws.delete_db_instance(DBInstanceIdentifier=writer)
                print("Deleted %s" % writer)
        except Exception:
            print("Could not delete DB instance from cluster %s" % id)

        # now we can delete the cluster
        aws.delete_db_cluster(DBClusterIdentifier=id, SkipFinalSnapshot=skip)
    else:
        print("Could not find DB cluster %s" % id)
    return cfnresponse.SUCCESS


def add_db_user(cluster_properties, result, db_region):
    # Required for User ARN.
    account = boto3.client('sts').get_caller_identity().get('Account')
    cluster_id = result.data["ClusterResourceId"]
    users = {
            'admin': 'GRANT ALL ON `%`.* TO {} REQUIRE SSL',
            'application': 'GRANT SELECT, INSERT, UPDATE, DELETE ON `%`.* TO {} REQUIRE SSL',
            'reader': 'GRANT SELECT ON `%`.* TO {} REQUIRE SSL',
            }

    if db_region and region != db_region:
        for username in users.keys():
            if username == "reader":
                output_user = "ReadUser"
            else:
                output_user = username.capitalize() + "User"
            result.data[output_user] = "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)
            print("User {}: {}".format(output_user, result.data[output_user]))
        return

    master_user = cluster_properties["MasterUsername"]
    master_password = cluster_properties["MasterUserPassword"]
    endpoint = result.data["Endpoint"]

    # wait for domain name to propagate
    wait_domain_name(endpoint)
    print("endpoint: {}, id: {}".format(endpoint, cluster_id))
    # master user does not have super privileges, cannot grant 'ALL PRIVILEGES ON *.*'.
    rdsdb = mysql.connector.connect(host=endpoint, user=master_user, password=master_password)
    cursor = rdsdb.cursor(buffered=True)
    for username, grant in users.items():
        if username == "reader":
            output_user = "ReadUser"
        else:
            output_user = username.capitalize() + "User"
        if not user_exists(cursor, username):
            statement = """CREATE USER {} IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';""".format(username)
            print(statement)
            cursor.execute(statement)
            if not user_exists(cursor, username):
                raise Exception("Unable to create User '{}'.".format(username))
            grant_statement = grant.format(username)
            print(grant_statement)
            cursor.execute(grant_statement)

        result.data[output_user] = "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)
        print(result.data[output_user])

    rdsdb.close()
    return


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
    response = CustomResourceResponse()
    try:
        if event['RequestType'] == 'Create':
            try:
                response = create_rdsglobal(event)
                print("Response Data: {}", response.data)
            except Exception as e:
                print(str(e))
                traceback.print_exc()
                phys_id = ''.join(random.choice(alnum) for _ in range(16))
                cfnresponse.send(event, context, cfnresponse.FAILED, response.data, phys_id, str(e))

        # Failover through dexbuilder.
        elif event['RequestType'] == 'Update':
            response = update_rdsglobal(event)
        elif event['RequestType'] == 'Delete':
            try:
                response = delete_rdsglobal(event)
            except Exception as e:
                print(str(e))
                traceback.print_exc()
                phys_id = event['PhysicalResourceId']
                cfnresponse.send(event, context, cfnresponse.FAILED, response.data, phys_id, str(e))
        print("Response Data: {}", response.data)
        cfnresponse.send(event, context, response.status, response.data, response.id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response.data, response.id, str(e))


class CustomResourceResponse(object):

    """ Custom resource notification response """

    def __init__(self):
        self.status = cfnresponse.FAILED
        self.id = None
        self.data = {}
