import boto3
import cfnresponse
import logging
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

# our logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


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
        logger.info("Global cluster exists (Secondary region or Primary Retry)")
        result.id = response["GlobalClusters"][0]["GlobalClusterIdentifier"]
        result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
        result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
        cluster_properties["SourceRegion"] = cfn.get("SourceRegion", "")
    else:
        # no...this is the primary region
        logger.info("Global cluster does not exist (Primary region) - creating new global database")
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

        logger.info("Waiting 9 minutes to let clusters be available")
        time.sleep(540)

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
    cluster_properties = cfn.get("ClusterProperties", {})
    instance_properties = cfn.get("InstanceProperties", {})

    if cfn["Mode"] in ["readonly", ""]:
        # nothing to update...just generate some return values
        response = aws.describe_global_clusters(GlobalClusterIdentifier=global_properties["GlobalClusterIdentifier"])
        source_region = get_source_region(response)
        result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
        result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
        result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]
        get_db_cluster_data(cluster_properties, source_region, result)
        result.status = "SUCCESS"
        return result
    if cfn["Mode"] == "failover":
        # promote replica cluster to standalone
        # deleting old global cluster
        logger.info("Deleting old global cluster '%s'" % global_properties["GlobalClusterIdentifier"])
        delete_global_cluster(global_properties["GlobalClusterIdentifier"])

        # add/remove properties needed to create new global cluster
        db_cluster_arn = aws.get_db_cluster_arn(DBClusterIdentifier=cluster_properties["DBClusterIdentifier"])
        get_db_cluster_data(cluster_properties, cfn.get("SourceRegion", ""), result)
        global_properties["SourceDBClusterIdentifier"] = db_cluster_arn
        global_properties.pop("Engine", None)
        global_properties.pop("EngineVersion", None)
        global_properties.pop("StorageEncrypted", None)

        # create a new global cluster with master in the failover region
        response = aws.create_global_cluster(**global_properties)
        result.id = response["GlobalCluster"].get("GlobalClusterIdentifier")
        if result.id:
            result.data["Arn"] = response["GlobalCluster"]["GlobalClusterArn"]
            result.data["ResourceId"] = response["GlobalCluster"]["GlobalClusterResourceId"]
            result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]
            result.status = "SUCCESS"
        else:
            result.status = "FAILED"
        return result
    if cfn["Mode"] == "postfailover":

        # delete old DB cluster
        logger.info("Deleting old db clusters and instances")
        delete_db_cluster(cluster_properties)

        # wait until the db cluster has been deleted
        id = cluster_properties["DBClusterIdentifier"]
        wait = 20.
        wait_max = 500.
        while wait < wait_max:
            clusters = aws.describe_db_clusters(DBClusterIdentifier=id)["DBClusters"]
            if len(clusters) > 0:
                logger.info("Waiting for %s to be deleted" % id)
                time.sleep(wait)
                wait = wait * 2.
            else:
                wait = wait_max

        # create new db cluster and instances
        del cluster_properties["DatabaseName"]
        del cluster_properties["MasterUsername"]
        del cluster_properties["MasterUserPassword"]
        response = aws.describe_global_clusters(GlobalClusterIdentifier=global_properties["GlobalClusterIdentifier"])
        source_region = get_source_region(response)
        cluster_properties["SourceRegion"] = source_region
        result = create_cluster(cluster_properties,
                                instance_properties,
                                result,
                                int(cfn["Replicas"]),
                                cfn["AvailabilityZones"]
                                )

        # generate some return values
        result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
        result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
        result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]
        if cluster_properties.get("EnableIAMDatabaseAuthentication", "false") == "true":
            add_db_user(cluster_properties, result, source_region)
        return result
    return modeless_update(notification, result)


def modeless_update(notification, result):
    """
    :return: CustomResourceResponse object (operation status,
                                            physical resource identifier,
                                            additional resource data)
    """
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
    instance_properties = {"AutoMinorVersionUpgrade": passed_instance_properties["AutoMinorVersionUpgrade"],
                           "DBInstanceClass": passed_instance_properties["DBInstanceClass"]}

    # Fetch return values from global cluster. These cannot be modified.
    response = aws.describe_global_clusters(GlobalClusterIdentifier=global_properties["GlobalClusterIdentifier"])
    result.id = response["GlobalClusters"][0]["GlobalClusterIdentifier"]
    result.data["Arn"] = response["GlobalClusters"][0]["GlobalClusterArn"]
    result.data["ResourceId"] = response["GlobalClusters"][0]["GlobalClusterResourceId"]
    result.data["DBClusterIdentifier"] = cluster_properties["DBClusterIdentifier"]

    logger.info("Modifying DB cluster and instances")
    logger.info("DB cluster parameters: {}".format(cluster_properties))
    try:
        cluster_response = aws.modify_db_cluster(**cluster_properties)
        aws.add_tags_to_resource(ResourceName=current_db_cluster["DBClusters"][0]["DBClusterArn"],
                                 Tags=passed_cluster_properties["Tags"])
    except (rds_client.exceptions.InvalidDBClusterStateFault, rds_client.exceptions.InvalidDBInstanceStateFault) as e:
        # Do not break Service Instance for this, update can happen later.
        logger.warn("Cluster Not Ready for Update: {}".format(e))
        result.status = cfnresponse.SUCCESS
        return result

    except Exception as e:
        logger.error("Cluster Update Failed: {}".format(e))
        result.status = cfnresponse.FAILED
        return result

    result.data["Endpoint"] = cluster_response["DBCluster"]["Endpoint"]
    result.data["ReadEndpoint"] = cluster_response["DBCluster"]["ReaderEndpoint"]
    result.data["ClusterResourceId"] = cluster_response["DBCluster"]["DbClusterResourceId"]
    logger.info("DB instance parameters: {0}".format(instance_properties))
    db_instances = cluster_response["DBCluster"]["DBClusterMembers"]
    deleted_instances = modify_replica_count(notification, cluster_response)
    for db_instance in db_instances:
        if db_instance["DBInstanceIdentifier"] in deleted_instances:
            continue
        instance_properties["DBInstanceIdentifier"] = db_instance["DBInstanceIdentifier"]
        instance_arn = aws.get_db_instance_arn(DBInstanceIdentifier=db_instance["DBInstanceIdentifier"])
        try:
            aws.modify_db_instance(**instance_properties)
            aws.add_tags_to_resource(ResourceName=instance_arn, Tags=passed_cluster_properties.get("Tags"))
        except rds_client.exceptions.InvalidDBInstanceStateFault as e:
            # Do not break Service Instance for this, update can happen later.
            logger.warn("Instance Not Ready for Update: {}".format(e))
            result.status = cfnresponse.SUCCESS
            return result
        except Exception as e:
            logger.error("Instance Update Failed: {}".format(e))
            result.status = cfnresponse.FAILED
            return result

    if passed_cluster_properties.get("EnableIAMDatabaseAuthentication", "false") == "true":
        add_db_user(passed_cluster_properties, result, cfn.get("SourceRegion", ""), True)

    result.status = cfnresponse.SUCCESS
    return result


def modify_replica_count(notification, cluster_response):
    """
    :return: list of deleted replicas
    """
    writer = "unknown"
    db_instances = cluster_response["DBCluster"]["DBClusterMembers"]
    instance_properties = notification["ResourceProperties"]["InstanceProperties"]
    deleted_replicas = []
    # Current instance count is the writer plus the number of replicas.
    current_replica_count = len(db_instances) - 1

    # Dictionary to determine which instance to delete or which az to add instances to.
    az_instances = {az: [] for az in cluster_response["DBCluster"]["AvailabilityZones"]}
    for db_instance in db_instances:
        if db_instance["IsClusterWriter"]:
            writer = db_instance["DBInstanceIdentifier"]
        az = aws.get_db_instance_az(DBInstanceIdentifier=db_instance["DBInstanceIdentifier"])
        az_instances[az].append(db_instance["DBInstanceIdentifier"])

    desired_num_replicas = notification["ResourceProperties"]["Replicas"]
    logger.info("DB writer {0}".format(writer))
    logger.info("Instance Properties: {}".format(instance_properties))
    difference = int(desired_num_replicas) - current_replica_count
    # Add replicas
    while difference > 0:
        least_populated_az = least_populated(az_instances)
        available_id = find_available_id(az_instances, instance_properties["DBClusterIdentifier"])
        instance_properties["DBInstanceIdentifier"] = available_id
        instance_properties["AvailabilityZone"] = least_populated_az
        logger.info("Adding replica {0}".format(available_id))
        aws.create_db_instance(**instance_properties)
        az_instances[least_populated_az].append(available_id)
        difference -= 1
    # Delete replicas
    while difference < 0:
        most_populated_az = most_populated(az_instances)
        instance = az_instances[most_populated_az].pop()
        if instance == writer:
            az_instances[most_populated_az].insert(0, instance)
            instance = az_instances[most_populated_az].pop()
        logger.info("Deleting replica {0}".format(instance))
        deleted_replicas.append(instance)
        try:
            aws.delete_db_instance(DBInstanceIdentifier=instance, SkipFinalSnapshot=True)
        except rds_client.exceptions.InvalidDBInstanceStateFault as e:
            # Do not break Service Instance for this, update can happen later.
            logger.warn("Cluster Not Ready for Update: {}".format(e))
        difference += 1
    return deleted_replicas


def find_available_id(az_instances, identifier):
    instance_identifiers = {instance: None for instances in az_instances.values() for instance in instances}
    for i in range(100):
        new_identifier = identifier + str(i)
        if new_identifier not in instance_identifiers:
            return new_identifier


def least_populated(az_instances):
    count = 100
    least_populated_az = "none"
    logger.info("az_instances {0}".format(az_instances))
    for az, instances in az_instances.items():
        if count > len(instances):
            count = len(instances)
            least_populated_az = az
    return least_populated_az


def most_populated(az_instances):
    count = 0
    most_populated_az = "none"
    for az, instances in az_instances.items():
        if count < len(instances):
            count = len(instances)
            most_populated_az = az
    return most_populated_az


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
            logger.info("'%s' has been removed from '%s'" % (cluster_properties["DBClusterIdentifier"],
                                                             response["GlobalCluster"]["GlobalClusterIdentifier"]))
            # wait for the db cluster to be promoted to standalone
            logger.info("Waiting 2 minutes before deleting old DB cluster")
            time.sleep(120)

    # now delete the leftover db cluster
    result.status = delete_db_cluster(cluster_properties)
    return result


def delete_global_cluster(global_cluster_id):
    """
    Delete a global cluster, with all its trimmings

    :param id: global cluster identifier
    :return: None
    """
    # first, remove any members
    clusters = aws.describe_global_clusters(GlobalClusterIdentifier=global_cluster_id)["GlobalClusters"]
    for cluster in clusters:
        for member in cluster["GlobalClusterMembers"]:
            if member["IsWriter"] is False or len(cluster["GlobalClusterMembers"]) == 1:
                remove_from_global(global_cluster_id, member["DBClusterArn"])
                if len(cluster["GlobalClusterMembers"]) > 1:
                    logger.info("Waiting 5 minutes for {} to exit {}".format(member["DBClusterArn"], global_cluster_id))
                    time.sleep(300)
                    return delete_global_cluster(global_cluster_id)
    # now we can delete him
    aws.delete_global_cluster(GlobalClusterIdentifier=global_cluster_id)


def remove_from_global(global_cluster_id, db_cluster_id):
    for attempt in range(3):
        try:
            logger.info("Removing {} from global cluster {}".format(db_cluster_id, global_cluster_id))
            aws.remove_from_global_cluster(GlobalClusterIdentifier=global_cluster_id,
                                           DbClusterIdentifier=db_cluster_id)
        except Exception as e:
            logger.error("Failed to remove {} from global cluster {}\n{}".format(db_cluster_id, global_cluster_id, e))

        else:
            return


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
    logger.info("Creating DB cluster and instances")
    logger.info("DB cluster parameters: {}, replicas: {}".format(cluster_properties, num_replicas))
    try:
        cluster_response = aws.create_db_cluster(**cluster_properties)
    except Exception as e:
        logger.error("Cluster Creation Failed: {}".format(e))
        result.status = cfnresponse.FAILED
        return result
    try:
        result.data["Endpoint"] = cluster_response["DBCluster"].get("Endpoint")
        result.data["ReadEndpoint"] = cluster_response["DBCluster"]["ReaderEndpoint"]
        result.data["ClusterResourceId"] = cluster_response["DBCluster"]["DbClusterResourceId"]
    except KeyError as e:
        logger.error("Unexpected results from create_db_cluster {}\n{}".format(e, cluster_response))

    # create db instances
    logger.info("DB instance parameters: {0}".format(instance_properties))
    instance_identifier = instance_properties["DBClusterIdentifier"]
    rand_int = random.randrange(12)
    for i in range(num_replicas+1):
        instance_properties["DBInstanceIdentifier"] = instance_identifier + str(i)
        # Sometimes there are more replicas than AZs
        i_az = (i + rand_int) % len(azs)
        instance_properties["AvailabilityZone"] = azs[i_az]
        try:
            aws.create_db_instance(**instance_properties)
        except Exception as e:
            logger.error("Instance Creation Failed: {}".format(e))
            result.status = cfnresponse.FAILED
            return result
    result.status = cfnresponse.SUCCESS
    return result


def get_db_cluster_data(cluster_properties, source_region, result):
    """
    Get data from cluster for result outputs.
    """
    cluster_response = aws.describe_db_clusters(DBClusterIdentifier=cluster_properties["DBClusterIdentifier"])
    result.data["Endpoint"] = cluster_response["DBClusters"][0].get("Endpoint")
    result.data["ReadEndpoint"] = cluster_response["DBClusters"][0]["ReaderEndpoint"]
    result.data["ClusterResourceId"] = cluster_response["DBClusters"][0]["DbClusterResourceId"]
    if cluster_properties.get("EnableIAMDatabaseAuthentication", "false") == "true":
        add_db_user(cluster_properties, result, source_region)


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
                    logger.info("Deleted %s" % instance["DBInstanceIdentifier"])
            # now delete writer instances
            for writer in writers:
                aws.delete_db_instance(DBInstanceIdentifier=writer)
                logger.info("Deleted %s" % writer)
        except Exception:
            logger.error("Could not delete DB instance from cluster %s" % id)

        # now we can delete the cluster
        aws.delete_db_cluster(DBClusterIdentifier=id, SkipFinalSnapshot=skip)
    else:
        logger.warn("Could not find DB cluster %s" % id)
    return cfnresponse.SUCCESS


def get_source_region(global_cluster_response):
    """
    Get writer region from 'describe_global_clusters' response.
    :param global_cluster_response: output of boto3 describe_global_clusters
    :return: aws region
    """
    clusters = global_cluster_response["GlobalClusters"]
    for cluster in clusters:
        for member in cluster["GlobalClusterMembers"]:
            if member["IsWriter"] is True:
                return member["DBClusterArn"].split(":")[3]
    return "unknown"


def add_db_user(cluster_properties, result, db_region, reapply=False):
    # Required for User ARN.
    account = boto3.client('sts').get_caller_identity().get('Account')
    cluster_id = result.data["ClusterResourceId"]
    users = {
            'admin': 'GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, ' +
                     'SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, ' +
                     'REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, ' +
                     'TRIGGER, LOAD FROM S3, SELECT INTO S3 ON *.* TO {} REQUIRE SSL WITH GRANT OPTION',
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
            logger.info("User {}: {}".format(output_user, result.data[output_user]))
        return

    master_user = cluster_properties["MasterUsername"]
    master_password = cluster_properties["MasterUserPassword"]
    endpoint = result.data["Endpoint"]

    # wait for domain name to propagate
    wait_domain_name(endpoint)
    logger.info("endpoint: {}, id: {}".format(endpoint, cluster_id))
    # master user does not have super privileges, cannot grant 'ALL PRIVILEGES ON *.*'.
    rdsdb = mysql.connector.connect(host=endpoint, user=master_user, password=master_password)
    cursor = rdsdb.cursor(buffered=True)
    for username, grant in users.items():
        grant_statement = grant.format(username)
        if username == "reader":
            output_user = "ReadUser"
        else:
            output_user = username.capitalize() + "User"
        if not user_exists(cursor, username):
            statement = """CREATE USER {} IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';""".format(username)
            logger.info(statement)
            cursor.execute(statement)
            if not user_exists(cursor, username):
                raise Exception("Unable to create User '{}'.".format(username))

            logger.info(grant_statement)
            cursor.execute(grant_statement)
        elif reapply is True:
            logger.info(grant_statement)
            cursor.execute(grant_statement)

        result.data[output_user] = "arn:aws:rds-db:{}:{}:dbuser:{}/{}".format(region, account, cluster_id, username)
        logger.info(result.data[output_user])

    rdsdb.close()
    return


def user_exists(cursor, username):
    # Verify user was created.
    statement = """SELECT User FROM mysql.user WHERE User ='{}'""".format(username)
    logger.info(statement)
    cursor.execute(statement)
    logger.info(cursor.rowcount)
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
            logger.info("domain resolution error: {}".format(err))
        else:
            return
        time.sleep(period)
    raise Exception("Domain name did not resolve in {} seconds".format(period*attempts))


def handler(event, context):
    response = CustomResourceResponse()
    logger.info("Event: {}".format(event))
    try:
        if event['RequestType'] == 'Create':
            try:
                response = create_rdsglobal(event)
                logger.info("Response Data: {}", response.data)
            except Exception as e:
                logger.info(str(e))
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
                logger.info(str(e))
                traceback.print_exc()
                phys_id = event['PhysicalResourceId']
                cfnresponse.send(event, context, cfnresponse.FAILED, response.data, phys_id, str(e))
        logger.info("Response Data: {}".format(response.data))
        cfnresponse.send(event, context, response.status, response.data, response.id)
    except Exception as e:
        logger.info(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response.data, response.id, str(e))


class CustomResourceResponse(object):

    """ Custom resource notification response """

    def __init__(self):
        self.status = cfnresponse.FAILED
        self.id = None
        self.data = {}
