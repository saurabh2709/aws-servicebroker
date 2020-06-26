# *************************************************************************
#
# ADOBE CONFIDENTIAL
# ___________________
#
#  Copyright 2016 Adobe Systems Incorporated
#  All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains
# the property of Adobe Systems Incorporated and its suppliers,
# if any.  The intellectual and technical concepts contained
# herein are proprietary to Adobe Systems Incorporated and its
# suppliers and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination of this information or reproduction of this material
# is strictly forbidden unless prior written permission is obtained
# from Adobe Systems Incorporated.
# *************************************************************************

from __future__ import absolute_import, division, print_function, unicode_literals

import base64
import inspect
import json
import logging
import re
import requests
import time
import traceback
import troposphere.ec2 as ec2
import troposphere.sns as sns

import boto3

from botocore.exceptions import ClientError, WaiterError
from botocore.parsers import ResponseParserError
from operator import itemgetter
from six import string_types
from socket import error as SocketError
from _ssl import SSLError

# aws_call() retry parameters
RETRY_WAIT_MIN = 0.5
RETRY_WAIT_MAX = 30.0
RETRY_WAIT_FACTOR = 1.5

# ISO 8601 format
ISO8601FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


# our logger
logger = logging.getLogger(__name__)


def fixer(client, operation):
    """
    A method decorator to fix boto3 arguments. If the arguments come from a custom resource notification,
    they must be converted from strings to the expected types.

    :param client: service client name
    :param operation: operation name
    :return: wrapped function
    """
    def fixer(func):
        def wrapper(self, **kwargs):
            return func(self, **self.repair_arguments(eval("self.%s" % client), operation, kwargs))
        return wrapper
    return fixer


class AwsHelper(object):
    """Helper class to perform AWS API calls"""

    def __init__(self, region):
        """
        Constructs client objects.

        :param region: the AWS region
        :param account: AWS account configuration
        """

        self.sts = boto3.client("sts")
        self.caller = self.get_caller_identity()

        # establish our AWS connections
        self.cfn = boto3.client("cloudformation")
        self.ec2 = boto3.client("ec2")
        self.r53 = boto3.client("route53")
        self.s3 = boto3.client("s3")
        self.cdn = boto3.client("cloudfront")
        self.iam = boto3.client("iam")
        self.elb = boto3.client("elb")
        self.kms = boto3.client("kms")
        self.asg = boto3.client("autoscaling")
        self.rds = boto3.client("rds")
        self.rdsd = boto3.client("rds-data")
        self.sqs = boto3.client("sqs")
        self.sns = boto3.client("sns")
        self.logs = boto3.client("logs")
        self.lamb = boto3.client("lambda")
        self.eks = boto3.client("eks")

        # and some other handy Lego blocks
        self.region = region
        self.storage = None

    # boto3 helpers

    def operation_arguments(self, client, operation):
        """
        Return the arguments for a boto3 operation

        :param client: service client object
        :param operation: operation name
        :return: dict of operation arguments
        """
        return client._service_model.operation_model(operation).input_shape.members

    def repair_arguments(self, client, operation, kwargs):  # noqa: C901 the flake8 complexity check reports 11 paths
        """
        Repair method arguments

        Why is this necessary? All properties in custom resource notifications are strings.
        If we want to pass them to boto calls, we have to convert them to the expected types.

        :param client: service client object
        :param operation: operation name
        :param kwargs: keyword argument dictionary
        :return: updated keyword argument dictionary
        """
        members = self.operation_arguments(client, operation)
        # walk the operation arguments
        for argument, shape in members.items():
            if shape.name in ["Boolean", "BooleanOptional"]:
                value = kwargs.get(argument)
                if isinstance(value, string_types):
                    kwargs[argument] = value in ["true", "True", "1"]
            elif shape.name in ["Integer", "IntegerOptional"]:
                value = kwargs.get(argument)
                if isinstance(value, string_types):
                    kwargs[argument] = int(value)
            elif shape.name in ["Long", "LongOptional"]:
                value = kwargs.get(argument)
                if isinstance(value, string_types):
                    kwargs[argument] = long(value)
            elif shape.name in ["Float"]:
                value = kwargs.get(argument)
                if isinstance(value, string_types):
                    kwargs[argument] = float(value)
            else:
                value = None
            if isinstance(value, string_types):
                logger.info("Converting %s:%s to %s" % (operation, argument, shape.name))
        return kwargs

    # platform-specific APIs

    @property
    def configuration_key(self):
        return "cfn"

    def hidden_parameter(self, settings):
        """
        Determine whether a template parameter is hidden

        :param settings: template parameter settings
        :return: boolean
        """
        noecho = settings.get(self.configuration_key, {}).get("NoEcho")
        return noecho in [True, "True", "true"]

    def template_cache(self, suite, settings):
        """
        Establish an S3 bucket for temporary template storage

        :param suite: suite name
        :param settings: settings values
        :return: None
        """
        # generate a region-specific template bucket name
        bucket = "-".join([settings["dexbuilder"]["bucket"],
                           self.region.replace("-", ""),
                           settings["dexbuilder"]["account"]])
        logger.info("template bucket is %s" % bucket)
        args = {}
        if self.region != "us-east-1":
            args["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        # ensure that the template bucket exists
        self.create_bucket(ACL="private",
                           Bucket=bucket,
                           **args)
        # tag it
        self.put_bucket_tagging(Bucket=bucket,
                                Tagging={"TagSet": [{"Key": "Owner", "Value": "DEXBuilder"},
                                                    {"Key": "Adobe.DataClassification", "Value": "Restricted"}]})
        # and throw out templates after a configured number of days
        expiration = settings["dexbuilder"]["template_expiration"]
        self.put_bucket_lifecycle_configuration(Bucket=bucket,
                                                LifecycleConfiguration={"Rules": [{"ID": "Cleanup",
                                                                                   "Prefix": "templates",
                                                                                   "Expiration": {"Days": expiration},
                                                                                   "Status": "Enabled"}]})
        # save the suite name and bucket name for later
        self.suite = suite
        self.bucket = bucket

        # and, while we're in here, set up an SNS topic for custom resource notifications
        self.topic = self.create_topic(Name="-".join([suite, self.region.replace("-", ""), "dexbuilder"]),
                                       Tags=[{"Key": "Owner", "Value": "DEXBuilder"}])["TopicArn"]
        logger.info("custom resource topic is %s" % self.topic)

    def upload_template(self, stack, body):
        """
        Upload the specified CloudFormation template to the configured S3 bucket

        :param stack: stack name
        :param body: template text
        :return: S3 URL, for use by subsequent CloudFormation operations
        """
        key = "/".join(["templates", self.suite, stack + ".template"])
        self.put_object(ACL="private",
                        Body=body,
                        Bucket=self.bucket,
                        Key=key,
                        ServerSideEncryption="AES256")
        url = self.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key})
        if len(url) > 1024:
            # CloudFormation APIs limit the URL length, but some
            # scenarios (AWS-HMAC-SHA4 signing while using an IAM role, for example)
            # generate longer strings. Fall back.
            url = "https://s3.amazonaws.com/%s/%s" % (self.bucket, key)
        logger.debug(url)
        return url

    def details(self, id, generator, executor):
        """
        Wait for a CloudFormation operation to complete

        :param id: stack identifier
        :param generator: DEXBuilderGenerator object
        :param executor: DexBuilderExecutor object
        :return: describe_stacks() response
        """
        status = executor.progress[0]
        poll = executor.CFN_POLL_MIN
        details = None
        while status in executor.progress:
            # process any custom resources
            generator.custom_resources.process(id)

            # get the stack description, after a little nap
            time.sleep(poll)
            response = self.describe_stacks(StackName=id)

            # process the response
            details = response["Stacks"][0]
            status = details["StackStatus"]
            logger.info("%s: %s" % (id, status))

            # exponential backoff for the poll interval
            if poll < executor.CFN_POLL_MAX:
                poll = poll * 2

        if status not in executor.complete:
            # display the CloudFormation events
            logger.error("Events for stack %s:\n%s" % (executor.label, "\n".join(self.describe_stack_events(id))))
        return details

    def dummy_resource(self, title):
        """
        Return a dummy resource, to avoid a
        "Template format error: At least one Resources member must be defined." error

        :param title: template title
        :return: AWSObject
        """
        vpc = self.get_default_vpc_id()
        if vpc:
            # cook up a phony resource to keep CloudFormation happy
            resource = ec2.NetworkAcl(title + "NullNacl", VpcId=vpc)
        else:
            resource = sns.Topic(title + "NullTopic")
        return resource

    def deployment_tags(self, generator):
        """
        Return deployment tags

        :param generator: DexBuilderGenerator object
        :return: tags list
        """
        return generator.dict_to_tags(generator.tags)

    # CloudFormation wrappers

    def validate_template(self, TemplateURL=None, **kwargs):
        return aws_call(lambda: self.cfn.validate_template(TemplateURL=TemplateURL))

    def create_stack(self, **kwargs):
        # We sometimes get spurious AlreadyExistsException errors from this API...the API reports AlreadyExists but
        # the create operation continues. A mutex around the create_stack call seems to eliminate the problem, and
        # it is less ugly than adding some squirrelly logic to an exception handler.
        with self.lock:
            return aws_call(lambda: self.cfn.create_stack(**kwargs), ignore=["AlreadyExistsException"])

    def update_stack(self, **kwargs):
        return aws_call(lambda: self.cfn.update_stack(**kwargs), ignore=["ValidationError"])

    def delete_stack(self, StackName=None, **kwargs):
        aws_call(lambda: self.cfn.delete_stack(StackName=StackName, **kwargs))
        return {"StackId": StackName}

    def list_stacks(self, filter=["CREATE_IN_PROGRESS",  # by default, fetch everything except 'DELETE_COMPLETE'
                                  "CREATE_FAILED",
                                  "CREATE_COMPLETE",
                                  "ROLLBACK_IN_PROGRESS",
                                  "ROLLBACK_FAILED",
                                  "ROLLBACK_COMPLETE",
                                  "DELETE_IN_PROGRESS",
                                  "DELETE_FAILED",
                                  "UPDATE_IN_PROGRESS",
                                  "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
                                  "UPDATE_COMPLETE",
                                  "UPDATE_ROLLBACK_IN_PROGRESS",
                                  "UPDATE_ROLLBACK_FAILED",
                                  "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                                  "UPDATE_ROLLBACK_COMPLETE"]):
        response = aws_call(lambda: self.cfn.list_stacks(StackStatusFilter=filter))
        result = {"StackSummaries": response["StackSummaries"]}
        while "NextToken" in response:
            response = aws_call(lambda: self.cfn.list_stacks(StackStatusFilter=filter,
                                                             NextToken=response["NextToken"]))
            result["StackSummaries"].extend(response["StackSummaries"])
        return result

    def describe_stacks(self, **kwargs):
        response = aws_call(lambda: self.cfn.describe_stacks(**kwargs))
        result = {"Stacks": response["Stacks"]}
        while "NextToken" in response:
            response = aws_call(lambda: self.cfn.describe_stacks(NextToken=response["NextToken"], **kwargs))
            result["Stacks"].extend(response["Stacks"])
        return result

    def describe_stack_resources(self, **kwargs):
        try:
            result = aws_call(lambda: self.cfn.describe_stack_resources(**kwargs))
        except:
            result = {"StackResources": []}
        return result

    def create_change_set(self, **kwargs):
        return aws_call(lambda: self.cfn.create_change_set(**kwargs))

    def describe_change_set(self, **kwargs):
        response = aws_call(lambda: self.cfn.describe_change_set(**kwargs))
        changes = response["Changes"]
        while "NextToken" in response:
            response = aws_call(lambda: self.cfn.describe_change_set(NextToken=response["NextToken"], **kwargs))
            changes.extend(response["Changes"])
        response["Changes"] = changes
        return response

    def delete_change_set(self, **kwargs):
        return aws_call(lambda: self.cfn.delete_change_set(**kwargs))

    def get_template(self, **kwargs):
        return aws_call(lambda: self.cfn.get_template(**kwargs))

    def describe_stack_events(self, stack):
        """
        Return a list of recent events for a specified stack, in reverse chronological order

        :param stack: stack name or ID
        :return: list of stack event strings
        """
        result = []
        try:
            response = aws_call(lambda: self.cfn.describe_stack_events(StackName=stack))
            more, strings = self.process_stack_events(response.get("StackEvents", []))
            result += strings
            while more and "NextToken" in response:
                response = aws_call(lambda: self.cfn.describe_stack_events(StackName=stack,
                                                                           NextToken=response["NextToken"]))
                more, strings = self.process_stack_events(response.get("StackEvents", []))
                result += strings
        except Exception as e:
            logger.error(e)
        return result

    def process_stack_events(self, events):
        """
        Process a batch of events from the describe_stack_events() call

        :param events: stack events from describe_stack_events()
        :return: tuple(True to continue/False if done, list of processed event strings)
        """
        more = True
        strings = []
        for event in events:
            if more:
                # format the event
                reason = event.get("ResourceStatusReason", "")
                strings.append(" %s(%s) %s%s %s" % (event["ResourceType"],
                                                    event["LogicalResourceId"],
                                                    event["ResourceStatus"],
                                                    ":" if reason else "",
                                                    reason))
                # is this the oldest event of interest?
                if event["ResourceType"] == "AWS::CloudFormation::Stack" and \
                   event["ResourceStatus"] in ["CREATE_IN_PROGRESS", "DELETE_IN_PROGRESS", "UPDATE_IN_PROGRESS"]:
                    # yep...no mas
                    more = False
        return more, strings

    # EC2 wrappers

    def describe_instances(self, **kwargs):
        return aws_call(lambda: self.ec2.describe_instances(**kwargs))

    def describe_security_groups(self, **kwargs):
        return aws_call(lambda: self.ec2.describe_security_groups(**kwargs))

    def revoke_security_group_egress(self, **kwargs):
        return aws_call(lambda: self.ec2.revoke_security_group_egress(**kwargs))

    def revoke_security_group_ingress(self, **kwargs):
        return aws_call(lambda: self.ec2.revoke_security_group_ingress(**kwargs))

    def create_vpc_peering_connection(self, **kwargs):
        return aws_call(lambda: self.ec2.create_vpc_peering_connection(**kwargs))

    def describe_vpc_peering_connections(self, **kwargs):
        try:
            result = aws_call(lambda: self.ec2.describe_vpc_peering_connections(**kwargs),
                              ignore=["InvalidVpcPeeringConnectionID.NotFound"])
        except:
            # cook up a result so the caller won't have to handle exceptions
            result = {"VpcPeeringConnections": []}
        return result

    def modify_vpc_peering_connection_options(self, **kwargs):
        return aws_call(lambda: self.ec2.modify_vpc_peering_connection_options(**kwargs))

    def delete_vpc_peering_connection(self, **kwargs):
        try:
            return aws_call(lambda: self.ec2.delete_vpc_peering_connection(**kwargs), ignore=["InvalidStateTransition"])
        except Exception as e:
            logger.info(e)

    def accept_vpc_peering_connection(self, **kwargs):
        # If we call accept_vpc_peering_connection too quickly after create_vpc_peering_connection,
        # boto3 may throw one of these exceptions:
        #   * ValidationError (if the connection is not yet in a 'pending-acceptance' state)
        #   * InvalidVpcPeeringConnectionID.NotFound (if we try to accept the connection so quickly
        #     that the receiving VPC does not think the peering connection even exists)
        try:
            return aws_call(
                lambda: self.ec2.accept_vpc_peering_connection(**kwargs),
                retry=[
                    "ValidationError",
                    "InvalidVpcPeeringConnectionID.NotFound"
                ]
            )
        except:
            # if acceptance fails (no credentials, perhaps) simply log the error and soldier on
            pass

    def describe_vpcs(self, **kwargs):
        return aws_call(lambda: self.ec2.describe_vpcs(**kwargs))

    def get_default_vpc_id(self):
        """
        Return the VPC ID of the default VPC.

        :return: Default VPC ID
        """
        vpcs = self.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
        return vpcs[0]["VpcId"] if len(vpcs) > 0 else None

    def describe_tags(self, **kwargs):
        """
        Return the specified resource tags

        :param kwargs: resource filters
        :return: {"Tags": []}
        """
        result = {"Tags": []}
        try:
            response = aws_call(lambda: self.ec2.describe_tags(**kwargs))
            result["Tags"].extend(response["Tags"])
            while "NextToken" in response:
                response = aws_call(lambda: self.ec2.describe_tags(NextToken=response["NextToken"], **kwargs))
                result["Tags"].extend(response["Tags"])
        except:
            # an error has already been logged, so return the partial result
            pass
        return result

    def describe_resource_tags(self, resource_id):
        """
        Return the tags for a specified resource

        :param resource_id: resource identifier
        :return: dictionary of resource tags
        """
        tags = {}
        response = self.describe_tags(Filters=[{"Name": "resource-id", "Values": [resource_id]}])
        for tag in response["Tags"]:
            if tag["ResourceId"] == resource_id:
                tags[tag["Key"]] = tag["Value"]
            else:
                logger.warn("Ignoring tag for %s" % tag["ResourceId"])
        return tags

    def create_tags(self, retry=[], **kwargs):
        return aws_call(lambda: self.ec2.create_tags(**kwargs), retry=retry)

    def update_tags(self, resource_id, tags):
        """
        Update the tags for a specified resource

        :param resource_id: resource identifier
        :param tags: list of Key/Value tag definitions
        :return: None
        """
        # get the existing tags
        response = self.describe_resource_tags(resource_id)
        # construct a set of the new tag keys
        keys = {tag["Key"] for tag in tags}
        # create a list of the tags to delete
        filter = [{"Key": tag} for tag in response if tag not in keys]
        # are any of the existing tags unwanted?
        if filter:
            # yep...delete them
            self.delete_tags(Resources=[resource_id], Tags=filter)
        # now we can create the new tags
        return self.create_tags(Resources=[resource_id], Tags=tags)

    def delete_tags(self, **kwargs):
        return aws_call(lambda: self.ec2.delete_tags(**kwargs))

    def describe_route_tables(self, **kwargs):
        try:
            result = aws_call(lambda: self.ec2.describe_route_tables(**kwargs))
        except:
            # an error has already been logged, so cook up an innocuous result
            result = {"RouteTables": []}
        return result

    def create_route(self, **kwargs):
        try:
            result = aws_call(lambda: self.ec2.create_route(**kwargs), ignore=["RouteAlreadyExists"])
        except Exception as e:
            # demote RouteAlreadyExists errors
            logger.warn(e)
            result = {"Return": False}
        return result

    def delete_route(self, **kwargs):
        return aws_call(lambda: self.ec2.delete_route(**kwargs))

    def modify_route(self, **kwargs):
        return aws_call(lambda: self.ec2.modify_route(**kwargs))

    def create_key_pair(self, **kwargs):
        return aws_call(lambda: self.ec2.create_key_pair(**kwargs), ignore=["InvalidKeyPair.Duplicate"])

    def delete_key_pair(self, **kwargs):
        return aws_call(lambda: self.ec2.delete_key_pair(**kwargs))

    def get_image_id(self, filters, **kwargs):
        """
        Return the ID of the most recent AMI matching the specified filters

        :param filters: something like [{"Name": "tag:Color", "Values": ["blue", "green"]}]
                        http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.describe_images
                        Values can contain wildcards
        :return: AMI ID
        """
        try:
            # phony up a result...this might be a validate or print action
            result = "ami-00000000"
            if filters:
                # get all specified images
                images = []
                response = aws_call(lambda: self.ec2.describe_images(Filters=filters, **kwargs))
                # sort them by creation date
                for image in response["Images"]:
                    images.append((image["ImageId"], time.strptime(image["CreationDate"], ISO8601FORMAT)))
                if len(images) > 0:
                    results = sorted(images, key=itemgetter(1), reverse=True)
                    # return the most recent
                    result = results[0][0]
        except Exception as e:
            logger.error("Could not find AMI %s: %s" % (filters, e))
            result = None
        return result

    def describe_nat_gateways(self, **kwargs):
        return aws_call(lambda: self.ec2.describe_nat_gateways(**kwargs))

    def instance_ami(self, id):
        """
        Return the AMI ID of a specified instance

        :param id: instance ID
        :return: AMI ID
        """
        result = None
        response = aws_call(lambda: self.ec2.describe_instances(InstanceIds=[id]))
        reservations = response.get("Reservations", [])
        if len(reservations) == 1:
            instances = reservations[0].get("Instances", [])
            if len(instances) == 1:
                result = instances[0].get("ImageId")
        logger.info("Instance %s was created from AMI %s" % (id, result))
        return result

    def describe_subnets(self, **kwargs):
        return aws_call(lambda: self.ec2.describe_subnets(**kwargs))

    # ASG wrappers

    def describe_auto_scaling_groups(self, **kwargs):
        response = aws_call(lambda: self.asg.describe_auto_scaling_groups(**kwargs))
        result = {"AutoScalingGroups": response["AutoScalingGroups"]}
        while "NextToken" in response:
            response = aws_call(lambda: self.asg.describe_auto_scaling_groups(NextToken=response["NextToken"],
                                                                              **kwargs))
            result["AutoScalingGroups"].extend(response["AutoScalingGroups"])
        return result

    def describe_launch_configurations(self, **kwargs):
        return aws_call(lambda: self.asg.describe_launch_configurations(**kwargs))

    def terminate_instance_in_auto_scaling_group(self, **kwargs):
        return aws_call(lambda: self.asg.terminate_instance_in_auto_scaling_group(**kwargs))

    def set_desired_capacity(self, **kwargs):
        try:
            return aws_call(lambda: self.asg.set_desired_capacity(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    def attach_load_balancer_target_groups(self, **kwargs):
        return aws_call(lambda: self.asg.attach_load_balancer_target_groups(**kwargs))

    def detach_load_balancer_target_groups(self, **kwargs):
        return aws_call(lambda: self.asg.detach_load_balancer_target_groups(**kwargs))

    # Route53 wrappers

    def get_hosted_zone(self, **kwargs):
        return aws_call(lambda: self.r53.get_hosted_zone(**kwargs))

    def list_hosted_zones(self, **kwargs):
        result = {"HostedZones": []}
        try:
            response = aws_call(lambda: self.r53.list_hosted_zones(**kwargs))
            result["HostedZones"].extend(response["HostedZones"])
            while "NextMarker" in response:
                response = aws_call(lambda: self.r53.list_hosted_zones(Marker=response["NextMarker"], **kwargs))
                result["HostedZones"].extend(response["HostedZones"])
        except:
            # an error has already been logged, so return the partial result
            pass
        return result

    def list_hosted_zones_by_name(self, **kwargs):
        return aws_call(lambda: self.r53.list_hosted_zones_by_name(**kwargs))

    def associate_vpc_with_hosted_zone(self, **kwargs):
        return aws_call(lambda: self.r53.associate_vpc_with_hosted_zone(**kwargs))

    def disassociate_vpc_from_hosted_zone(self, **kwargs):
        return aws_call(lambda: self.r53.disassociate_vpc_from_hosted_zone(**kwargs))

    def change_resource_record_sets(self, **kwargs):
        return aws_call(lambda: self.r53.change_resource_record_sets(**kwargs))

    def list_resource_record_sets(self, **kwargs):
        return aws_call(lambda: self.r53.list_resource_record_sets(**kwargs))

    def hosted_zone_id_from_name(self, dnsname):
        """
        Return the ID of the hosted zone.

        :param dnsname: Hosted Zone Domain Name
        :return: Hosted Zone ID
        """
        zone_info = self.list_hosted_zones_by_name(DNSName=dnsname, MaxItems="1")
        # ID returned by API is "/hostedzone/<Zone ID>"
        try:
            zone_id = zone_info["HostedZones"][0]["Id"]
        except IndexError:
            logger.error("Hosted Zone {} not found".format(dnsname))
            return
        if zone_info["HostedZones"][0]["Name"] == dnsname:
            return zone_id.replace("/hostedzone/", "")
        else:
            # Raise Error here instead of returning?
            logger.error("Hosted Zone {} not found".format(dnsname))
            return

    def recordset_exists(self, hosted_zone_id, recordset):
        retrieved_recordset = self.list_resource_record_sets(HostedZoneId=hosted_zone_id,
                                                             StartRecordName=recordset["Name"],
                                                             MaxItems="1")
        if retrieved_recordset["ResourceRecordSets"][0]["Name"] == recordset["Name"]:
            logger.info("Found RecordSet {}".format(recordset["Name"]))
            return True
        else:
            logger.info("RecordSet {} not found".format(recordset["Name"]))
            return False

    # STS wrappers

    def assume_role(self, **kwargs):
        return aws_call(lambda: self.sts.assume_role(**kwargs))

    def get_caller_identity(self, **kwargs):
        return aws_call(lambda: self.sts.get_caller_identity(**kwargs))

    @property
    def account(self):
        """
        Determine the account number associated with this connection

        :return: account number
        """
        try:
            account = self.get_caller_identity().get("Account", "")
        except:
            account = ""
        return account

    def get_original_identity(self):
        """
        Return the original caller identity, before any assume_role() call

        :return: get_caller_identity() response
        """
        return self.caller

    # S3 wrappers

    def create_bucket(self, **kwargs):
        try:
            result = aws_call(lambda: self.s3.create_bucket(**kwargs), ignore=["BucketAlreadyExists",
                                                                               "BucketAlreadyOwnedByYou"])
        except:
            result = {"Location": ""}
        return result

    def put_object(self, **kwargs):
        return aws_call(lambda: self.s3.put_object(**kwargs))

    def upload_file(self, **kwargs):
        return aws_call(lambda: self.s3.upload_file(**kwargs))

    def upload_fileobj(self, **kwargs):
        return aws_call(lambda: self.s3.upload_fileobj(**kwargs))

    def generate_presigned_url(self, ClientMethod, Params=None, ExpiresIn=3600, HttpMethod=None):
        return aws_call(lambda: self.s3.generate_presigned_url(ClientMethod, Params, ExpiresIn, HttpMethod))

    def get_bucket_lifecycle_configuration(self, **kwargs):
        return aws_call(lambda: self.s3.get_bucket_lifecycle_configuration(**kwargs))

    def put_bucket_lifecycle_configuration(self, **kwargs):
        return aws_call(lambda: self.s3.put_bucket_lifecycle_configuration(**kwargs))

    def put_bucket_policy(self, **kwargs):
        return aws_call(lambda: self.s3.put_bucket_policy(**kwargs))

    def put_bucket_tagging(self, **kwargs):
        return aws_call(lambda: self.s3.put_bucket_tagging(**kwargs))

    def head_object(self, **kwargs):
        return aws_call(lambda: self.s3.head_object(**kwargs))

    def bucket_is_empty(self, bucket):
        response = aws_call(lambda: self.s3.list_objects_v2(Bucket=bucket, MaxKeys=1))
        objects = response.get("Contents", [])
        return len(objects) == 0

    def copy_bucket(self, source, target, pattern=None):
        args = {"Bucket": source}
        done = False
        count = 0
        logger.info("Copying the contents of %s to %s" % (source, target))
        try:
            matcher = re.compile(pattern) if pattern else None
            # walk the source bucket
            while not done:
                response = aws_call(lambda: self.s3.list_objects_v2(**args))
                objects = response.get("Contents", [])
                # walk the objects in this batch
                for object in objects:
                    # a person of interest?
                    if not matcher or matcher.match(object["Key"]):
                        # copy the object, with reasonable guesses for the arguments
                        aws_call(lambda: self.s3.copy_object(Bucket=target,
                                                             CopySource={"Bucket": source, "Key": object["Key"]},
                                                             Key=object["Key"],
                                                             MetadataDirective="COPY",
                                                             TaggingDirective="COPY",
                                                             ServerSideEncryption="AES256",
                                                             StorageClass=object["StorageClass"]))
                        count += 1
                        if count % 100 == 0:
                            # report some progress
                            logger.info("%d objects copied" % count)
                # all done?
                if response.get("IsTruncated"):
                    # nope...keep going where we left off
                    args["ContinuationToken"] = response.get("NextContinuationToken")
                else:
                    # yep
                    done = True
        except:
            # an error has already been logged, so skedaddle
            pass
        logger.info("%d objects copied" % count)

    # CloudFront wrappers

    def create_cloud_front_origin_access_identity(self, **kwargs):
        return aws_call(lambda: self.cdn.create_cloud_front_origin_access_identity(**kwargs))

    def get_distribution_config(self, **kwargs):
        return aws_call(lambda: self.cdn.get_distribution_config(**kwargs))

    def update_distribution(self, **kwargs):
        return aws_call(lambda: self.cdn.update_distribution(**kwargs))

    # IAM wrappers

    def get_server_certificate(self, **kwargs):
        return aws_call(lambda: self.iam.get_server_certificate(**kwargs), ignore=["NoSuchEntity"])

    def upload_server_certificate(self, **kwargs):
        try:
            result = aws_call(lambda: self.iam.upload_server_certificate(**kwargs), ignore=["AccessDenied"])
        except Exception as e:
            # demote AccessDenied errors and phony up a response
            logger.warn(e)
            result = {"ServerCertificateMetadata": {"Arn": "arn:aws:iam::xxxxxxxxxxxx:server-certificate/xxx"}}
        return result

    def get_role_arn(self, name):
        try:
            response = aws_call(lambda: self.iam.get_role(RoleName=name), ignore=["NoSuchEntity"])
            arn = response["Role"]["Arn"]
        except Exception as e:
            # demote NoSuchEntity errors and cook up a response
            logger.warn(e)
            arn = "arn:aws:iam::xxxxxxxxxxxx:role/" + name
            # and demote the log message
        return arn

    def get_user_arn(self, name):
        try:
            response = aws_call(lambda: self.iam.get_user(UserName=name), ignore=["NoSuchEntity"])
            arn = response["User"]["Arn"]
        except Exception as e:
            # demote NoSuchEntity errors and cook up a response
            logger.warn(e)
            arn = ""
        return arn

    def get_policy_arn(self, name, **kwargs):
        """
        Return the ARN of an IAM managed policy

        :param name: policy name, which may have been generated by CloudFormation
        :param kwargs: iam.list_policies() additional arguments
        :return: policy ARN
        """
        # get the managed policies
        policies = self.list_policies(**kwargs).get("Policies", [])
        # prune based on name
        arns = []
        matcher = re.compile(".+-%s-.+" % name)
        for policy in policies:
            if name == policy["PolicyName"]:
                # an exact match...go to the head of the class
                arns.insert(0, policy["Arn"])
            elif matcher.match(policy["PolicyName"]):
                # a pattern match...take it if nothing better shows up
                arns.append(policy["Arn"])
        logger.debug("managed policy ARNs: %s", arns)
        # return the best choice
        return arns[0] if len(arns) > 0 else ""

    def list_policies(self, **kwargs):
        response = aws_call(lambda: self.iam.list_policies(**kwargs))
        result = {"Policies": response["Policies"]}
        while "Marker" in response:
            response = aws_call(lambda: self.iam.list_policies(Marker=response["Marker"], **kwargs))
            result["Policies"].extend(response["Policies"])
        return result

    def create_access_key(self, **kwargs):
        return aws_call(lambda: self.iam.create_access_key(**kwargs))

    def list_access_keys(self, **kwargs):
        return aws_call(lambda: self.iam.list_access_keys(**kwargs))

    # ELB wrappers

    def describe_load_balancers(self, **kwargs):
        result = {"LoadBalancerDescriptions": []}
        try:
            response = aws_call(lambda: self.elb.describe_load_balancers(**kwargs), ignore=["LoadBalancerNotFound"])
            result["LoadBalancerDescriptions"].extend(response["LoadBalancerDescriptions"])
            while "NextMarker" in response:
                response = aws_call(lambda: self.elb.describe_load_balancers(Marker=response["NextMarker"], **kwargs))
                result["LoadBalancerDescriptions"].extend(response["LoadBalancerDescriptions"])
        except:
            # an error has already been logged, so return the partial result
            pass
        return result

    def describe_load_balancer_policies(self, **kwargs):
        return aws_call(lambda: self.elb.describe_load_balancer_policies(**kwargs))

    def describe_load_balancer_tags(self, LoadBalancerNames=None, **kwargs):
        if not LoadBalancerNames:
            # this means all
            response = self.describe_load_balancers()
            LoadBalancerNames = [balancer["LoadBalancerName"] for
                                 balancer in response.get("LoadBalancerDescriptions", [])]
        # the weak API only handles 20 names at a time
        tags = []
        names = [LoadBalancerNames[i:i + 20] for i in range(0, len(LoadBalancerNames), 20)]
        for chunk in names:
            response = aws_call(lambda: self.elb.describe_tags(LoadBalancerNames=chunk, **kwargs))
            tags.extend(response["TagDescriptions"])
        return {"TagDescriptions": tags}

    def set_load_balancer_listener_ssl_certificate(self, **kwargs):
        return aws_call(lambda: self.elb.set_load_balancer_listener_ssl_certificate(**kwargs),
                        retry=["CertificateNotFound"])  # retry since the certificate may have just been uploaded

    def delete_load_balancer(self, **kwargs):
        return aws_call(lambda: self.elb.delete_load_balancer(**kwargs))

    def find_load_balancers(self, filters):
        """
        Find load balancers with the specified tags

        :param filters: dictionary of tag key:value pairs
        :return: list of matching load balancers
        """
        result = []
        if filters:
            response = self.describe_load_balancer_tags()
            for balancer in response["TagDescriptions"]:
                # convert the load balancer tags to a dict for easier matching
                tags = {}
                for tag in balancer["Tags"]:
                    tags[tag["Key"]] = tag["Value"]
                # does this guy have the specified tags?
                match = True
                for key, value in filters.items():
                    match = match and (value == tags.get(key))
                if match:
                    # bingo
                    result.append(balancer["LoadBalancerName"])
        return result

    # KMS wrappers

    def create_alias(self, **kwargs):
        return aws_call(lambda: self.kms.create_alias(**kwargs))

    def delete_alias(self, **kwargs):
        return aws_call(lambda: self.kms.delete_alias(**kwargs))

    def list_aliases(self, **kwargs):
        response = aws_call(lambda: self.kms.list_aliases(**kwargs))
        result = {"Aliases": response["Aliases"]}
        while "NextMarker" in response:
            response = aws_call(lambda: self.kms.list_aliases(Marker=response["NextMarker"], **kwargs))
            result["Aliases"].extend(response["Aliases"])
        return result

    def describe_key(self, **kwargs):
        try:
            result = aws_call(lambda: self.kms.describe_key(**kwargs), ignore=["NotFoundException"])
        except:
            result = {"KeyMetadata": {}}
        return result

    def generate_random(self, bytes):
        """
        Returns a cryptographically secure random byte string

        :param bytes: byte string length
        :return: byte string
        """
        return aws_call(lambda: self.kms.generate_random(NumberOfBytes=bytes))["Plaintext"]

    def kms_encrypt(self, **kwargs):
        """
        Encrypt plaintext, directly using a specified Customer Master Key.
        This API should only be used for small amounts of non-critical data.

        :return: Base64-encoded ciphertext
        """
        logger.warn("This API is not as secure as the encrypt() envelope encryption API.")
        response = aws_call(lambda: self.kms.encrypt(**kwargs))
        return base64.b64encode(response.get("CiphertextBlob", ""))

    def filter(self, record):
        """
        Filter log events

        :param record: LogRecord object
        :return: 0 or 1
        """
        if record.levelno == logging.INFO:
            # demote aws_encryption_sdk and noisy urllib3.connectionpool INFO events
            logger.debug(record.msg % (record.args))
            result = 0
        else:
            result = 1
        return result

    # RDS wrappers

    def update_db_password(self, db_cluster_identifier, master_password):
        return aws_call(lambda: self.rds.modify_db_cluster(DBClusterIdentifier=db_cluster_identifier,
                                                           ApplyImmediately=True, MasterUserPassword=master_password))

    @fixer("rds", "ModifyDBInstance")
    def modify_db_instance(self, **kwargs):
        return aws_call(lambda: self.rds.modify_db_instance(**kwargs),
                        retry=["InvalidDBInstanceState"],  # we might have to wait for the instance to become available
                        wait_max=300.0)

    @fixer("rds", "ModifyDBCluster")
    def modify_db_cluster(self, **kwargs):
        return aws_call(lambda: self.rds.modify_db_cluster(**kwargs))

    @fixer("rds", "CreateDBCluster")
    def create_db_cluster(self, **kwargs):
        return aws_call(lambda: self.rds.create_db_cluster(**kwargs))

    @fixer("rds", "CreateDBInstance")
    def create_db_instance(self, **kwargs):
        return aws_call(lambda: self.rds.create_db_instance(**kwargs),
                        retry=["InvalidDBClusterStateFault"],
                        wait_max=300.0)

    @fixer("rds", "DeleteDBCluster")
    def delete_db_cluster(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.delete_db_cluster(**kwargs))
        except:
            result = {"DBCluster": {}}
        return result

    def delete_db_instance(self, **kwargs):
        return aws_call(lambda: self.rds.delete_db_instance(**kwargs))

    def promote_read_replica_db_cluster(self, **kwargs):
        return aws_call(lambda: self.rds.promote_read_replica_db_cluster(**kwargs))

    def describe_db_instances(self, **kwargs):
        return aws_call(lambda: self.rds.describe_db_instances(**kwargs))

    def describe_db_clusters(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.describe_db_clusters(**kwargs), ignore=["DBClusterNotFoundFault"])
        except:
            result = {"DBClusters": []}
        return result

    def get_db_instance_arn(self, **kwargs):
        dbs = self.describe_db_instances(**kwargs)["DBInstances"]
        return dbs[0]["DBInstanceArn"] if len(dbs) > 0 else "arn:aws:rds:us-east-1:XXXXXXXXXXXX:db:XXXXXX"

    def get_db_cluster_arn(self, **kwargs):
        dbs = self.describe_db_clusters(**kwargs)["DBClusters"]
        return dbs[0]["DBClusterArn"] if len(dbs) > 0 else "arn:aws:rds:us-east-1:XXXXXXXXXXXX:cluster:XXXXXX"

    def get_db_cluster_endpoint(self, id):
        dbs = self.describe_db_clusters(**kwargs)["DBClusters"]
        return dbs[0]["Endpoint"] if len(dbs) > 0 else "XXXXXXXXXXXX.rds.amazonaws.com"

    def list_tags_for_resource(self, **kwargs):
        return aws_call(lambda: self.rds.list_tags_for_resource(**kwargs))

    def add_tags_to_resource(self, **kwargs):
        return aws_call(lambda: self.rds.add_tags_to_resource(**kwargs))

    def describe_global_clusters(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.describe_global_clusters(**kwargs),
                              ignore=["GlobalClusterNotFoundFault"])
        except:
            result = {"GlobalClusters": []}
        return result

    @fixer("rds", "CreateGlobalCluster")
    def create_global_cluster(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.create_global_cluster(**kwargs))
        except:
            result = {"GlobalCluster": {}}
        return result

    def modify_global_cluster(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.modify_global_cluster(**kwargs))
        except:
            result = {"GlobalCluster": {}}
        return result

    def remove_from_global_cluster(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.remove_from_global_cluster(**kwargs))
        except:
            result = {"GlobalCluster": {}}
        return result

    def delete_global_cluster(self, **kwargs):
        try:
            result = aws_call(lambda: self.rds.delete_global_cluster(**kwargs))
        except:
            result = {"GlobalCluster": {}}
        return result

    def get_global_cluster_arn(self, id):
        c = self.describe_global_clusters(GlobalClusterIdentifier=id)["GlobalClusters"]
        return c[0]["GlobalClusterArn"] if len(c) > 0 else "arn:aws:rds:us-east-1:XXXXXXXXXXXX:global-cluster:XXXXXX"

    def get_global_cluster_resourceid(self, id):
        c = self.describe_global_clusters(GlobalClusterIdentifier=id)["GlobalClusters"]
        return c[0]["GlobalClusterResourceId"] if len(c) > 0 else "cluster-XXXXXXXXXXXX"

    def get_global_writer_arn(self, id):
        result = "arn:aws:rds:us-east-1:XXXXXXXXXXXX:cluster:XXXXXX"
        c = self.describe_global_clusters(GlobalClusterIdentifier=id)["GlobalClusters"]
        if len(c) > 0:
            for db in c[0]["GlobalClusterMembers"]:
                if db["IsWriter"]:
                    result = db["DBClusterArn"]
        return result

    def get_global_cluster_endoint(self, id):
        result = "arn:aws:rds:us-east-1:XXXXXXXXXXXX:cluster:XXXXXX"
        c = self.describe_global_clusters(GlobalClusterIdentifier=id)["GlobalClusters"]
        if len(c) > 0:
            for db in c[0]["GlobalClusterMembers"]:
                if db["IsWriter"]:
                    result = db["DBClusterArn"]
        return result

    # RDSDataService wrappers

    def execute_statement(self, **kwargs):
        return aws_call(lambda: self.rdsd.execute_statement(**kwargs))

    # SQS wrappers

    def create_queue(self, **kwargs):
        try:
            result = aws_call(lambda: self.sqs.create_queue(**kwargs),
                              retry=["AWS.SimpleQueueService.QueueDeletedRecently"])
        except:
            result = {"QueueUrl": ""}
        return result

    def delete_queue(self, **kwargs):
        try:
            return aws_call(lambda: self.sqs.delete_queue(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    def get_queue_arn(self, url):
        try:
            response = aws_call(lambda: self.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"]))
            arn = response["Attributes"]["QueueArn"]
        except:
            arn = "arn:aws:sqs:xxxxxxxx:xxxxxxxxxxxx:xxxxxxxx"
        return arn

    def get_queue_url(self, **kwargs):
        try:
            response = aws_call(lambda: self.sqs.get_queue_url(**kwargs))
            url = response.get("QueueUrl")
        except:
            url = "https://queue.amazonaws.com"
        return url

    def tag_queue(self, **kwargs):
        try:
            return aws_call(lambda: self.sqs.tag_queue(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    def set_queue_attributes(self, **kwargs):
        try:
            return aws_call(lambda: self.sqs.set_queue_attributes(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    def receive_message(self, **kwargs):
        try:
            result = aws_call(lambda: self.sqs.receive_message(**kwargs))
        except:
            result = {"Messages": []}
        return result

    def delete_message(self, **kwargs):
        try:
            return aws_call(lambda: self.sqs.delete_message(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    # SNS wrappers

    def create_topic(self, **kwargs):
        return aws_call(lambda: self.sns.create_topic(**kwargs))

    def delete_topic(self, **kwargs):
        return aws_call(lambda: self.sns.delete_topic(**kwargs))

    def subscribe(self, **kwargs):
        try:
            result = aws_call(lambda: self.sns.subscribe(**kwargs))
        except:
            result = {"SubscriptionArn": ""}
        return result

    def unsubscribe(self, **kwargs):
        try:
            return aws_call(lambda: self.sns.unsubscribe(**kwargs))
        except:
            # an error has already been logged, so skedaddle
            pass

    # EKS wrappers

    def update_cluster_config(self, **kwargs):
        try:
            # update the configuration
            cluster = kwargs["name"]
            response = aws_call(lambda: self.eks.update_cluster_config(**kwargs), ignore=["InvalidParameterException"])
            logger.debug(response)

            # wait up to 1 minute for the change to take effect
            waiter = aws_call(lambda: self.eks.get_waiter("cluster_active"))
            waiter.wait(name=cluster, WaiterConfig={"MaxAttempts": 2})
            logger.info("Configuring cluster config %s" % cluster)
        except ClientError as e:
            if "No changes needed" in str(e):
                logger.info("No configuration changes required for %s" % cluster)
            else:
                raise
        except WaiterError as e:
            logger.error("Failed to configure EKS cluster %s: %s" % (cluster, e))
            raise

    # loggroup wrappers

    def put_retention_policy(self, loggroupname, retentionindays):
        return aws_call(lambda: self.logs.put_retention_policy(logGroupName=loggroupname,
                        retentionInDays=retentionindays))

    # Lambda wrappers

    def update_function_code(self, **kwargs):
        return aws_call(lambda: self.lamb.update_function_code(**kwargs))

    def invoke_function(self, **kwargs):
        return aws_call(lambda: self.lamb.invoke(**kwargs))

    # platform-specific classes

    class DeploymentParameters(list):
        """
        AWS deployment parameters
        """
        def add_parameter(self, key, value):
            self.append({"ParameterKey": key,
                         "ParameterValue": value})

        def get_parameter(self, key):
            value = None
            for parameter in self:
                if parameter["ParameterKey"] == key:
                    value = parameter["ParameterValue"]
            return value

    class CustomResources(object):
        """
        CloudFormation custom resource components
        """
        def __init__(self, generator):
            """
            Create a helper for custom resources

            :param generator: DEXBuilderGenerator object
            """
            self.generator = generator
            self.cloud = generator.cloud

            # create the required components
            name = "-".join([generator.label, str(int(time.time()))[-8:]])
            logger.debug("Creating custom resource components: %s" % name)
            self.queue = self.cloud.create_queue(QueueName=name)["QueueUrl"]
            arn = self.cloud.get_queue_arn(self.queue)
            policy = {
                "Version": "2008-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "sqs:SendMessage",
                        "Resource": arn,
                        "Condition": {
                            "ArnEquals": {"aws:SourceArn": self.cloud.topic}
                        }
                    }
                ]
            }
            self.cloud.tag_queue(QueueUrl=self.queue,
                                 Tags=generator.tags)
            self.cloud.set_queue_attributes(QueueUrl=self.queue,
                                            Attributes={"Policy": json.dumps(policy)})
            self.subscription = self.cloud.subscribe(TopicArn=self.cloud.topic,
                                                     Protocol="sqs",
                                                     Endpoint=arn)["SubscriptionArn"]

        def __del__(self):
            """
            Clean up any custom resource components
            """
            logger.debug("Cleaning up custom resource components")
            self.cloud.unsubscribe(SubscriptionArn=self.subscription)
            self.cloud.delete_queue(QueueUrl=self.queue)

        def process(self, id):
            """
            Process custom resource notifications

            :param id: stack identifier
            :return: None
            """
            response = self.cloud.receive_message(QueueUrl=self.queue, MaxNumberOfMessages=10)
            # any news?
            for message in response.get("Messages", []):
                body = json.loads(message.get("Body", "{}"))
                type = body.get("Type")
                self.cloud.delete_message(QueueUrl=self.queue, ReceiptHandle=message["ReceiptHandle"])
                if type == "Notification":
                    logger.debug(body.get("Subject"))
                    notification = json.loads(body.get("Message", "{}"))
                    # is this me?
                    if notification.get("StackId") == id:
                        # yep
                        logger.info("Custom resource request '%s %s'" % (notification.get("RequestType"),
                                                                         notification.get("LogicalResourceId")))
                        target = notification.get("ResponseURL")
                        properties = notification.get("ResourceProperties")
                        # execute the handler for this custom resource
                        properties.pop("ServiceToken")
                        handler = self.generator.import_resource(properties.pop("ServiceHandler"))
                        report = handler.custom(self.cloud, notification)
                        # send the result back to CloudFormation
                        answer = requests.put(target, data=json.dumps(report))
                        logger.info("Custom resource CloudFormation response: %d" % answer.status_code)
                else:
                    logger.warn("Ignoring %s notification" % type)


def instance_sort(instance):
    """
    Return a sort key which can be used to sort instances by launch time

    :param instance: instance description from describe_instances()
    :return: sort key
    """
    return instance["LaunchTime"]


def subnet_sort(subnet):
    """
    Return a sort key which can be used to sort subnets by number of available IP addresses

    :param instance: subnet description from describe_subnets()
    :return: sort key
    """
    return subnet["AvailableIpAddressCount"]


def aws_call(function, retry=[], ignore=[], wait_max=RETRY_WAIT_MAX):
    """
    AWS API wrapper to retry throttling (and other specified) errors

    :param function: the AWS API to call
    :param retry: a list of exception error codes to retry in addition to "Throttling"
    :param ignore: a list of exception error codes to silently rethrow
    :param wait_max: the maximum retry wait, in seconds
    """
    logger.debug(inspect.getsource(function))
    wait = RETRY_WAIT_MIN
    while True:
        try:
            return function()

        except ClientError as e:
            # inspect boto3 exceptions
            error_code = e.response["Error"].get("Code", "Unknown")
            if error_code in retry + ["Throttling", "RequestLimitExceeded"] and wait < wait_max:
                # sleep and retry
                logger.warn("%s, retry in %.3f seconds" % (e.message, wait))
                time.sleep(wait)
                wait = wait * RETRY_WAIT_FACTOR
            elif error_code in ignore:
                # rethrow without logging
                raise
            elif error_code == "DryRunOperation":
                # just log a warning and exit
                logger.warn(e.response["Error"].get("Message", "DryRunOperation"))
                return
            else:
                # log and rethrow
                logger.error(e)
                raise

        except (SSLError, ResponseParserError, SocketError) as e:
            # we occasionally see random errors (SSL, invalid response, connection reset by peer)
            if wait < wait_max:
                # sleep and retry
                api = traceback.extract_stack(limit=2)[0][2]
                logger.warn("%s for %s, retry in %.3f seconds" % (e.__class__.__name__, api, wait))
                time.sleep(wait)
                wait = wait * RETRY_WAIT_FACTOR
            else:
                # log and rethrow
                logger.error(e)
                raise

        except Exception as e:
            # log and rethrow
            logger.error("%s: %s" % (e.__class__.__name__, e))
            raise
