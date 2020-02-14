import cfnresponse
import string
import boto3
import random
import traceback


alnum = string.ascii_uppercase + string.ascii_lowercase + string.digits
ec2 = boto3.resource('ec2')
client = boto3.client('ec2')


def get_subnet_ids(vpc_id, name):
    vpc = ec2.Vpc(vpc_id)
    return {s.subnet_id: s.availability_zone
            for s in vpc.subnets.filter(Filters=[{'Name': 'tag:Name', 'Values': [name]}])}


def get_vpc_id(vpc_name):
    vpcs = client.describe_vpcs(Filters=[{'Name': 'tag:Name', 'Values': [vpc_name]}])
    return [vpc['VpcId'] for vpc in vpcs['Vpcs']][0]


def handler(event, context):
    response_code = cfnresponse.SUCCESS
    response_data = {}
    if event['RequestType'] == 'Create':
        phys_id = ''.join(random.choice(alnum) for _ in range(16))
    else:
        phys_id = event['PhysicalResourceId']
    try:
        if event['RequestType'] in ['Create', 'Update']:
            response_data['VpcId'] = get_vpc_id(
                event['ResourceProperties']['VpcName']
            )
            subnets = get_subnet_ids(
                response_data['VpcId'],
                event['ResourceProperties']['SubnetName']
                )
            subnet_ids = list(subnets.keys())
            random.shuffle(subnet_ids)
            response_data['SubnetIDs'] = subnet_ids
            azs = list(subnets.values())
            random.shuffle(azs)
            response_data['AvailabilityZones'] = azs
        cfnresponse.send(event, context, response_code, response_data, phys_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data, phys_id, str(e))
