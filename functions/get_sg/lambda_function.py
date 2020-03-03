import cfnresponse
import string
import boto3
import random
import traceback


alnum = string.ascii_uppercase + string.ascii_lowercase + string.digits
ec2_client = boto3.client('ec2')

# Get security group ID from name.
def get_sg(sg_name):
    # Glob wildcards may be used.
    security_groups = ec2_client.describe_security_groups(Filters=[{'Name':'tag:Name', 'Values': [sg_name]}])
    return [sg['GroupId'] for sg in security_groups['SecurityGroups']][0]

def handler(event, context):
    response_code = cfnresponse.SUCCESS
    response_data = {}
    print(event)
    if event['RequestType'] == 'Create':
        phys_id = ''.join(random.choice(alnum) for _ in range(16))
    else:
        phys_id = event['PhysicalResourceId']
    try:
        if event['RequestType'] in ['Create', 'Update']:
            for i,sg in enumerate(event['ResourceProperties']['SGName'].split(',')):
                key = 'SecurityGroupId{}'.format(i)
                response_data[key] = get_sg(sg)
        cfnresponse.send(event, context, response_code, response_data, phys_id)
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data, phys_id, str(e))
