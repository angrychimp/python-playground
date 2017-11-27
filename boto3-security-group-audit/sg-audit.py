""" Find orphaned security groups and report or remove them """

import boto3
import sys
import argparse
import logging

logging.basicConfig(level=logging.ERROR, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

def get_security_group_list(vpc_id):
    ec2 = boto3.client('ec2')
    response = ec2.describe_security_groups(Filters=[{ 'Name': 'vpc-id', 'Values': [vpc_id] }])
    sg_list = {}
    for sg in response['SecurityGroups']:
        sg['Resources'] = {}
        sg_list[sg['GroupId']] = sg
    return sg_list

def find_orphans(vpc_id):
    """ Find orphaned security groups """
    # First, get a list of all groups
    sg_list = get_security_group_list(vpc_id)
    
    # Remove groups from the list if they're in use
    # EC2
    client = boto3.client('ec2')
    response = client.describe_instances()
    while True:
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                for sg in instance['SecurityGroups']:
                    if sg['GroupId'] in sg_list:
                        sg_list.pop(sg['GroupId'])
                        logger.warn("Found security group: %s [%s]" % (sg['GroupId'], 'EC2'))
        if 'NextToken' in response and response['NextToken']:
            response = client.describe_instances(NextToken=response['NextToken'])
        else:
            break
    # ELB
    client = boto3.client('elb')
    response = client.describe_load_balancers()
    while True:
        for load_balancer in response['LoadBalancerDescriptions']:
            for sg in load_balancer['SecurityGroups']:
                if sg in sg_list:
                    sg_list.pop(sg)
                    logger.warn("Found security group: %s [%s]" % (sg, 'ELBv1'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_load_balancers(Marker=response['NextMarker'])
        else:
            break
    client = boto3.client('elbv2')
    response = client.describe_load_balancers()
    while True:
        for load_balancer in response['LoadBalancers']:
            for sg in load_balancer['SecurityGroups']:
                if sg in sg_list:
                    sg_list.pop(sg)
                    logger.warn("Found security group: %s [%s]" % (sg, 'ELBv2'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_load_balancers(Marker=response['NextMarker'])
        else:
            break
    # EFS
    client = boto3.client('efs')
    response = client.describe_file_systems()
    while True:
        for efs in response['FileSystems']:
            mount_targets = client.describe_mount_targets(FileSystemId=efs['FileSystemId'])
            for target in mount_targets['MountTargets']:
                security_groups = client.describe_mount_target_security_groups(MountTargetId=target['MountTargetId'])
                for sg in security_groups['SecurityGroups']:
                    if sg in sg_list:
                        sg_list.pop(sg)
                        logger.warn("Found security group: %s [%s]" % (sg, 'EFS'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_file_systems(Marker=response['NextMarker'])
        else:
            break
    # RDS
    client = boto3.client('rds')
    response = client.describe_db_instances()
    while True:
        for db_inst in response['DBInstances']:
            for sg in db_inst['VpcSecurityGroups']:
                if sg['VpcSecurityGroupId'] in sg_list:
                    sg_list.pop(sg['VpcSecurityGroupId'])
                    logger.warn("Found security group: %s [%s]" % (sg['VpcSecurityGroupId'], 'RDS'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_db_instances(Marker=response['NextMarker'])
        else:
            break
    # Elasticache
    client = boto3.client('elasticache')
    response = client.describe_cache_clusters()
    while True:
        for cluster in response['CacheClusters']:
            for sg in cluster['SecurityGroups']:
                if sg['SecurityGroupId'] in sg_list:
                    sg_list.pop(sg['SecurityGroupId'])
                    logger.warn("Found security group: %s [%s]" % (sg['SecurityGroupId'], 'ElastiCache'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_cache_clusters(Marker=response['NextMarker'])
        else:
            break
    # Lambda
    client = boto3.client('lambda')
    response = client.list_functions()
    while True:
        for func in response['Functions']:
            if "VpcConfig" in func:
                for sg in func['VpcConfig']['SecurityGroupIds']:
                    if sg in sg_list:
                        sg_list.pop(sg)
                        logger.warn("Found security group: %s [%s]" % (sg, 'Lambda'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.list_functions(Marker=response['NextMarker'])
        else:
            break
    # Redshift
    client = boto3.client('redshift')
    response = client.describe_clusters()
    while True:
        for cluster in response['Clusters']:
            for sg in cluster['VpcSecurityGroups']:
                if sg['VpcSecurityGroupId'] in sg_list:
                    sg_list.pop(sg['VpcSecurityGroupId'])
                    logger.warn("Found security group: %s [%s]" % (sg['VpcSecurityGroupId'], 'RedShift'))
        if 'NextMarker' in response and response['NextMarker']:
            response = client.describe_clusters(Marker=response['NextMarker'])
        else:
            break
    return sg_list

def remove_orphans(vpc_id):
    """ Remove all orphaned groups """
    return find_orphans(vpc_id)

def main():
    """ Determine what to do """
    try:
        # Gather command line arguments:
        parser = argparse.ArgumentParser(
            description='Orphan security group manager',
            add_help=True
        )
        parser.add_argument(
            '--version',
            action='version',
            version='1.0'
        )
        parser.add_argument(
            "-v", "--verbose",
            action='store_true',
            default=False,
            help="Provide debbuging messages."
        )
        parser.add_argument(
            "--profile",
            action="store",
            default="default",
            help="AWS profile for API"
        )
        parser.add_argument(
            "--vpc-id",
            action="store",
            help="[Required] VPC ID to scan for security groups"
        )

        actions = parser.add_mutually_exclusive_group()
        actions.add_argument(
            "--delete",
            action='store_true',
            default=False,
            help="Remove orphaned groups"
        )
        actions.add_argument(
            "--display",
            action='store_true',
            default=False,
            help="Display orphaned security groups"
        )
        args = parser.parse_args()
        
        if not args.vpc_id:
            raise Exception("vpc-id required")

        if args.profile:
            boto3.setup_default_session(profile_name=args.profile)

        if args.verbose:
            logger.setLevel(logging.WARN)

        if args.delete is False:
            # Report only
            print(find_orphans(args.vpc_id))
        else:
            remove_orphans(args.vpc_id)
        
    except Exception as e:
        print("Exception: %s" % (e))
        quit()


if __name__ == "__main__":
    main()