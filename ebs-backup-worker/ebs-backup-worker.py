""" Manages backups for EBS volumes based on a "backup" tag """

import datetime
import logging
import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
myhandler = logging.StreamHandler()  # writes to stderr
myformatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
myhandler.setFormatter(myformatter)
logger.addHandler(myhandler)

DEFAULT_RETENTION_PERIOD = 21

def cleanup_snapshots(vol_id, retention_days):
    """ Removes old snapshots that past retention period"""
    ec2 = boto3.client('ec2')
    response = ec2.describe_snapshots(
        Filters=[{'Name': 'volume-id', 'Values': [vol_id]}]
    )
    current_time = datetime.datetime.now(datetime.timezone.utc)
    output = {'removed': 0, 'retained': 0}
    try:
        while True:    
            for snapshot in response['Snapshots']:
                time_diff = current_time - snapshot['StartTime']
                if time_diff.days > retention_days:
                    print("Vol: %s | Snap: %s | Date: %s" % (vol_id, snapshot['SnapshotId'], snapshot['StartTime']))
                    ec2.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
                    output['removed'] = output['removed'] + 1
                else:
                    output['retained'] = output['retained'] + 1
            if 'NextToken' in response and response['NextToken']:
                response = ec2.describe_snapshots(
                    Filters=[{'Name': 'volume-id', 'Values': [vol_id]}],
                    NextToken=response['NextToken']
                )
            else:
                break
    except: pass
    return output

def snapshot_single(vol_id):
    """ Handles a single volume """
    # Create a new snapshot of the volume
    output = {}
    ec2 = boto3.client('ec2')
    ecr = boto3.resource('ec2')
    volume = ec2.describe_volumes(
        Filters=[{'Name': 'volume-id', 'Values': [vol_id]}]
    )['Volumes'][0]
    logger.info(volume)
    snap = ec2.create_snapshot(
        VolumeId=volume['VolumeId'],
        Description="Auto-generated snapshot via lambda:ebs-backup-worker"
    )
    output['snapshot'] = snap['SnapshotId']
    logger.info("Created snapshot %s for volume %s" % (snap['SnapshotId'], volume['VolumeId']))

    # Tag the new snapshot
    new_tags = []
    retention_days = False
    for tag in volume['Tags']:
        if tag['Key'] == 'BackupRetentionDays':
            retention_days = int(tag['Value'])
        if tag['Key'].lower().find('backup') != 0:
            new_tags.append(tag)
    snap = ecr.Snapshot(snap['SnapshotId'])
    snap.create_tags(Tags=new_tags)

    # Clean up old snapshots
    if retention_days and retention_days > 0:
        output['cleanup'] = cleanup_snapshots(vol_id, retention_days)

    return output

def snapshot_all():
    """ Create snapshot for all volumes based on tag values """
    ec2 = boto3.client('ec2')
    volumes = ec2.describe_volumes(
        Filters=[{'Name': 'tag-key', 'Values': ['backup', 'Backup']}]
    )['Volumes']

    output = {}
    for volume in volumes:
        output[volume['VolumeId']] = snapshot_single(volume['VolumeId'])

    return output

def cleanup_all():
    """ Just performs a full cleanup """
    ec2 = boto3.client('ec2')
    volumes = ec2.describe_volumes(
        Filters=[{'Name': 'tag-key', 'Values': ['backup', 'Backup']}]
    )['Volumes']

    output = {}
    for volume in volumes:
        output[volume['VolumeId']] = {}
        tags = volume['Tags']
        retention_days = DEFAULT_RETENTION_PERIOD
        for tag in tags:
            if tag['Key'] == 'BackupRetentionDays':
                retention_days = int(tag['Value'])
        if retention_days and retention_days > 0:
            output[volume['VolumeId']]['cleanup'] = cleanup_snapshots(volume['VolumeId'], retention_days)

    return output

def handler(event, context):
    """ If we get a vol ID then just back up that one """
    if 'vol_id' in event and event['vol_id']:
        logger.info("Generating snapshot for volume ID: %s" % (event['vol_id']))
        result = snapshot_single(event['vol_id'])
    elif 'cleanup' in event and event['cleanup']:
        logger.info("Performing snapshot cleanup")
        result = cleanup_all()
    else:
        logger.info("Generating snapshots for all backup volumes")
        result = snapshot_all()
    return result

if __name__  == "__main__":
    """ Execute an empty event handler """
    logger.info(handler({'cleanup': True}, {}))
