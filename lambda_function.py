import os
import boto3
from datetime import datetime, timedelta, timezone

ec2 = boto3.client("ec2")
cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

IDLE_CPU_THRESHOLD = float(os.environ.get("IDLE_CPU_THRESHOLD", "5"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "14"))
SNAPSHOT_AGE_DAYS = int(os.environ.get("SNAPSHOT_AGE_DAYS", "60"))


def get_name_tag(tags):
    """Return the Name tag from an AWS resource."""
    for tag in tags or []:
        if tag.get("Key") == "Name":
            return tag.get("Value", "")
    return ""


def get_idle_ec2_instances():
    idle_instances = []
    paginator = ec2.get_paginator("describe_instances")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=LOOKBACK_DAYS)

    for page in paginator.paginate(
        Filters=[
            {
                "Name": "instance-state-name",
                "Values": ["running"]
            }
        ]
    ):
        for reservation in page["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]

                metrics = cloudwatch.get_metric_statistics(
                    Namespace="AWS/EC2",
                    MetricName="CPUUtilization",
                    Dimensions=[
                        {
                            "Name": "InstanceId",
                            "Value": instance_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=["Average"]
                )

                datapoints = metrics.get("Datapoints", [])

                if not datapoints:
                    continue

                avg_cpu = sum(
                    point["Average"] for point in datapoints
                ) / len(datapoints)

                if avg_cpu < IDLE_CPU_THRESHOLD:
                    idle_instances.append(
                        {
                            "InstanceId": instance_id,
                            "Name": get_name_tag(instance.get("Tags")),
                            "InstanceType": instance["InstanceType"],
                            "AverageCPU": round(avg_cpu, 2),
                            "LookbackDays": LOOKBACK_DAYS
                        }
                    )

    return idle_instances


def get_unused_volumes():
    unused_volumes = []
    paginator = ec2.get_paginator("describe_volumes")

    for page in paginator.paginate(
        Filters=[
            {
                "Name": "status",
                "Values": ["available"]
            }
        ]
    ):
        for volume in page["Volumes"]:
            unused_volumes.append(
                {
                    "VolumeId": volume["VolumeId"],
                    "Name": get_name_tag(volume.get("Tags")),
                    "SizeGB": volume["Size"],
                    "VolumeType": volume["VolumeType"],
                    "AvailabilityZone": volume["AvailabilityZone"],
                    "CreatedAt": volume["CreateTime"].isoformat()
                }
            )

    return unused_volumes


def get_unused_eips():
    unused_eips = []

    # describe_addresses does not currently require pagination.
    response = ec2.describe_addresses()

    for address in response.get("Addresses", []):
        # An EIP without AssociationId is not currently associated.
        if not address.get("AssociationId"):
            unused_eips.append(
                {
                    "AllocationId": address.get("AllocationId", "EC2-Classic"),
                    "PublicIp": address.get("PublicIp", "Unknown"),
                    "Domain": address.get("Domain", "Unknown")
                }
            )

    return unused_eips


def get_old_snapshots():
    """
    Find snapshots owned by this AWS account that are older than the
    configured retention threshold.

    This reports old snapshots only. It does not prove they are unused.
    """
    old_snapshots = []
    cutoff_time = datetime.now(timezone.utc) - timedelta(
        days=SNAPSHOT_AGE_DAYS
    )

    paginator = ec2.get_paginator("describe_snapshots")

    for page in paginator.paginate(OwnerIds=["self"]):
        for snapshot in page["Snapshots"]:
            start_time = snapshot["StartTime"]

            if start_time < cutoff_time:
                snapshot_age = (
                    datetime.now(timezone.utc) - start_time
                ).days

                old_snapshots.append(
                    {
                        "SnapshotId": snapshot["SnapshotId"],
                        "Name": get_name_tag(snapshot.get("Tags")),
                        "VolumeId": snapshot.get("VolumeId", "Unknown"),
                        "VolumeSizeGB": snapshot.get("VolumeSize", 0),
                        "State": snapshot.get("State", "Unknown"),
                        "CreatedAt": start_time.isoformat(),
                        "AgeDays": snapshot_age,
                        "Description": snapshot.get("Description", "")
                    }
                )

    old_snapshots.sort(
        key=lambda snapshot: snapshot["AgeDays"],
        reverse=True
    )

    return old_snapshots


def format_items(items, empty_message):
    if not items:
        return empty_message

    return "\n".join(
        f"- {item}" for item in items
    )


def publish_report(
    idle_ec2,
    unused_volumes,
    unused_eips,
    old_snapshots
):
    generated_at = datetime.now(timezone.utc).isoformat()

    report = f"""
AWS Cost Optimization Report
==================================

SUMMARY
----------------------------------
Idle EC2 instances: {len(idle_ec2)}
Unattached EBS volumes: {len(unused_volumes)}
Unused Elastic IPs: {len(unused_eips)}
Snapshots older than {SNAPSHOT_AGE_DAYS} days: {len(old_snapshots)}

IDLE EC2 INSTANCES
CPU threshold: less than {IDLE_CPU_THRESHOLD}%
Lookback period: {LOOKBACK_DAYS} days
----------------------------------
{format_items(idle_ec2, "No idle EC2 instances found.")}

UNATTACHED EBS VOLUMES
----------------------------------
{format_items(unused_volumes, "No unattached EBS volumes found.")}

UNUSED ELASTIC IPS
----------------------------------
{format_items(unused_eips, "No unused Elastic IPs found.")}

OLD EBS SNAPSHOTS
Age threshold: older than {SNAPSHOT_AGE_DAYS} days
----------------------------------
{format_items(old_snapshots, "No old EBS snapshots found.")}

IMPORTANT
----------------------------------
Old snapshots are not automatically considered unused.
Check whether they are required by:
- An AMI
- AWS Backup
- Disaster recovery policies
- Compliance or retention requirements

Generated at: {generated_at}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="AWS Cost Optimizer Report",
        Message=report
    )

    return report


def lambda_handler(event, context):
    idle_ec2 = get_idle_ec2_instances()
    unused_volumes = get_unused_volumes()
    unused_eips = get_unused_eips()
    old_snapshots = get_old_snapshots()

    report = publish_report(
        idle_ec2,
        unused_volumes,
        unused_eips,
        old_snapshots
    )

    return {
        "statusCode": 200,
        "idle_ec2_count": len(idle_ec2),
        "unused_volume_count": len(unused_volumes),
        "unused_eip_count": len(unused_eips),
        "old_snapshot_count": len(old_snapshots),
        "snapshot_age_threshold_days": SNAPSHOT_AGE_DAYS,
        "report": report
    }
