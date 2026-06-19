"""
Run Delta Lake backfill on EC2 via SSM
"""
import boto3, time, sys

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=300):
    label = commands[0][:60]
    print(f"  SSM [{label}] timeout={timeout}s")
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
        TimeoutSeconds=max(30, timeout),
    )
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 15:
        time.sleep(5)
        waited += 5
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            out = inv.get("StandardOutputContent","").strip()
            err = inv.get("StandardErrorContent","").strip()
            print(f"    STATUS={s}")
            for line in out.split("\n")[-30:]:
                if line.strip(): print(f"    {line.strip()}")
            if err:
                for line in err.split("\n")[-10:]:
                    if line.strip(): print(f"    [ERR] {line.strip()}")
            return s, out, err
    return "TIMEOUT", "", ""

BACKFILL_CMD = (
    "export JAVA_HOME=/usr/lib/jvm/jre-11; "
    "export PATH=$JAVA_HOME/bin:$PATH; "
    f"cd /home/ec2-user/spark_jobs && "
    f"spark-submit --master 'local[4]' --driver-memory 6g {REMOTE_SCRIPT} --backfill"
)

print("=== Running Delta backfill (expect 2-3 minutes) ===")
print(f"  Command: {BACKFILL_CMD[:100]}...")

status, stdout, stderr = run_ssm([BACKFILL_CMD], timeout=300)

if status == "Success":
    print("\n=== BACKFILL COMPLETED SUCCESSFULLY ===")
else:
    print(f"\n=== BACKFILL STATUS: {status} ===")
