"""
Check the uploaded file integrity and Python version on EC2
"""
import boto3, time

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
REMOTE_SCRIPT = "/home/ec2-user/spark_jobs/spark_silver_to_delta.py"

ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(cmd, timeout=30):
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd], "executionTimeout": [str(timeout)]},
        TimeoutSeconds=30,
    )
    cmd_id = resp["Command"]["CommandId"]
    waited = 0
    while waited < timeout + 15:
        time.sleep(2)
        waited += 2
        inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=INSTANCE_ID)
        s = inv["Status"]
        if s in ("Success", "Failed", "TimedOut", "Cancelled"):
            return inv.get("StandardOutputContent","").strip(), inv.get("StandardErrorContent","").strip()
    return "", "TIMEOUT"

cmds = [
    f"cat -n {REMOTE_SCRIPT} | head -80",
    "echo '--- Python version ---' && python3 --version",
    "echo '--- Python3.9 pyspark ---' && /usr/local/bin/python3.9 --version 2>&1 || echo no39",
    "echo '--- python3 path ---' && which python3",
    "echo '--- spark-submit python check ---' && head -5 /usr/local/bin/spark-submit",
    "echo '--- pyspark python ---' && head -5 /usr/local/lib/python3.9/site-packages/pyspark/bin/spark-submit",
]

for c in cmds:
    print(f"Running: {c[:60]}...")
    out, err = run_ssm(c, 30)
    print(out[:1000])
    if err: print(f"  ERR: {err[:300]}")
    print()
