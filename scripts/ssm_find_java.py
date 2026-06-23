"""
Find Java version and spark config on EC2
"""
import boto3, time

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
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
    "echo '=== java versions ==='; find / -name 'java' -type f 2>/dev/null | head -10",
    "echo '=== java -version ==='; /usr/bin/java -version 2>&1; /usr/lib/jvm/jre-11/bin/java -version 2>&1",
    "echo '=== find jdk ==='; ls /usr/lib/jvm/ 2>/dev/null",
    "echo '=== pyspark java check ==='; python3 -c \"import pyspark; print(pyspark.__file__)\"",
    "echo '=== spark class check ==='; head -20 /usr/local/lib/python3.9/site-packages/pyspark/bin/spark-class",
    "echo '=== which java ==='; which java; java -version 2>&1; echo '---'; ls -la /etc/alternatives/java 2>/dev/null",
    "echo '=== yum list java ==='; sudo yum list installed 2>/dev/null | grep -i java | head -10",
]

for c in cmds:
    print(f"Running: {c[:60]}...")
    out, err = run_ssm(c, 30)
    if out: print(out[:500])
    if err: print(f"  ERR: {err[:300]}")
    print()
