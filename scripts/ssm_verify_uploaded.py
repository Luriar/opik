"""
Verify the uploaded file is complete (check tail)
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
            out = inv.get("StandardOutputContent","").strip()
            err = inv.get("StandardErrorContent","").strip()
            print(out[-2000:])
            if err: print(f"ERR: {err[:300]}")
            return s, out, err
    return "TIMEOUT", "", ""

print("=== Tail of uploaded file (last 30 lines) ===")
run_ssm("tail -30 /home/ec2-user/spark_jobs/spark_silver_to_delta.py", 30)

print("\n=== Full file test with python3 (syntax check) ===")
run_ssm("python3 -c 'import py_compile; py_compile.compile(\"/home/ec2-user/spark_jobs/spark_silver_to_delta.py\", doraise=True); print(\"SYNTAX OK\")'", 30)
