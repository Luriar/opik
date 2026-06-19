"""
Check the printf upload result
"""
import boto3, time

INSTANCE_ID = "i-0395d9432acf6630d"
REGION = "ap-northeast-2"
ssm = boto3.client("ssm", region_name=REGION)

def run_ssm(commands, timeout=30):
    if isinstance(commands, str):
        commands = [commands]
    label = commands[0][:60]
    print(f"  SSM [{label}]")
    resp = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
        TimeoutSeconds=max(30, timeout),
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
            if s == "Success":
                for line in out.split("\n")[-10:]:
                    if line.strip(): print(f"    {line.strip()}")
            else:
                print(f"    STATUS={s}")
                if err: print(f"    ERR: {err[:500]}")
            return s, out, err
    return "TIMEOUT", "", ""

s, out, _ = run_ssm("wc -c /home/ec2-user/spark_jobs/spark_silver_to_delta.py")
print(f"Size result: {s}, out: {out}")

s, out, _ = run_ssm("wc -l /home/ec2-user/spark_jobs/spark_silver_to_delta.py")
print(f"Lines: {out}")

s, out, _ = run_ssm("tail -10 /home/ec2-user/spark_jobs/spark_silver_to_delta.py")
print(f"Tail: {out}")

s, out, _ = run_ssm("python3 -c \"import py_compile; py_compile.compile('/home/ec2-user/spark_jobs/spark_silver_to_delta.py', doraise=True); print('SYNTAX OK')\"")
print(f"Syntax: {s} / {out}")
