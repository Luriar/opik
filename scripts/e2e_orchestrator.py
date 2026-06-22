#!/usr/bin/env python3
"""E2E Pipeline Orchestrator — runs on EC2 via SSM."""

import subprocess, json, sys, time

def airflow(cmd_args, timeout=120):
    result = subprocess.run(
        ['docker', 'exec', 'opik-airflow-airflow-scheduler-1', 'airflow'] + cmd_args,
        capture_output=True, text=True, timeout=timeout
    )
    return result.stdout, result.stderr

def get_latest_run(dag_id):
    out, err = airflow(['dags', 'list-runs', '-d', dag_id, '-o', 'json'])
    try:
        runs = json.loads(out)
        if runs:
            return runs[0]
    except:
        pass
    return None

def trigger_dag(dag_id, exec_date):
    out, err = airflow(['dags', 'trigger', '-e', exec_date, dag_id])
    if 'DagRunAlreadyExists' in out:
        return 'ALREADY_EXISTS'
    if 'Created <DagRun' in out:
        return 'TRIGGERED'
    return out[:200]

def clear_dag(dag_id):
    out, err = airflow(['tasks', 'clear', '-y', '-d', dag_id])
    return 'OK' if 'Filling up the DagBag' in out else out[-200:]

def wait_for_success(dag_id, exec_date_prefix, max_seconds=300):
    for i in range(max_seconds // 10):
        time.sleep(10)
        run = get_latest_run(dag_id)
        if not run:
            continue
        if run.get('execution_date', '').startswith(exec_date_prefix):
            if run.get('state') == 'success':
                return True, run
            if run.get('state') == 'failed':
                return False, run
        if i % 6 == 0:
            st = run.get('state', '?') if run else '?'
            print(f'  waiting for {dag_id}... ({i*10}s, state={st})')
    return False, None

EXEC_DATE = '2026-06-20T15:00:00+00:00'
EXEC_PREFIX = '2026-06-20'

# Step 0: Clear all stuck runs
print('=== Step 0: Clearing stuck runs ===')
for dag in ['opik_silver_extract', 'opik_gold_structured', 'opik_gold_embeddings',
            'model_daily_prediction', 'opik_briefing', 'dag_maintenance_delta_faiss']:
    result = clear_dag(dag)
    print(f'  {dag}: {result[:80]}')

# Step 1: Trigger Silver Extract
print()
print('=== Step 1: Trigger Silver Extract ===')
result = trigger_dag('opik_silver_extract', EXEC_DATE)
print(f'  opik_silver_extract: {result}')

# Step 2: Wait for Silver
print()
print('=== Step 2: Wait for Silver ===')
ok, run = wait_for_success('opik_silver_extract', EXEC_PREFIX, max_seconds=300)
print(f'  Silver: {"SUCCESS" if ok else "FAILED/TIMEOUT"}')
if not ok:
    if run:
        print(f'  state={run.get("state")}')

# Step 3: Trigger Gold Structured + Embeddings
print()
print('=== Step 3: Trigger Gold ===')
for dag in ['opik_gold_structured', 'opik_gold_embeddings']:
    result = trigger_dag(dag, EXEC_DATE)
    print(f'  {dag}: {result}')

# Step 4: Wait for Gold
print()
print('=== Step 4: Wait for Gold ===')
for dag in ['opik_gold_structured', 'opik_gold_embeddings']:
    ok, run = wait_for_success(dag, EXEC_PREFIX, max_seconds=300)
    print(f'  {dag}: {"SUCCESS" if ok else "FAILED/TIMEOUT"}')

# Step 5: Check if Delta+FAISS triggered automatically
print()
print('=== Step 5: Check Delta+FAISS ===')
run = get_latest_run('dag_maintenance_delta_faiss')
if run:
    print(f'  Delta+FAISS: state={run.get("state")} exec_date={run.get("execution_date","?")[:19]}')

# Step 6: Trigger Model
print()
print('=== Step 6: Trigger Model ===')
result = trigger_dag('model_daily_prediction', EXEC_DATE)
print(f'  model_daily_prediction: {result}')

# Step 7: Wait for Model
print()
print('=== Step 7: Wait for Model ===')
ok, run = wait_for_success('model_daily_prediction', EXEC_PREFIX, max_seconds=600)
print(f'  Model: {"SUCCESS" if ok else "FAILED/TIMEOUT"}')

# Step 8: Trigger Briefing
print()
print('=== Step 8: Trigger Briefing ===')
result = trigger_dag('opik_briefing', EXEC_DATE)
print(f'  opik_briefing: {result}')

# Step 9: Wait for Briefing
print()
print('=== Step 9: Wait for Briefing ===')
ok, run = wait_for_success('opik_briefing', EXEC_PREFIX, max_seconds=600)
print(f'  Briefing: {"SUCCESS" if ok else "FAILED/TIMEOUT"}')

# Step 10: Summary
print()
print('=== Step 10: Final Summary ===')
for dag in ['opik_silver_extract', 'opik_gold_structured', 'opik_gold_embeddings',
            'dag_maintenance_delta_faiss', 'model_daily_prediction', 'opik_briefing']:
    run = get_latest_run(dag)
    if run:
        print(f'{dag:40s} {run.get("state","?"):12s} {run.get("execution_date","?")[:19]}')
    else:
        print(f'{dag:40s} NO_RUN')
