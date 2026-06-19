Write-Host "=== 1. server/agents/ -- 모든 .py 파일 ==="
Get-ChildItem -Path "C:\Users\HP\Documents\opik\server\agents\" -Filter "*.py" | Select-Object Name, Length, LastWriteTime | Format-Table -AutoSize

Write-Host ""
Write-Host "=== 2. dags/ -- 모든 파일 ==="
Get-ChildItem -Path "C:\Users\HP\Documents\opik\dags\" | Select-Object Name, Length, LastWriteTime | Format-Table -AutoSize

Write-Host ""
Write-Host "=== 3. src/model/ -- 전체 파일 트리 ==="
Get-ChildItem -Path "C:\Users\HP\Documents\opik\src\model\" -Recurse | Select-Object FullName, Length, LastWriteTime | Format-Table -AutoSize

Write-Host ""
Write-Host "=== 4. server/spark_jobs/ -- 파일 리스트 ==="
Get-ChildItem -Path "C:\Users\HP\Documents\opik\server\spark_jobs\" | Select-Object Name, Length, LastWriteTime | Format-Table -AutoSize

Write-Host ""
Write-Host "=== 5. server/prompts/ -- 파일 리스트 ==="
Get-ChildItem -Path "C:\Users\HP\Documents\opik\server\prompts\" | Select-Object Name, Length, LastWriteTime | Format-Table -AutoSize

Write-Host ""
Write-Host "=== Prompt 파일 라인 수 ==="
Write-Host "--- system.md ---"
Get-Content "C:\Users\HP\Documents\opik\server\prompts\system.md" | Measure-Object -Line

Write-Host ""
Write-Host "--- intent_parser.md ---"
Get-Content "C:\Users\HP\Documents\opik\server\prompts\intent_parser.md" | Measure-Object -Line
