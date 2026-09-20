# M5 冒烟测试：观测栈闭环——指标被抓取进 Prometheus，看板预置进 Grafana
#   1) compose 起 prometheus + grafana，server 开 /metrics 跑在宿主
#   2) 打流量（3 个任务到终态）
#   3) 断言：Prometheus API 能查到 agentflow_tasks_total；Grafana API 有 datasource 与看板
# 前置: docker 运行中；python 3 可用
# 用法: ./scripts/smoke-m5.ps1
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$ServerBase = "http://localhost:18099"
$PromBase = "http://localhost:9090"
$GrafanaBase = "http://localhost:3300"
$Exe = "$PSScriptRoot\..\tmp\agentflow-server.exe"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}
function WaitPort($port, $timeoutSec = 30) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $c = New-Object System.Net.Sockets.TcpClient
        try { $c.Connect("127.0.0.1", $port); return $true } catch { Start-Sleep -Milliseconds 300 }
        finally { $c.Close() }
    }
    return $false
}

Step "环境准备（compose 观测栈 + server 二进制）"
go build -o $Exe ./cmd/agentflow-server
if ($LASTEXITCODE -ne 0) { throw "构建失败" }
# PS5.1 + ErrorActionPreference=Stop 会把 docker 的 stderr 进度（Pulling...）当异常抛出
$ErrorActionPreference = "Continue"
docker compose -f "$PSScriptRoot\..\deploy\docker-compose.yml" up -d | Out-Null
if ($LASTEXITCODE -ne 0) { throw "compose up 失败" }
$ErrorActionPreference = "Stop"
Check (WaitPort 9090 60) "prometheus(9090) 就绪" "prometheus 未就绪"
Check (WaitPort 3300 60) "grafana(3300) 就绪" "grafana 未就绪"

$procs = @()
$composeUp = $true
try {
    # ---------- 1. server 开指标 ----------
    Step "启动 python agent + server(:18099, /metrics 开)"
    $agentProc = Start-Process python -ArgumentList "$PSScriptRoot\..\examples\python-agent\agent.py" -PassThru -WindowStyle Hidden
    $procs += $agentProc
    Check (WaitPort 8081) "agent(8081) 就绪" "agent 未监听 8081"

    $env:AGENTFLOW_ADDR = ":18099"
    $env:AGENTFLOW_METRICS_ENABLED = "1"
    $serverProc = Start-Process $Exe -PassThru -WindowStyle Hidden
    $procs += $serverProc
    Check (WaitPort 18099) "server(18099) 就绪" "server 未监听"

    # ---------- 2. 打流量：3 个带会话归因的任务 ----------
    Step "提交 3 个任务（session_id=s_smoke）"
    $agent = Invoke-RestMethod -Method Post "$ServerBase/api/v1/agents" -ContentType "application/json" -Body (@{
        name = "m5-obs"; type = "chat"
        runtime = @{ type = "python-http"; host = "http://localhost:8081" }
    } | ConvertTo-Json -Depth 5)
    $done = 0
    for ($i = 0; $i -lt 3; $i++) {
        $t = Invoke-RestMethod -Method Post "$ServerBase/api/v1/tasks" -ContentType "application/json" -Body (@{
            agent_id = $agent.id; session_id = "s_smoke"; payload = @{ q = "obs-$i" }
        } | ConvertTo-Json)
        $deadline = (Get-Date).AddSeconds(15)
        while ((Get-Date) -lt $deadline) {
            $cur = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($t.id)" -TimeoutSec 2
            if ($cur.status -in @("succeeded", "failed", "timeout", "cancelled")) { break }
            Start-Sleep -Milliseconds 200
        }
        if ($cur.status -eq "succeeded") { $done++ }
    }
    Check ($done -eq 3) "3 个任务全部 succeeded" "succeeded 数: $done/3"

    # ---------- 3. Prometheus 侧断言 ----------
    Step "等抓取周期后查 Prometheus"
    Start-Sleep -Seconds 6   # scrape 2s，留足余量
    $q = Invoke-RestMethod "$PromBase/api/v1/query?query=agentflow_tasks_total" 
    $succ = ($q.data.result | Where-Object { $_.metric.status -eq "succeeded" } | Select-Object -First 1).value[1]
    Check ([double]$succ -ge 3) "agentflow_tasks_total{succeeded} = $succ" "查询值异常: $succ"

    $qt = Invoke-RestMethod "$PromBase/api/v1/query?query=agentflow_tokens_total"
    $tok = ($qt.data.result | Where-Object { $_.metric.direction -eq "prompt" } | Select-Object -First 1).value[1]
    Check ([double]$tok -ge 126) "agentflow_tokens_total{prompt} = $tok (3x42)" "token 指标异常: $tok"

    # ---------- 4. Grafana 侧断言 ----------
    Step "Grafana datasource 与看板预置"
    $ds = Invoke-RestMethod "$GrafanaBase/api/datasources"
    Check (@($ds | Where-Object { $_.type -eq "prometheus" }).Count -ge 1) "prometheus datasource 已预置" "datasource 缺失"
    $db = Invoke-RestMethod "$GrafanaBase/api/search?query=agentflow"
    Check (@($db | Where-Object { $_.type -eq "dash-db" }).Count -ge 1) "agentflow 看板已预置" "看板缺失"
}
finally {
    Step "清理"
    foreach ($p in $procs) { try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch {} }
    Remove-Item Env:AGENTFLOW_ADDR, Env:AGENTFLOW_METRICS_ENABLED -ErrorAction SilentlyContinue
    if ($composeUp) {
        $ErrorActionPreference = "Continue"
        docker compose -f "$PSScriptRoot\..\deploy\docker-compose.yml" down | Out-Null
        $ErrorActionPreference = "Stop"
    }
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "ALL PASS: M5 观测栈冒烟通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
