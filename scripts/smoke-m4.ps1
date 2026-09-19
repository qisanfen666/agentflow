# M4 冒烟测试：库形态嵌入——agentflow 挂进既有 gin 应用
#   1) 宿主业务路由（/api/orders）与挂载的控制面（/agentflow/*）共存互不干扰
#   2) 挂载路径下全链路可用：注册 -> 提交 -> SSE -> succeeded + usage
# 前置: python 3 可用（跑 examples/python-agent/agent.py），无需 docker
# 用法: ./scripts/smoke-m4.ps1
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$Base = "http://localhost:9090"
$Exe = "$PSScriptRoot\..\tmp\embed-demo.exe"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}
# 等 TCP 端口真正可连。任务重试虽能兜底 agent 慢启动，但那会把
# "环境未就绪"混进测试信号（attempt>1），这里显式等就绪保证断言干净。
function WaitPort($port, $timeoutSec = 10) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $c = New-Object System.Net.Sockets.TcpClient
        try {
            $c.Connect("127.0.0.1", $port)
            return $true
        } catch { Start-Sleep -Milliseconds 200 }
        finally { $c.Close() }
    }
    return $false
}

Step "环境准备（构建嵌入示例二进制）"
New-Item -ItemType Directory -Force "$PSScriptRoot\..\tmp" | Out-Null
go build -o $Exe ./examples/embed
if ($LASTEXITCODE -ne 0) { throw "构建失败" }
Check $true "二进制就绪" "不应到达"

$procs = @()
try {
    # ---------- 1. 执行面 + 嵌入示例宿主 ----------
    Step "启动 python agent + 嵌入示例（宿主 :9090，控制面挂 /agentflow）"
    $agentProc = Start-Process python -ArgumentList "$PSScriptRoot\..\examples\python-agent\agent.py" -PassThru -WindowStyle Hidden
    $procs += $agentProc
    Check (WaitPort 8081) "agent(8081) 就绪" "agent 未监听 8081"

    $hostProc = Start-Process $Exe -PassThru -WindowStyle Hidden
    $procs += $hostProc
    Check (WaitPort 9090) "宿主(9090) 就绪" "宿主未监听 9090"

    # ---------- 2. 路由共存 ----------
    Step "宿主业务路由与挂载路由共存"
    $orders = Invoke-RestMethod "$Base/api/orders"
    Check ($orders.orders.Count -eq 2 -and $orders.orders[0] -eq "o-1001") "GET /api/orders（宿主业务）-> $($orders.orders -join ',')" "宿主业务路由异常"

    $h = Invoke-RestMethod "$Base/agentflow/health"
    Check ($h.status -eq "ok") "GET /agentflow/health -> ok" "挂载 health 异常: $($h.status)"

    # ---------- 3. 挂载路径下的完整任务链路 ----------
    Step "注册 Agent + 提交任务（全部走 /agentflow/api/v1）"
    $agent = Invoke-RestMethod -Method Post "$Base/agentflow/api/v1/agents" -ContentType "application/json" -Body (@{
        name = "m4-embed"; type = "chat"
        runtime = @{ type = "python-http"; host = "http://localhost:8081" }
    } | ConvertTo-Json -Depth 5)
    Check ($agent.id -and $agent.version -eq 1) "POST agents -> $($agent.id)" "注册失败"

    $task = Invoke-RestMethod -Method Post "$Base/agentflow/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $agent.id; payload = @{ q = "嵌入模式" } } | ConvertTo-Json)
    Check ($task.status -eq "pending") "提交 $($task.id)" "提交失败"

    Step "SSE 透传（挂载路径）"
    $raw = curl.exe -s -N "$Base/agentflow/api/v1/tasks/$($task.id)/stream"
    $lines = $raw -split "`n" | Where-Object { $_ -match '^data: ' } | ForEach-Object { $_.Substring(6) }
    $tokens = @($lines | Where-Object { $_ -match '"type":"token"' })
    Check ($tokens.Count -ge 10) "token x$($tokens.Count) 按序到达" "token 数异常: $($tokens.Count)"

    Step "终态与 usage"
    $final = $null
    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-Date) -lt $deadline) {
        $t = Invoke-RestMethod "$Base/agentflow/api/v1/tasks/$($task.id)"
        if ($t.status -in @("succeeded", "failed", "timeout", "cancelled")) { $final = $t; break }
        Start-Sleep -Milliseconds 200
    }
    Check ($final.status -eq "succeeded" -and $final.usage.prompt_tokens -eq 42 -and $final.usage.completion_tokens -eq 33) "succeeded, usage=42/33, attempt=$($final.attempt_count)" "终态异常: $($final.status) $($final.error | ConvertTo-Json -Compress)"
}
finally {
    Step "清理"
    foreach ($p in $procs) { try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch {} }
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "ALL PASS: M4 嵌入形态冒烟通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
