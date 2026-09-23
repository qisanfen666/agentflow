# M6 冒烟测试：治理三层防线——认证授权 / 提交守门（限流+预算）/ 审批闸门
#   1) 认证三态：无 key 401、reader 越权 403、admin 放行
#   2) 审批流：高危 Agent 任务待审不执行 -> approve 放行至终态；reject -> cancelled
#   3) 成本护栏：预算耗尽后提交被拒（BUDGET_EXCEEDED）
#   4) 限流：令牌耗尽后提交被拒（RATE_LIMITED + Retry-After）
# 前置: python 3 可用（审批放行的任务真实执行）
# 用法: ./scripts/smoke-m6.ps1
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$ServerBase = "http://localhost:18097"
$Exe = "$PSScriptRoot\..\tmp\agentflow-server.exe"
$AdminKey = "sk-admin"; $SubKey = "sk-sub"; $ReadKey = "sk-read"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}
function WaitPort($port, $timeoutSec = 10) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $c = New-Object System.Net.Sockets.TcpClient
        try { $c.Connect("127.0.0.1", $port); return $true } catch { Start-Sleep -Milliseconds 200 }
        finally { $c.Close() }
    }
    return $false
}
function Send($method, $path, $body = $null, $key = $AdminKey) {
    $h = @{ "X-API-Key" = $key }
    if ($method -eq "GET" -and $key -eq "") { $h = @{} }
    if ($body -ne $null) {
        return Invoke-WebRequest -Method $method "$ServerBase$path" -Headers $h -ContentType "application/json" -Body ($body | ConvertTo-Json -Depth 6) -UseBasicParsing
    }
    return Invoke-WebRequest -Method $method "$ServerBase$path" -Headers $h -UseBasicParsing
}
function TrySend($method, $path, $body = $null, $key = $AdminKey) {
    try { return Send $method $path $body $key } catch { return $_.Exception.Response }
}
function WaitFinal($id, $timeoutSec = 20) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $t = (Invoke-RestMethod "$ServerBase/api/v1/tasks/$id" -Headers @{ "X-API-Key" = $AdminKey })
        if ($t.status -in @("succeeded", "failed", "timeout", "cancelled")) { return $t }
        Start-Sleep -Milliseconds 200
    }
    return $t
}

Step "环境准备"
go build -o $Exe ./cmd/agentflow-server
if ($LASTEXITCODE -ne 0) { throw "构建失败" }
Check $true "二进制就绪" "不应到达"

$procs = @()
try {
    # ---------- 1. 启动（治理全开：认证 + 限流 4/min + 预算 100 token） ----------
    Step "启动 agent + server(:18097, auth=on, rate=4/min, budget=100)"
    $procs += Start-Process python -ArgumentList "$PSScriptRoot\..\examples\python-agent\agent.py" -PassThru -WindowStyle Hidden
    Check (WaitPort 8081) "agent(8081) 就绪" "agent 未监听"

    $env:AGENTFLOW_ADDR = ":18097"
    $env:AGENTFLOW_AUTH_ENABLED = "1"
    $env:AGENTFLOW_API_KEYS = "$AdminKey`:admin;$SubKey`:submitter;$ReadKey`:reader"
    $env:AGENTFLOW_RATE_LIMIT_PER_MIN = "4"
    $env:AGENTFLOW_TOKEN_BUDGET = "100"
    $procs += Start-Process $Exe -PassThru -WindowStyle Hidden
    Check (WaitPort 18097) "server(18097) 就绪" "server 未监听"

    # ---------- 2. 认证三态 ----------
    Step "认证授权"
    $r = TrySend "POST" "/api/v1/agents" @{ name = "x"; type = "chat"; runtime = @{ type = "python-http"; host = "http://localhost:1" } } ""
    Check ([int]$r.StatusCode -eq 401) "无 key -> 401" "无 key 期望 401 实得 $($r.StatusCode)"
    $r = TrySend "POST" "/api/v1/agents" @{ name = "x"; type = "chat"; runtime = @{ type = "python-http"; host = "http://localhost:1" } } $ReadKey
    Check ([int]$r.StatusCode -eq 403) "reader 建 Agent -> 403" "期望 403 实得 $($r.StatusCode)"

    $dangerBody = @{ name = "m6-danger"; type = "chat"; require_approval = $true; runtime = @{ type = "python-http"; host = "http://localhost:8081" } }
    $goodBody = @{ name = "m6-good"; type = "chat"; runtime = @{ type = "python-http"; host = "http://localhost:8081" } }
    $danger = (Send "POST" "/api/v1/agents" $dangerBody).Content | ConvertFrom-Json
    $good = (Send "POST" "/api/v1/agents" $goodBody).Content | ConvertFrom-Json
    Check ($danger.id -and $danger.require_approval -eq $true) "admin 建高危 Agent（require_approval=true）" "高危 Agent 创建失败"
    Check ($good.id -and -not $good.require_approval) "admin 建普通 Agent" "普通 Agent 创建失败"

    # ---------- 3. 审批流 ----------
    Step "审批闸门：待审不执行 -> approve 放行"
    $taskBody = @{ agent_id = $danger.id; payload = @{ q = "高危操作" } }
    $t1 = (Send "POST" "/api/v1/tasks" $taskBody $SubKey).Content | ConvertFrom-Json   # 提交 #1
    Check ($t1.status -eq "pending_approval") "提交高危任务 -> pending_approval（不入队）" "状态异常: $($t1.status)"
    Start-Sleep -Milliseconds 500
    $cur = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($t1.id)" -Headers @{ "X-API-Key" = $AdminKey }
    Check ($cur.status -eq "pending_approval") "待审 500ms 后仍未执行" "未审先执行: $($cur.status)"

    $r = TrySend "POST" "/api/v1/tasks/$($t1.id)/approve" $null $SubKey
    Check ([int]$r.StatusCode -eq 403) "submitter 审批 -> 403（审批权归 admin）" "期望 403 实得 $($r.StatusCode)"
    Send "POST" "/api/v1/tasks/$($t1.id)/approve" $null $AdminKey | Out-Null
    $f1 = WaitFinal $t1.id
    Check ($f1.status -eq "succeeded") "approve 放行 -> succeeded（消耗 75 token）" "放行后终态: $($f1.status)"

    Step "驳回与重复审批"
    $t2 = (Send "POST" "/api/v1/tasks" $taskBody $SubKey).Content | ConvertFrom-Json   # 提交 #2
    Send "POST" "/api/v1/tasks/$($t2.id)/reject" $null $AdminKey | Out-Null
    $f2 = WaitFinal $t2.id
    Check ($f2.status -eq "cancelled") "reject -> cancelled 终态" "驳回后状态: $($f2.status)"
    $r = TrySend "POST" "/api/v1/tasks/$($t2.id)/approve" $null $AdminKey
    Check ([int]$r.StatusCode -eq 409) "驳回后再审 -> 409" "期望 409 实得 $($r.StatusCode)"

    # ---------- 4. 成本护栏 ----------
    Step "成本护栏：预算 100，已耗 75"
    $t3 = (Send "POST" "/api/v1/tasks" @{ agent_id = $good.id; payload = @{ q = "正常" } } $SubKey).Content | ConvertFrom-Json  # 提交 #3
    $f3 = WaitFinal $t3.id
    Check ($f3.status -eq "succeeded") "普通任务执行（累计 150 > 预算 100）" "执行终态: $($f3.status)"

    $err4 = $null
    try { Send "POST" "/api/v1/tasks" @{ agent_id = $good.id; payload = @{ q = "超预算" } } $SubKey | Out-Null }  # 提交 #4
    catch { $err4 = $_ }
    $body4 = if ($err4) { $err4.ErrorDetails.Message } else { "" }
    Check ($body4 -match "BUDGET_EXCEEDED") "预算耗尽提交 -> 429 BUDGET_EXCEEDED" "响应: $body4"

    # ---------- 5. 限流 ----------
    Step "限流：4/min 已满"
    $err5 = $null
    try { Send "POST" "/api/v1/tasks" @{ agent_id = $good.id; payload = @{ q = "超速率" } } $SubKey | Out-Null }  # 提交 #5
    catch { $err5 = $_ }
    $body5 = if ($err5) { $err5.ErrorDetails.Message } else { "" }
    $ra = if ($err5) { $err5.Exception.Response.Headers["Retry-After"] } else { "" }
    Check ($body5 -match "RATE_LIMITED") "令牌耗尽提交 -> 429 RATE_LIMITED" "响应: $body5"
    Check ($ra -and [int]$ra -gt 0) "Retry-After = $ra 秒" "Retry-After 缺失"
}
finally {
    Step "清理"
    foreach ($p in $procs) { try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch {} }
    Remove-Item Env:AGENTFLOW_ADDR, Env:AGENTFLOW_AUTH_ENABLED, Env:AGENTFLOW_API_KEYS, Env:AGENTFLOW_RATE_LIMIT_PER_MIN, Env:AGENTFLOW_TOKEN_BUDGET -ErrorAction SilentlyContinue
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "ALL PASS: M6 治理三层防线冒烟通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
