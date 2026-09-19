# M1 冒烟测试：覆盖 11 个端点的核心行为
# 用法: ./scripts/smoke-m1.ps1 [-Server http://localhost:8080]
param([string]$Server = "http://localhost:18080")
$ErrorActionPreference = "Stop"
# PS5.1 的 Invoke-RestMethod 走系统代理，会被本地代理软件拦截 localhost；直接禁用
[System.Net.WebRequest]::DefaultWebProxy = $null
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}
function WaitStatus($id, $want, $timeoutSec = 10) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $t = Invoke-RestMethod "$Server/api/v1/tasks/$id"
        if ($t.status -eq $want) { return $t }
        Start-Sleep -Milliseconds 100
    }
    return Invoke-RestMethod "$Server/api/v1/tasks/$id"
}

# ---------- 健康检查 ----------
Step "健康检查"
$h = Invoke-RestMethod "$Server/health"
Check ($h.status -eq "ok") "GET /health -> ok" "health 异常: $($h.status)"

# ---------- Agent CRUD + 乐观锁 ----------
Step "创建 Agent"
$agentBody = @{ name = "demo"; type = "chat"; runtime = @{ type = "python-http"; host = "http://localhost:8081" } }
$agent = Invoke-RestMethod -Method Post "$Server/api/v1/agents" -ContentType "application/json" -Body ($agentBody | ConvertTo-Json -Depth 5)
Check ($agent.id -and $agent.version -eq 1) "POST agents -> $($agent.id) v1" "创建失败"

Step "乐观锁: 正确 base -> v2"
$upd = @{ name = "demo-v2"; type = "chat"; runtime = $agentBody.runtime; base_version = 1 } | ConvertTo-Json -Depth 5
$v2 = Invoke-RestMethod -Method Put "$Server/api/v1/agents/$($agent.id)" -ContentType "application/json" -Body $upd
Check ($v2.version -eq 2) "PUT -> v2" "更新版本错误: v$($v2.version)"

Step "乐观锁: 过期 base -> 409"
$code = 0
try { Invoke-RestMethod -Method Put "$Server/api/v1/agents/$($agent.id)" -ContentType "application/json" -Body $upd | Out-Null }
catch { $code = [int]$_.Exception.Response.StatusCode }
Check ($code -eq 409) "PUT 过期 base -> 409" "期望 409 实得 $code"

Step "版本历史"
$vs = Invoke-RestMethod "$Server/api/v1/agents/$($agent.id)/versions"
Check ($vs.Count -eq 2 -and $vs[0].version -eq 1 -and $vs[1].version -eq 2) "versions -> [v1,v2]" "历史异常: $($vs | ForEach-Object version)"

# ---------- 任务：提交 / 幂等 / 流 / 终态 ----------
Step "提交任务（幂等键）"
$submitBody = @{ agent_id = $agent.id; payload = @{ q = "hello" } } | ConvertTo-Json
$task = Invoke-RestMethod -Method Post "$Server/api/v1/tasks" -Headers @{ "Idempotency-Key" = "smoke-key-1" } -ContentType "application/json" -Body $submitBody
Check ($task.status -eq "pending" -and $task.agent_version -eq 2) "POST tasks -> $($task.id) 锁 v2" "提交异常"

Step "重复提交同幂等键 -> 同一任务"
$task2 = Invoke-RestMethod -Method Post "$Server/api/v1/tasks" -Headers @{ "Idempotency-Key" = "smoke-key-1" } -ContentType "application/json" -Body $submitBody
Check ($task2.id -eq $task.id) "幂等命中 $($task.id)" "幂等失效: $($task.id) vs $($task2.id)"

Step "SSE 透传（历史回放 + [DONE]）"
$raw = curl.exe -s -N "$Server/api/v1/tasks/$($task.id)/stream"
$lines = $raw -split "`n" | Where-Object { $_ -match '^data: ' } | ForEach-Object { $_.Substring(6) }
$tokens = @($lines | Where-Object { $_ -match '"type":"token"' })
$hasDone = [bool]($lines | Where-Object { $_ -like '*"type":"done"*' })
$hasSentinel = $lines -contains '[DONE]'
Check ($tokens.Count -ge 10) "token x$($tokens.Count) 按序到达" "token 数异常: $($tokens.Count)"
Check ($hasDone -and $hasSentinel) "done 事件 + [DONE] 结束" "流终止异常"

Step "终态与 usage 归集"
$final = WaitStatus $task.id "succeeded"
Check ($final.status -eq "succeeded" -and $final.usage.prompt_tokens -eq 42 -and $final.usage.completion_tokens -eq 33) "succeeded, usage=42/33, model=$($final.usage.model)" "终态异常: $($final.status) $($final.error | ConvertTo-Json -Compress)"

# ---------- 错误路径 ----------
Step "错误分类: 连不上的 Agent -> AGENT_UNREACHABLE"
$bad = Invoke-RestMethod -Method Post "$Server/api/v1/agents" -ContentType "application/json" -Body (@{ name = "bad"; type = "chat"; runtime = @{ type = "python-http"; host = "http://127.0.0.1:9" } } | ConvertTo-Json -Depth 5)
$badTask = Invoke-RestMethod -Method Post "$Server/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $bad.id; payload = @{} } | ConvertTo-Json)
# AGENT_UNREACHABLE 可重试：3 次退避 1+4+9=14s+，窗口给足 30s
$badFinal = WaitStatus $badTask.id "failed" 30
Check ($badFinal.status -eq "failed" -and $badFinal.error.code -eq "AGENT_UNREACHABLE") "failed / AGENT_UNREACHABLE" "错误路径异常: $($badFinal.status) $($badFinal.error.code)"

# ---------- 取消路径 ----------
Step "取消运行中任务"
$ct = Invoke-RestMethod -Method Post "$Server/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $agent.id; payload = @{} } | ConvertTo-Json)
Start-Sleep -Milliseconds 300   # 等 running（token 流约 1.7s，窗口足够）
$cancelled = Invoke-RestMethod -Method Delete "$Server/api/v1/tasks/$($ct.id)"
Check ($cancelled.status -eq "cancelled") "DELETE -> cancelled" "取消异常: $($cancelled.status)"
$code = 0
try { Invoke-RestMethod -Method Delete "$Server/api/v1/tasks/$($ct.id)" | Out-Null }
catch { $code = [int]$_.Exception.Response.StatusCode }
Check ($code -eq 409) "终态再删 -> 409" "期望 409 实得 $code"

# ---------- 软删除 ----------
Step "软删除 Agent"
Invoke-RestMethod -Method Delete "$Server/api/v1/agents/$($bad.id)" | Out-Null
$code = 0
try { Invoke-RestMethod "$Server/api/v1/agents/$($bad.id)" | Out-Null }
catch { $code = [int]$_.Exception.Response.StatusCode }
Check ($code -eq 404) "删除后 Get -> 404" "期望 404 实得 $code"
$postDel = $null; $code = 0
try { $postDel = Invoke-RestMethod -Method Post "$Server/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $bad.id; payload = @{} } | ConvertTo-Json) }
catch { $code = [int]$_.Exception.Response.StatusCode }
Check ($code -eq 404) "删除后提交 -> 404 拒绝" "期望 404 实得 $code"

# ---------- 汇总 ----------
Write-Host "`n========== 结果 ==========" -ForegroundColor Yellow
if ($script:Failures -eq 0) { Write-Host "全部通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
