﻿# M2 冒烟测试：持久化 + 可靠队列的招牌场景
#   提交任务 -> 强杀 server -> 断言任务还在 Redis -> 重启 -> 任务照样执行成功
# 前置: 1) docker Desktop 运行中，shortlink-redis 容器映射 6380（共享实例，DB 0，
#          脚本只删 agentflow:* 前缀键，不动其他项目数据）
#       2) python 3 可用（跑 examples/python-agent/agent.py）
# 用法: ./scripts/smoke-m2.ps1
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$RedisContainer = "shortlink-redis"
$RedisDB = 0
$ServerBase = "http://localhost:18090"
$Exe = "$PSScriptRoot\..\tmp\agentflow-server.exe"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}

# ---------- 0. 环境 ----------
Step "环境准备（清理 agentflow:* 键 + 构建二进制）"
$keys = docker exec $RedisContainer redis-cli -n $RedisDB --scan --pattern "agentflow:*"
if ($keys) { docker exec $RedisContainer redis-cli -n $RedisDB DEL @keys | Out-Null }
New-Item -ItemType Directory -Force "$PSScriptRoot\..\tmp" | Out-Null
go build -o $Exe ./cmd/agentflow-server
if ($LASTEXITCODE -ne 0) { throw "构建失败" }
Check $true "二进制就绪 + Redis(6380 DB$RedisDB) 已清 agentflow 键" "不应到达"

$procs = @()
try {
    # ---------- 1. 起执行面 + 控制面（redis 模式，可见性 3s 便于演示回收） ----------
    Step "启动 python agent + server(redis 模式)"
    $agentProc = Start-Process python -ArgumentList "$PSScriptRoot\..\examples\python-agent\agent.py" -PassThru -WindowStyle Hidden
    $procs += $agentProc

    $env:AGENTFLOW_MODE = "redis"
    $env:AGENTFLOW_ADDR = ":18090"
    $env:AGENTFLOW_REDIS_ADDR = "localhost:6380"
    $env:AGENTFLOW_REDIS_DB = "$RedisDB"
    $env:AGENTFLOW_VISIBILITY_SEC = "3"
    $serverProc = Start-Process $Exe -PassThru -WindowStyle Hidden
    $procs += $serverProc

    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        try { $null = Invoke-RestMethod "$ServerBase/health" -TimeoutSec 1; break } catch { Start-Sleep -Milliseconds 200 }
    }
    $h = Invoke-RestMethod "$ServerBase/health"
    Check ($h.status -eq "ok") "server 起来了（pid $($serverProc.Id)）" "server 未就绪"

    # ---------- 2. 注册 + 提交 ----------
    Step "注册 Agent + 提交任务"
    $agentBody = @{ name = "m2-demo"; type = "chat"; runtime = @{ type = "python-http"; host = "http://localhost:8081" } }
    $agent = Invoke-RestMethod -Method Post "$ServerBase/api/v1/agents" -ContentType "application/json" -Body ($agentBody | ConvertTo-Json -Depth 5)
    $task = Invoke-RestMethod -Method Post "$ServerBase/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $agent.id; payload = @{ q = "重启后见" } } | ConvertTo-Json)
    Check ($task.status -eq "pending") "提交 $($task.id)" "提交失败"

    # ---------- 3. 强杀 server ----------
    Step "任务执行中：强杀 server（模拟进程崩溃）"
    Start-Sleep -Milliseconds 300   # 让 worker 取走任务（大概率 running，租约在）
    Stop-Process -Id $serverProc.Id -Force
    Start-Sleep -Milliseconds 500
    Check ($serverProc.HasExited) "server 已被杀死" "进程未退出"

    # ---------- 4. 断言任务还在 Redis ----------
    Step "断言：任务数据仍在 Redis"
    $taskKeys = docker exec $RedisContainer redis-cli -n $RedisDB --scan --pattern "agentflow:task:$($task.id)*"
    $qLen = docker exec $RedisContainer redis-cli -n $RedisDB HLEN agentflow:q:tasks
    $hasData = ($taskKeys.Count -ge 1) -or ([int]$qLen -ge 1)
    Check $hasData "Redis 中仍有任务数据（task 键: $($taskKeys.Count), 队列正身: $qLen）" "Redis 里找不到任务！持久化失效"

    # ---------- 5. 重启 ----------
    Step "重启 server（世界线收束）"
    $serverProc2 = Start-Process $Exe -PassThru -WindowStyle Hidden
    $procs += $serverProc2
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        try { $null = Invoke-RestMethod "$ServerBase/health" -TimeoutSec 1; break } catch { Start-Sleep -Milliseconds 200 }
    }
    Check $true "server 重启完成（pid $($serverProc2.Id)）" ""

    # ---------- 6. 等任务到终态 ----------
    Step "等待任务执行完成（pending 在队 -> 直接跑；running 挂起 -> 3s 租约过期回收 -> 重跑）"
    $final = $null
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $t = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($task.id)"
        if ($t.status -in @("succeeded", "failed", "timeout", "cancelled")) { $final = $t; break }
        Start-Sleep -Milliseconds 300
    }
    if (-not $final) { $final = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($task.id)" }
    Check ($final.status -eq "succeeded") "重启后任务 -> $($final.status) (attempt=$($final.attempt_count))" "终态 $($final.status)，重启恢复失败"

    Step "usage 归集"
    Check ($final.usage -and $final.usage.completion_tokens -ge 33) "usage: prompt=$($final.usage.prompt_tokens) completion=$($final.usage.completion_tokens) model=$($final.usage.model)" "usage 缺失"
}
finally {
    Step "清理"
    foreach ($p in $procs) { try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch {} }
    Remove-Item Env:AGENTFLOW_MODE, Env:AGENTFLOW_ADDR, Env:AGENTFLOW_REDIS_ADDR, Env:AGENTFLOW_REDIS_DB, Env:AGENTFLOW_VISIBILITY_SEC -ErrorAction SilentlyContinue
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "ALL PASS: M2 持久化+可靠队列冒烟通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
