# M3 冒烟测试：双 Runtime 共存路由 + Tool Registry/MCP
#   1) python-http Agent（宿主进程）与 docker 沙箱 Agent（一次性容器）在同一 server 各跑一任务
#   2) 工具注册全流程：合法注册 / 名字重复 409 / schema 非法 400 / MCP 单个+全量导出
# 前置: 1) Docker Desktop 运行中
#       2) 沙箱镜像已构建：docker build -t agentflow-sandbox:latest -f deploy/Dockerfile.agent-sandbox .
#       3) python 3 可用
# 用法: ./scripts/smoke-m3.ps1
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$ServerBase = "http://localhost:18095"
$Exe = "$PSScriptRoot\..\tmp\agentflow-server.exe"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}

Step "环境准备（构建 + 确认沙箱镜像）"
New-Item -ItemType Directory -Force "$PSScriptRoot\..\tmp" | Out-Null
go build -o $Exe ./cmd/agentflow-server
if ($LASTEXITCODE -ne 0) { throw "构建失败" }
$img = docker image inspect agentflow-sandbox:latest --format "{{.Id}}" 2>$null
if (-not $img) { throw "缺沙箱镜像：先 docker build -t agentflow-sandbox:latest -f deploy/Dockerfile.agent-sandbox ." }
Check $true "二进制 + 镜像就绪" "不应到达"

$procs = @()
try {
    # ---------- 1. 双 Agent 环境 ----------
    Step "启动宿主 python agent + server(memory 模式, 双 Runtime)"
    $agentProc = Start-Process python -ArgumentList "$PSScriptRoot\..\examples\python-agent\agent.py" -PassThru -WindowStyle Hidden
    $procs += $agentProc

    $env:AGENTFLOW_MODE = "memory"
    $env:AGENTFLOW_ADDR = ":18095"
    $serverProc = Start-Process $Exe -PassThru -WindowStyle Hidden
    $procs += $serverProc

    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        try { $null = Invoke-RestMethod "$ServerBase/health" -TimeoutSec 1; break } catch { Start-Sleep -Milliseconds 200 }
    }
    Check ((Invoke-RestMethod "$ServerBase/health").status -eq "ok") "server 就绪" "server 未启动"

    # ---------- 2. python-http 路由 ----------
    Step "注册 python-http Agent 并跑任务"
    $a1 = Invoke-RestMethod -Method Post "$ServerBase/api/v1/agents" -ContentType "application/json" -Body (@{
        name = "m3-direct"; type = "chat"
        runtime = @{ type = "python-http"; host = "http://localhost:8081" }
    } | ConvertTo-Json -Depth 5)
    $t1 = Invoke-RestMethod -Method Post "$ServerBase/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $a1.id; payload = @{ q = "直连模式" } } | ConvertTo-Json)
    $f1 = $null
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $cur = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($t1.id)"
        if ($cur.status -in @("succeeded", "failed", "timeout", "cancelled")) { $f1 = $cur; break }
        Start-Sleep -Milliseconds 300
    }
    Check ($f1.status -eq "succeeded") "python-http 路由: $($f1.status) attempt=$($f1.attempt_count)" "python-http 路由失败: $($f1.status)"

    # ---------- 3. docker 沙箱路由（同 server 内切换执行环境） ----------
    Step "注册 docker 沙箱 Agent 并跑任务（一次性容器生命周期）"
    $a2 = Invoke-RestMethod -Method Post "$ServerBase/api/v1/agents" -ContentType "application/json" -Body (@{
        name = "m3-sandbox"; type = "chat"
        runtime = @{ type = "docker" }
    } | ConvertTo-Json -Depth 5)
    Check ($a2.runtime.type -eq "docker") "docker Agent 注册成功（image 走默认）" "注册失败"
    $t2 = Invoke-RestMethod -Method Post "$ServerBase/api/v1/tasks" -ContentType "application/json" -Body (@{ agent_id = $a2.id; payload = @{ q = "沙箱模式" } } | ConvertTo-Json)
    $f2 = $null
    $deadline = (Get-Date).AddSeconds(60)   # 沙箱含拉起+健康等待，放宽
    while ((Get-Date) -lt $deadline) {
        $cur = Invoke-RestMethod "$ServerBase/api/v1/tasks/$($t2.id)"
        if ($cur.status -in @("succeeded", "failed", "timeout", "cancelled")) { $f2 = $cur; break }
        Start-Sleep -Milliseconds 500
    }
    Check ($f2.status -eq "succeeded") "docker 沙箱路由: $($f2.status) attempt=$($f2.attempt_count)" "docker 沙箱路由失败: $($f2.status)"

    Step "容器清理断言（一次性沙箱不残留）"
    $leftover = docker ps -a --filter "name=agentflow-sbx" --format "{{.ID}}"
    Check (-not $leftover) "无残留沙箱容器" "发现残留容器: $leftover"

    # ---------- 4. Tool Registry ----------
    Step "工具注册全流程"
    $toolBody = @{
        name = "search_docs"; description = "检索文档"; timeout_sec = 45
        parameters = @{
            type = "object"
            properties = @{ query = @{ type = "string" } }
            required = @("query")
        }
    } | ConvertTo-Json -Depth 6
    $tool = Invoke-RestMethod -Method Post "$ServerBase/api/v1/tools" -ContentType "application/json" -Body $toolBody
    Check ($tool.name -eq "search_docs") "注册成功 id=$($tool.id) timeout=$($tool.timeout_sec)" "注册失败"

    try {
        Invoke-RestMethod -Method Post "$ServerBase/api/v1/tools" -ContentType "application/json" -Body $toolBody | Out-Null
        Check $false "不应到达" "重复注册未拒绝"
    } catch { Check ($_.Exception.Response.StatusCode.value__ -eq 409) "名字重复 -> 409" "重复注册状态码异常" }

    $badBody = @{ name = "bad_tool"; parameters = @{ type = "nonsense" } } | ConvertTo-Json -Depth 6
    try {
        Invoke-RestMethod -Method Post "$ServerBase/api/v1/tools" -ContentType "application/json" -Body $badBody | Out-Null
        Check $false "不应到达" "非法 schema 未拒绝"
    } catch { Check ($_.Exception.Response.StatusCode.value__ -eq 400) "schema 非法 -> 400" "非法 schema 状态码异常" }

    # ---------- 5. MCP 导出 ----------
    Step "MCP 标准格式导出"
    $mcpOne = Invoke-RestMethod "$ServerBase/api/v1/tools/$($tool.id)/mcp"
    $mcpAll = Invoke-RestMethod "$ServerBase/api/v1/tools/mcp"
    Check ($mcpOne.type -eq "function" -and $mcpOne.function.name -eq "search_docs") "单个导出: type=function name=search_docs" "单个导出形态错误"
    Check ($mcpAll.tools.Count -ge 1 -and $mcpAll.tools[0].function.name) "全量导出: $($mcpAll.tools.Count) 个工具" "全量导出为空"
}
finally {
    Step "清理"
    foreach ($p in $procs) { try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force } } catch {} }
    Remove-Item Env:AGENTFLOW_MODE, Env:AGENTFLOW_ADDR -ErrorAction SilentlyContinue
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "ALL PASS: M3 双 Runtime + Tool Registry/MCP 冒烟通过" -ForegroundColor Green; exit 0 }
else { Write-Host "$($script:Failures) 项失败" -ForegroundColor Red; exit 1 }
