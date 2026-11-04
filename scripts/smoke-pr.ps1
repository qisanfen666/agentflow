# smoke-pr.ps1 -- PR Review demo end-to-end smoke (story stages 0-4).
#
# Covers: bootstrap agents -> CI webhook (idempotent) -> mock claude container ->
# SSE replay (token/tool_call/[DONE]) -> writeback conclusion -> prod-repo approval
# gate -> audit session attribution -> metrics.
#
# Requires: docker (mock image), git (test repos). No API key needed (mock mode).
# Usage: ./scripts/smoke-pr.ps1   (run from repo root)
$ErrorActionPreference = "Stop"
[System.Net.WebRequest]::DefaultWebProxy = $null

$Base    = "http://localhost:18098"
$CP      = "$Base/agentflow"
$AdminKey = "admin-key"
$DemoDir = "$PSScriptRoot\..\examples\pr-review"
$Audit   = "$PSScriptRoot\..\tmp\pr-audit-smoke.jsonl"
$script:Failures = 0

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Check($cond, $okMsg, $badMsg) {
    if ($cond) { Write-Host "  [PASS] $okMsg" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $badMsg" -ForegroundColor Red; $script:Failures++ }
}
function WaitPort($port, $timeoutSec = 20) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $c = New-Object System.Net.Sockets.TcpClient
        try { $c.Connect("127.0.0.1", $port); return $true } catch { Start-Sleep -Milliseconds 300 }
        finally { $c.Close() }
    }
    return $false
}
function WaitPRFinal($num, $timeoutSec = 60) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $b = Invoke-RestMethod "$Base/api/board"
        $p = $b.prs | Where-Object { $_.number -eq $num }
        if ($p.task_status -in @("succeeded", "failed", "timeout", "cancelled")) { return $p }
        Start-Sleep -Milliseconds 400
    }
    return $p
}
function Webhook($repo, $pr) {
    return Invoke-RestMethod -Method Post "$Base/webhook/pr" -ContentType "application/json" `
        -Body (@{ repo = $repo; pr = $pr; title = "smoke $repo #$pr" } | ConvertTo-Json)
}

# ---------- preflight ----------
Step "preflight: docker daemon"
docker version --format "{{.Server.Version}}" *> $null
if ($LASTEXITCODE -ne 0) { throw "docker daemon unreachable - start Docker Desktop first" }
Check $true "docker ok" "unreachable"

Step "preflight: testdata repos + patches"
powershell -File "$DemoDir\scripts\make-testdata.ps1" | Out-Null
Check (Test-Path "$DemoDir\diffs\pr-101.patch") "patches generated" "patch missing"
Check ((Get-Item "$DemoDir\diffs\pr-101.patch").Length -gt 100) "patch non-empty" "patch empty"

Step "preflight: mock claude image"
docker build -q -f "$DemoDir\docker\Dockerfile.claude-mock" -t agentflow-claude-mock:latest "$DemoDir\docker" | Out-Null
Check ($LASTEXITCODE -eq 0) "image built" "image build failed"

Step "preflight: build midway binary"
go build -o "$PSScriptRoot\..\tmp\pr-midway.exe" ./examples/pr-review
if ($LASTEXITCODE -ne 0) { throw "build failed" }

# ---------- run ----------
$env:AUDIT_PATH = $Audit
$env:PORT = "18098"
$env:DEMO_ROOT = "."
Remove-Item $Audit -ErrorAction SilentlyContinue
$proc = Start-Process "$PSScriptRoot\..\tmp\pr-midway.exe" -WorkingDirectory "$PSScriptRoot\.." -PassThru -WindowStyle Hidden
try {
    Step "stage 0: midway up + control plane mounted"
    Check (WaitPort 18098) "listening :18098" "port never opened"
    $h = Invoke-WebRequest "$Base/healthz" -UseBasicParsing
    Check ($h.StatusCode -eq 200) "midway /healthz" "healthz failed"
    $cpH = Invoke-WebRequest "$CP/health" -UseBasicParsing   # auth-exempt probe
    Check ($cpH.StatusCode -eq 200) "control plane /health" "cp health failed"

    Step "stage 0: three review agents bootstrapped"
    $agents = Invoke-RestMethod "$CP/api/v1/agents" -Headers @{ "X-API-Key" = $AdminKey }
    Check ($agents.Count -eq 3) "3 agents registered" ("agent count = " + $agents.Count)
    $prod = $agents | Where-Object { $_.name -eq "pr-reviewer-prod" }
    Check ($prod.require_approval -eq $true) "prod agent gated by approval" "prod gate missing"

    Step "stage 1: CI webhook -> task submitted (normal repo)"
    $r1 = Webhook "svc-orders" 101
    Check ($r1.task_status -in @("pending", "running", "succeeded")) "task accepted ($($r1.task_status))" "submit failed"
    Check ($r1.session_id -eq "pr-svc-orders-101") "session_id attribution" "session mismatch"

    Step "stage 2: mock claude executes -> succeeded + writeback"
    $p1 = WaitPRFinal 101
    Check ($p1.task_status -eq "succeeded") "task succeeded" ("status = " + $p1.task_status)
    Check ($null -ne $p1.review) "review written back" "no review"
    Check ($p1.review.conclusion -eq "REQUEST_CHANGES") "conclusion=REQUEST_CHANGES" ("conclusion = " + $p1.review.conclusion)
    Check (($p1.review.prompt_tokens + $p1.review.completion_tokens) -gt 0) "usage tokens aggregated" "no usage"

    Step "stage 2: SSE replay has token/tool_call/[DONE]"
    $sse = Invoke-WebRequest "$CP/api/v1/tasks/$($r1.task_id)/stream" -Headers @{ "X-API-Key" = $AdminKey } -UseBasicParsing
    $s = $sse.Content
    Check ($s -match '"type":"token"') "token events" "no token events"
    Check ($s -match '"event":"tool_call"') "tool_call events" "no tool_call events"
    Check ($s -match '\[DONE\]') "stream terminated with [DONE]" "no [DONE]"

    Step "stage 1b: idempotent webhook resend -> same task"
    $r2 = Webhook "svc-orders" 101
    Check ($r2.task_id -eq $r1.task_id) "same task_id returned" ("ids differ: " + $r1.task_id + " vs " + $r2.task_id)

    Step "stage 3: prod repo gated by approval"
    $r3 = Webhook "svc-payments" 201
    Check ($r3.task_status -eq "pending_approval") "pending_approval (not enqueued)" ("status = " + $r3.task_status)
    $ap = Invoke-RestMethod -Method Post "$Base/api/board/prs/201/approve"
    Check ($ap.status -eq "pending") "approve -> pending (enqueued)" ("approve -> " + $ap.status)
    $p3 = WaitPRFinal 201
    Check ($p3.task_status -eq "succeeded") "prod task succeeded after approval" ("status = " + $p3.task_status)
    Check ($null -ne $p3.review) "prod review written back" "no prod review"

    Step "stage 4: audit carries session attribution + usage"
    $auditText = Get-Content $Audit -Raw -ErrorAction SilentlyContinue
    Check ($auditText -match "pr-svc-orders-101") "audit rows for session pr-svc-orders-101" "session missing in audit"
    Check ($auditText -match '"prompt_tokens"') "audit carries usage" "usage missing in audit"

    Step "stage 4: metrics exposed (tokens_total)"
    $m = Invoke-WebRequest "$Base/api/metrics" -UseBasicParsing
    Check ($m.Content -match "tokens_total") "tokens_total present" "metrics missing tokens_total"
} finally {
    Step "cleanup"
    if ($proc -and -not $proc.HasExited) { Stop-Process $proc.Id -Force; "server stopped" }
}

Write-Host ""
if ($script:Failures -eq 0) { Write-Host "SMOKE-PR: ALL PASS" -ForegroundColor Green; exit 0 }
Write-Host "SMOKE-PR: $script:Failures FAILURES" -ForegroundColor Red; exit 1
