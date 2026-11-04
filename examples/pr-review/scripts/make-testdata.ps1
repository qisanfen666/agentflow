# make-testdata.ps1 -- initialize testdata/ repo snapshots as real git repos and
# generate PR diffs (idempotent: re-runs reset .git and regenerate).
#
# This script doubles as the CI-side command reference: a real CI runner
# (see cicd/github-actions-pr-review.yml.example) does the same thing --
# check out the PR branch, `git diff` the patch, call the platform webhook.
#
# Run from repo root: powershell -File examples/pr-review/scripts/make-testdata.ps1
param(
    [string]$Root = "$PSScriptRoot\.."
)

$ErrorActionPreference = "Stop"
$repos = @(
    @{ Name = "svc-orders";   PRDir = "pr-101"; Branch = "pr-101-orders-query" },
    @{ Name = "svc-payments"; PRDir = "pr-201"; Branch = "pr-201-charge" }
)

New-Item -ItemType Directory -Force -Path "$Root\diffs" | Out-Null

foreach ($repo in $repos) {
    $dir = Join-Path $Root "testdata\$($repo.Name)"
    Write-Host "==> $($repo.Name)"

    if (Test-Path "$dir\.git") { Remove-Item -Recurse -Force "$dir\.git" }

    # idempotency: strip PR files previously copied to the repo root (from an
    # earlier run), so the base commit contains only the base snapshot
    Get-ChildItem "$dir\$($repo.PRDir)" -File | ForEach-Object {
        Remove-Item (Join-Path $dir $_.Name) -Force -ErrorAction SilentlyContinue
    }

    git -C $dir init -q -b main
    git -C $dir -c user.name=demo -c user.email=demo@local add .
    git -C $dir -c user.name=demo -c user.email=demo@local commit -q -m "base"

    # copy PR files into repo root (simulate the PR branch changes)
    git -C $dir checkout -q -b $repo.Branch
    Copy-Item "$dir\$($repo.PRDir)\*" $dir -Force
    git -C $dir -c user.name=demo -c user.email=demo@local add .
    git -C $dir -c user.name=demo -c user.email=demo@local commit -q -m "$($repo.Branch)"

    # diff = base..PR ; stay on the PR branch (workspace = PR checkout, read-only mounted for claude)
    $patch = Join-Path $Root "diffs\$($repo.PRDir).patch"
    git -C $dir diff "main..$($repo.Branch)" | Out-File -Encoding utf8 $patch
    Write-Host "    diff -> $patch"
}

Write-Host ""
Write-Host "OK: test repos ready (workspaces on PR branches, patches under diffs/)"
