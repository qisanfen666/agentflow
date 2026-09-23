# agentflow

English | [简体中文](README.md)

**agentflow** is a control-plane library in Go for LLM/Agent workloads. It consolidates **scheduling, queueing, sandboxing, auditing, and governance** into an importable infrastructure package. Inspired by the Kubernetes layering model, the control plane owns lifecycle and routing — it does not provide AI capabilities itself.

## Scope

**What it provides**

- Agent registration and versioning (optimistic locking, soft delete, version-pinned execution)
- Task submission, state-machine-driven execution, and reliable queues (at-least-once semantics)
- Runtime routing: in-process direct connection (python-http) and ephemeral Docker sandbox containers
- SSE output passthrough (reconnect replays history)
- Tool registry (JSON Schema validation, MCP-format export)
- Observability: append-only audit log, Prometheus metrics, OTel distributed tracing
- Governance: API Key authentication/authorization, rate limiting, token budgets, approval workflow

**What it does not provide**: model inference, prompt orchestration, or agent business logic. The execution plane is your own process — implement an [HTTP endpoint + SSE protocol](docs/protocol/sse.md) and you are integrated.

## Quick Start

### As a library: embed into an existing gin application

```go
panel, err := agentflow.New(agentflow.Config{
    Storage: agentflow.StorageConfig{
        Driver: "redis", // or "memory"
        Redis:  agentflow.RedisConfig{Addr: "localhost:6380"},
    },
    Runtimes: []string{"python-http", "docker"},
})
if err != nil { log.Fatal(err) }

panel.Mount(r.Group("/agentflow")) // mount under a host subpath; empty Server.Addr disables self-listening
panel.Start(ctx)                    // starts background components (worker / delayed mover)
defer panel.Stop(ctx)               // idempotent, ordered shutdown
```

See [examples/embed](examples/embed/main.go) for a complete runnable example.

### Standalone deployment

```powershell
go build -o agentflow-server.exe ./cmd/agentflow-server
$env:AGENTFLOW_MODE = "redis"; $env:AGENTFLOW_ADDR = ":8080"
./agentflow-server.exe
```

Both forms share the same `Panel`; the only difference is who owns the HTTP listener
(whether `Server.Addr` is set).

### Execution-plane (Agent-side) contract

Implement one HTTP endpoint in any language that returns an SSE stream:

```
data: {"type":"token","content":" Hel"}
data: {"type":"usage","prompt_tokens":42,"completion_tokens":33,"model":"demo"}
data: {"type":"done","task_id":"t_9f3k2"}
data: [DONE]
```

Zero-dependency reference implementation: [examples/python-agent/agent.py](examples/python-agent/agent.py).
Protocol details and error taxonomy: [docs/protocol/sse.md](docs/protocol/sse.md), [docs/contracts/](docs/contracts/).

## Architecture

```
Client ──HTTP/SSE──► Panel (facade: New assembles / Start-Stop lifecycle)
                      │
     ┌────────────────┼──────────────────────────────────────────┐
     │ api         REST endpoints + SSE passthrough (mountable)     │
     │ dispatch    state-machine execution, event hub (replay +     │
     │             wake)                                            │
     │ engine      worker + queue: claim/ack/nack, backoff requeue, │
     │             delayed->ready mover, visibility-timeout reclaim │
     │ runtime     python-http (direct) | docker (ephemeral sandbox)│
     │ registry    tool JSON Schema compilation, MCP export         │
     │ policy      pre-submission governance chain (rate/budget)    │
     │ storage     memory | redis — interfaces as contracts,        │
     │             guarded by shared contract tests                 │
     └────────────────┼──────────────────────────────────────────┘
                      ▼ SSE (token-level increments)
              Execution-plane Agent (user process, any language)
```

Main flow: submit (version pin + idempotent claim + governance gate) → enqueue →
worker claims → dispatcher drives pending→running→final → runtime invokes the agent →
token stream passes through the hub in real time → final state persisted + usage aggregated.

## HTTP API

| Method | Path | Description |
|---|---|---|
| GET | /health | Health check (probe-exempt from auth) |
| POST | /api/v1/agents | Create (version=1) |
| GET | /api/v1/agents | List undeleted agents |
| GET | /api/v1/agents/{id} | Current version details |
| PUT | /api/v1/agents/{id} | Update (base_version optimistic lock, creates a new version) |
| DELETE | /api/v1/agents/{id} | Soft delete (in-flight tasks keep running) |
| GET | /api/v1/agents/{id}/versions | Version history (ascending) |
| POST | /api/v1/tasks | Submit (Idempotency-Key, agent_version pinned) |
| GET | /api/v1/tasks/{id} | Status query |
| DELETE | /api/v1/tasks/{id} | Cancel (409 on final states) |
| POST | /api/v1/tasks/{id}/approve | Approve (admin only; enqueues) |
| POST | /api/v1/tasks/{id}/reject | Reject (admin only; → cancelled) |
| GET | /api/v1/tasks/{id}/stream | SSE output subscription (reconnect replays history) |
| POST | /api/v1/tools | Register tool (JSON Schema parameters) |
| GET | /api/v1/tools | List tools |
| POST | /api/v1/tools/validate | Validate arguments against tool schema |
| GET | /api/v1/tools/mcp | Export full MCP tools manifest |
| GET | /api/v1/tools/{id}/mcp | Export single tool in MCP format |

Full definition (request/response schemas): [docs/api/openapi.yaml](docs/api/openapi.yaml).

## Reliability Semantics

- **State machine** ([task-state.md](docs/contracts/task-state.md)): pending →
  pending_approval → running → succeeded/failed/timeout/cancelled. Finality is enforced
  atomically at the storage layer (mutex in memory / WATCH transaction in redis);
  when concurrent cancellation races with execution finalization, first writer wins.
- **Error taxonomy** ([error-taxonomy.md](docs/contracts/error-taxonomy.md)): retryable
  errors (AGENT_UNREACHABLE/AGENT_TIMEOUT) requeue with exponential backoff
  (base×attempt², 3 attempts by default); non-retryable ones go straight to a final state.
- **At-least-once**: tasks flow through ready/processing/delayed lists with in-flight
  leases; crashed consumers are reclaimed via visibility timeout, and running→pending
  normalization self-heals on restart. Tasks survive process restarts.
- **Submission idempotency**: Idempotency-Key atomic claim (SETNX); duplicate submissions
  return the original task.
- **Version pinning**: a task fixes (agent_id, version) so agent updates never affect
  in-flight tasks; updates use base_version optimistic locking (409 on conflict).

## Observability

Three signals, each with an independent switch (zero overhead when disabled):

| Signal | Enable | Carrier and purpose |
|---|---|---|
| **Audit** | `Observability.AuditLogPath` | Append-only JSONL: full-chain action trail (submit/execute/retry/final/approval + Agent/Tool CRUD). Final events carry usage summaries and error codes; `session_id` attribution lives here for ELK-style aggregation |
| **Metrics** | `Observability.MetricsEnabled` | `GET /metrics` (private registry — never pollutes the host's global one): tasks_total{status}, retries_total, tokens_total{direction,model}, task_duration histogram. High-cardinality ids never become labels; they stay in audit details |
| **Tracing** | `Observability.OTLPEndpoint` | OTLP gRPC export (Jaeger/Tempo compatible). Three span layers: API root → task.execute → agent.http; outbound requests carry W3C traceparent so agents can continue the trace. Cross-queue causality: traceparent is persisted with the task record and linked back on the worker side (shown as FollowsFrom in Jaeger) |

One-command observability stack (Prometheus scraping + pre-provisioned Grafana dashboard):

```powershell
docker compose -f deploy/docker-compose.yml up -d
# Grafana http://localhost:3300 (admin/admin) · Prometheus http://localhost:9090
```

Alerting stance: the control plane only produces signals (metrics/audit); notification
channels and alert rules belong to the consuming side (Alertmanager/Grafana Rules) —
no built-in webhook.

## Governance

Three lines of defense, outermost first:

| Line | Mechanism | Rejection semantics |
|---|---|---|
| **AuthN/AuthZ** | X-API-Key + role-based method-level permission matrix (admin full / submitter submit-consume / reader read-only) | 401 unauthenticated / 403 forbidden |
| **Submission gate** | Policy rule chain (K8s-admission-shaped): token-bucket rate limit + daily token budget (debited from actual usage, resets at local midnight) | 429 `RATE_LIMITED` (with Retry-After) / `BUDGET_EXCEEDED` |
| **Approval gate** | `AgentSpec.require_approval` marks high-risk agents: tasks enter `pending_approval` (never enqueued or executed) until an admin approves or rejects | Unapproved tasks are invisible to the execution plane; approval authority never belongs to the caller |

Risk-decision authority stays on the management side (agent specs change through
optimistic locking with audit trails), never delegated to callers. In embedded form the
auth layer is fully replaceable: inject your own middleware via `Dependencies.Auth`.

## Testing and Smoke Tests

```powershell
go test ./...                 # unit + contract + Redis integration (auto-skip if unreachable)
./scripts/smoke-m1.ps1        # full endpoint behavior (python3 + server instance)
./scripts/smoke-m2.ps1        # kill -9 → data survives in Redis → resumes after restart (docker redis)
./scripts/smoke-m3.ps1        # dual-runtime routing + zero container residue + MCP (docker)
./scripts/smoke-m4.ps1        # library form embedding: host routes coexist with mounted plane (python3)
./scripts/smoke-m5.ps1        # observability loop: metrics into Prometheus + dashboard in Grafana (docker)
./scripts/smoke-m6.ps1        # governance: auth/approval/budget/rate-limit (python3)
```

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| Server.Addr | empty (no listen) | Listen address; empty = embedded mode, routing owned by host |
| Server.Mode | empty | gin mode (debug/release/test) |
| Storage.Driver | memory | memory \| redis |
| Storage.Redis | localhost:6379 | Redis connection (when Driver=redis) |
| Queue.MaxRetries | 3 | Max attempts for retryable errors |
| Queue.RetryBackoffBaseMs | 1000 | Backoff base (delay = base × attempt²) |
| Queue.VisibilityTimeoutSec | 300 | In-flight lease / visibility timeout |
| Runtimes | [python-http] | Enabled runtimes; unknown/duplicate → error |
| Sandbox | — | Resource limits for the docker runtime (read-only rootfs/memory/CPU) |
| Observability.AuditLogPath | empty (off) | Audit JSONL path |
| Observability.MetricsEnabled | false | Mount GET /metrics |
| Observability.OTLPEndpoint | empty (off) | OTLP gRPC address (e.g. localhost:4317) |
| Observability.ServiceName | agentflow | OTel service.name |
| Auth.Enabled | false | Enable API Key authn/authz |
| Auth.Keys | — | `[{key, roles}]`, roles: admin/submitter/reader |
| Governance.RateLimitPerMin | 0 (unlimited) | Global submission rate limit per minute |
| Governance.DailyTokenBudget | 0 (unlimited) | Daily token budget (debited from actual usage) |

## Repository Layout

```
agent.go                 Panel facade (public API)
config.go                Config definition and defaults
model/                   Domain models: AgentSpec/Task state machine/error codes/ToolDef
storage/                 Storage interfaces + memory/redis implementations + contract tests
runtime/                 python-http / docker runtimes (shared SSE core)
internal/api/            REST endpoints + SSE passthrough + authn/authz
internal/dispatch/       Execution orchestration + event hub
internal/engine/         Worker + in-memory/Redis reliable queues
internal/registry/       Tool schema compilation + MCP export
internal/policy/         Governance rule chain (rate limit / budget)
internal/observability/  Audit / metrics / tracing
cmd/agentflow-server/    Standalone binary
examples/                embed (embedding example), python-agent (zero-dependency execution plane)
docs/                    Contract documents, SSE protocol, OpenAPI
deploy/                  Observability stack (Prometheus + Grafana)
scripts/                 Smoke tests per acceptance scenario
```
