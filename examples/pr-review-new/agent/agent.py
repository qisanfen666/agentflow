#!/usr/bin/env python3
"""PR Review 裸 Agent：零依赖实现 agentflow SSE 合同（docs/protocol/sse.md）。

验证命题：一个"裸 agent"（基础 LLM 调用 + 基本工具能力）只需实现
POST /run/stream 一个端点，即可被 agentflow 控制面生产化
（排队/审批/审计/预算/直播/回写全部由控制面赋予）。

能力构成：
  - OpenAI 兼容 chat/completions 流式调用（OPENAI_BASE_URL / OPENAI_API_KEY）
  - 工具循环：read_file / write_file（按 agent_config.allowed_tools 过滤；
    路径越界防护：只允许访问 payload.workspace 内）
  - mock 降级：无 key 时内置假 LLM，按 diff 启发式生成评审，全链路仍可演示
  - usage 上报：优先网关真实值，不支持的网关按字符数估算（预算实扣依赖此事件）

启动: python agent.py [端口]  （默认 8091）
配置: 与中台共用 ../config.json（llm 段）；同名环境变量优先；CONFIG 可指定其它路径
真实模式: config.json 填 llm.api_key 即可（或设 OPENAI_API_KEY 环境变量）
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8091

# ---------- 配置：env > config.json(llm 段) > 默认值 ----------

CONFIG_PATH = os.environ.get("CONFIG") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")


def _load_llm_cfg():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return (json.load(f) or {}).get("llm") or {}
    except Exception:
        return {}


_LLM_CFG = _load_llm_cfg()
BASE_URL = (os.environ.get("OPENAI_BASE_URL") or _LLM_CFG.get("base_url")
            or "https://api.openai.com/v1").rstrip("/")
API_KEY = os.environ.get("OPENAI_API_KEY") or _LLM_CFG.get("api_key") or ""
MODEL = os.environ.get("OPENAI_MODEL") or _LLM_CFG.get("model") or "gpt-4o-mini"
MODE = "openai" if API_KEY else "mock"
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT_SEC") or _LLM_CFG.get("timeout_sec") or 180)

try:  # Windows 控制台中文输出
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# ---------- 错误码（对照 docs/contracts/error-taxonomy.md，控制面按此判定重试） ----------

class ToolError(Exception):
    """工具业务错误 -> TOOL_EXEC_FAILED（permanent，不重试）"""


class LLMError(Exception):
    """LLM 网关错误。code: 4xx=AGENT_PROTOCOL_VIOLATION（permanent），5xx/网络=AGENT_INTERNAL（retryable）"""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


# ---------- 系统提示与工具定义 ----------

SYSTEM_PROMPT = """你是资深代码评审员，负责评审 Pull Request。

规则：
1. 只关注真实风险，不泛泛表扬；按 blocker / major / minor 分级输出问题，给出文件与行号依据。
2. 需要上下文时调用 read_file 工具查看仓库源码（相对路径，如 orders.go）；不要臆测未看到的代码。
3. 用 Markdown 输出；全部问题列完后，最后一行必须是总结论，只能是 APPROVE 或 REQUEST_CHANGES 之一。
"""

TOOLS_SPEC = {
    "read_file": {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "读取 PR 工作区内指定路径的文件内容（只读）",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "工作区相对路径，如 orders.go"},
                },
                "required": ["path"],
            },
        },
    },
    "write_file": {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "把修复写入 PR 工作区内指定文件（auto-fixer 用）",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "工作区相对路径"},
                    "content": {"type": "string", "description": "完整文件内容"},
                },
                "required": ["path", "content"],
            },
        },
    },
}


def build_user_prompt(payload):
    parts = ["请评审以下 Pull Request。"]
    for k, label in (("repo", "仓库"), ("pr", "PR"), ("base", "目标分支"), ("title", "标题")):
        v = payload.get(k)
        if v not in (None, ""):
            parts.append(f"{label}: {v}")
    ws = payload.get("workspace", "")
    if ws:
        parts.append(f"工作区目录: {ws}（read_file 以它为根解析相对路径）")
    parts.append("--- DIFF ---")
    parts.append(payload.get("diff", "") or "（空 diff）")
    return "\n".join(parts)


# ---------- 工具实现（路径安全：只能碰 workspace 内） ----------

def resolve_in_workspace(workspace, rel):
    if not workspace or not rel:
        raise ToolError("需要 workspace 与 path 参数")
    root = os.path.realpath(workspace)
    full = os.path.realpath(os.path.join(root, rel))
    if os.path.commonpath([full, root]) != root:
        raise ToolError(f"路径越界（仅允许工作区内）: {rel}")
    return full


def tool_read_file(args, workspace):
    full = resolve_in_workspace(workspace, args.get("path", ""))
    try:
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            content = f.read(8000)
    except FileNotFoundError:
        raise ToolError(f"文件不存在: {args.get('path')}")
    return {"path": args.get("path"), "bytes": len(content), "content": content}


def tool_write_file(args, workspace):
    full = resolve_in_workspace(workspace, args.get("path", ""))
    content = args.get("content", "")
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)
    return {"path": args.get("path"), "written": len(content)}


TOOL_IMPLS = {"read_file": tool_read_file, "write_file": tool_write_file}


# ---------- OpenAI 兼容流式调用 ----------

def chat_completions_stream(messages, tool_names):
    """调 chat/completions（stream=true），产出 (kind, payload) 事件流。

    kind: "token" 正文增量 | "tool_call" 完整工具调用 | "finish" 结束原因 | "usage"
    4xx -> AGENT_PROTOCOL_VIOLATION（不重试）；5xx/网络 -> AGENT_INTERNAL（控制面可重试）
    """
    body = {"model": MODEL, "messages": messages, "stream": True}
    if tool_names:
        body["tools"] = [TOOLS_SPEC[n] for n in tool_names if n in TOOLS_SPEC]
        body["stream_options"] = {"include_usage": True}
    req = urllib.request.Request(
        BASE_URL + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + API_KEY},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=LLM_TIMEOUT)
    except urllib.error.HTTPError as e:
        detail = e.read(400).decode("utf-8", "replace")
        if e.code == 408 or e.code == 504:
            raise LLMError("AGENT_TIMEOUT", f"LLM 网关超时: {e.code}")
        code = "AGENT_PROTOCOL_VIOLATION" if e.code < 500 else "AGENT_INTERNAL"
        raise LLMError(code, f"LLM 网关 {e.code}: {detail}")
    except Exception as e:  # URLError / timeout 等
        raise LLMError("AGENT_INTERNAL", f"LLM 网关不可达: {e}")

    tool_acc = {}
    with resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                chunk = json.loads(data)
            except json.JSONDecodeError:
                continue
            if chunk.get("error"):
                raise LLMError("AGENT_INTERNAL", "LLM 流内错误: " + json.dumps(chunk["error"])[:300])
            usage = chunk.get("usage")
            if usage:
                yield "usage", usage
            choices = chunk.get("choices") or []
            if not choices:
                continue
            ch = choices[0]
            delta = ch.get("delta") or {}
            if delta.get("content"):
                yield "token", delta["content"]
            for tc in delta.get("tool_calls") or []:
                idx = tc.get("index", 0)
                slot = tool_acc.setdefault(idx, {"id": "", "name": "", "args": ""})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    slot["name"] = fn["name"]
                if fn.get("arguments"):
                    slot["args"] += fn["arguments"]
            if ch.get("finish_reason"):
                yield "finish", ch["finish_reason"]
    for slot in tool_acc.values():
        if slot["name"]:
            yield "tool_call", slot


# ---------- 评审主流程（OpenAI 真实模式） ----------

def run_openai(task_id, send, payload, cfg, workspace):
    allowed = list(cfg.get("allowed_tools") or ["read_file"])
    max_rounds = int(cfg.get("max_tool_rounds", 6))
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(payload)},
    ]
    usage_sum = {"prompt_tokens": 0, "completion_tokens": 0}

    for round_no in range(1, max_rounds + 2):
        send({"type": "event", "event": "thought",
              "data": {"text": f"评审第 {round_no} 轮（已用工具 {round_no - 1} 次）"}})
        tool_calls, finish, round_tokens = [], None, {"prompt_tokens": 0, "completion_tokens": 0}
        for kind, val in chat_completions_stream(messages, allowed):
            if kind == "token":
                send({"type": "token", "content": val})
            elif kind == "tool_call":
                tool_calls.append(val)
            elif kind == "finish":
                finish = val
            elif kind == "usage":
                for k in usage_sum:
                    usage_sum[k] += int(val.get(k) or 0)
                    round_tokens[k] += int(val.get(k) or 0)
        if not tool_calls:
            break
        # 工具循环：执行本轮全部调用，结果拼回 messages 继续
        messages.append({"role": "assistant", "tool_calls": [
            {"id": tc["id"], "type": "function",
             "function": {"name": tc["name"], "arguments": tc["args"] or "{}"}}
            for tc in tool_calls]})
        for tc in tool_calls:
            send({"type": "event", "event": "tool_call",
                  "data": {"tool": tc["name"], "args": _safe_json(tc["args"])}})
            try:
                impl = TOOL_IMPLS.get(tc["name"])
                if impl is None or tc["name"] not in allowed:
                    raise ToolError(f"工具不在白名单: {tc['name']}")
                args = json.loads(tc["args"] or "{}")
                result = impl(args, workspace)
            except ToolError as e:
                result = {"ok": False, "error": str(e)}
            except json.JSONDecodeError:
                result = {"ok": False, "error": "工具参数不是合法 JSON"}
            send({"type": "event", "event": "tool_result", "data": result})
            messages.append({"role": "tool", "tool_call_id": tc["id"],
                             "content": json.dumps(result, ensure_ascii=False)[:8000]})
        if round_no > max_rounds:
            send({"type": "event", "event": "thought",
                  "data": {"text": f"工具轮次已达上限 {max_rounds}，基于现有信息收尾"}})
            messages.append({"role": "user", "content": "工具轮次已用尽，请立即基于已有信息输出最终评审（含最后一行总结论）。"})

    if usage_sum["prompt_tokens"] == 0 and usage_sum["completion_tokens"] == 0:
        est = sum(len(str(m.get("content", ""))) for m in messages) // 4
        usage_sum = {"prompt_tokens": est, "completion_tokens": max(est // 3, 1)}
    send({"type": "usage", "prompt_tokens": usage_sum["prompt_tokens"],
          "completion_tokens": usage_sum["completion_tokens"], "model": MODEL})
    send({"type": "done", "task_id": task_id})
    print(f"[agent] task={task_id} mode=openai rounds={round_no} "
          f"tokens={usage_sum['prompt_tokens']}+{usage_sum['completion_tokens']}")


def _safe_json(s):
    try:
        return json.loads(s or "{}")
    except json.JSONDecodeError:
        return {"_raw": (s or "")[:200]}


# ---------- mock 假 LLM（无 key 降级：按 diff 启发式生成评审） ----------

MOCK_RULES = [
    (r"postgres://|mysql://|redis://|://\w+:[^@\s]+@", "blocker", "连接串/凭据硬编码进代码，应改用配置注入或密钥管理"),
    (r"hunter2|password\s*=|passwd\s*=", "blocker", "疑似硬编码口令，属凭据泄漏风险"),
    (r"//\s*暂不处理|/\*\s*暂不处理|except\s*:\s*pass|_?\s*=\s*err\b", "major", "错误被静默吞掉，故障会被延迟暴露且难以定位"),
    (r"defer\s+\w+\.Unlock\(\)", "major", "锁的 Unlock 时机被改动，需确认互斥区间是否仍完整覆盖临界区"),
    (r"float(32|64)?\s*\*|\*\s*100\b.*float|yuan float", "major", "金额用浮点运算会产生精度误差，支付域应全部用整数分"),
    (r"WithTimeout\([^)]*60", "major", "下游超时被大幅放宽（60s），会拖垮上游线程/连接池，需设兜底上限"),
    (r"Unmarshal\([^,]+,\s*\w+\)|Unmarshal\([^,]+,\s*&?\w+\)", "major", "Unmarshal 目标疑似传值（缺 &），解析不会生效"),
    (r"TODO|FIXME", "minor", "遗留 TODO，建议建跟踪任务并注明负责人与期限"),
    (r"0\.98|打\s*折|discount", "minor", "折扣/计价规则硬编码，建议走配置以便运营侧调整"),
]


def mock_findings(diff):
    out = []
    for line_no, line in enumerate(diff.splitlines(), 1):
        if not line.startswith("+") or line.startswith("+++"):
            continue
        text = line[1:]
        for pat, level, why in MOCK_RULES:
            if re.search(pat, text, re.I):
                out.append((level, why, text.strip()[:100]))
                break
    dedup = []
    for item in out:
        if item not in dedup:
            dedup.append(item)
    return dedup


def run_mock(task_id, send, payload, cfg, workspace):
    diff = payload.get("diff", "")
    send({"type": "event", "event": "thought", "data": {"text": "mock LLM：按 diff 启发式扫描风险点"}})
    # 演示一次工具调用（真实 agent 能力的降级形态）
    if "read_file" in (cfg.get("allowed_tools") or ["read_file"]) and workspace:
        probe = _probe_file(diff, workspace)
        if probe:
            send({"type": "event", "event": "tool_call", "data": {"tool": "read_file", "args": {"path": probe}}})
            try:
                res = tool_read_file({"path": probe}, workspace)
                res["content"] = res["content"][:400] + ("…" if len(res["content"]) > 400 else "")
            except ToolError as e:
                res = {"ok": False, "error": str(e)}
            send({"type": "event", "event": "tool_result", "data": res})

    findings = mock_findings(diff)
    md = [f"## mock 评审：{payload.get('repo', '')} PR #{payload.get('pr', '')}"]
    for level in ("blocker", "major", "minor"):
        items = [f for f in findings if f[0] == level]
        if not items:
            continue
        md.append(f"\n### {level.upper()}\n")
        md.extend(f"- {why}\n  > `{text}`" for _, why, text in items)
    if len(md) == 1:
        md.append("\n（mock 启发式未命中规则，视为无阻塞问题）")
    conclusion = "REQUEST_CHANGES" if any(f[0] == "blocker" for f in findings) else "APPROVE"
    md.append(f"\n---\n{conclusion}")
    text = "\n".join(md)

    for i in range(0, len(text), 3):  # 逐 token 流式，验证控制面真流式透传
        send({"type": "token", "content": text[i:i + 3]})
        time.sleep(0.015)
    send({"type": "usage", "prompt_tokens": len(diff) // 4 + 320, "completion_tokens": len(text) // 4,
          "model": "mock-llm"})
    send({"type": "done", "task_id": task_id})
    print(f"[agent] task={task_id} mode=mock findings={len(findings)} conclusion={conclusion}")


def _probe_file(diff, workspace):
    """从 diff 里挑一个真实存在的工作区文件做 read_file 演示。

    两种 diff 路径形态都兼容：
      GitHub 快照：workspace 即仓库根，diff 路径 = 仓库相对路径（drill/xxx.go）
      本地静态：  workspace 是仓库子目录，diff 路径带仓库前缀（svc-orders/orders.go）
    """
    for m in re.finditer(r"^\+\+\+ b/(.+)$", diff, re.M):
        rel = m.group(1).strip()
        candidates = [rel]
        parts = rel.split("/", 1)
        if len(parts) == 2:
            candidates.append(parts[1])
        for cand in candidates:
            try:
                resolve_in_workspace(workspace, cand)
                return cand
            except ToolError:
                continue
    return None


# ---------- SSE 合同端点 ----------

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "mode": MODE, "model": MODEL}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def do_POST(self):
        if self.path != "/run/stream":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(length) or b"{}")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.end_headers()

        def send(obj):
            self.wfile.write(f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.flush()

        task_id = str(req.get("task_id", ""))
        payload = req.get("payload") or {}
        cfg = req.get("agent_config") or {}
        workspace = payload.get("workspace", "")
        try:
            if MODE == "openai":
                run_openai(task_id, send, payload, cfg, workspace)
            else:
                run_mock(task_id, send, payload, cfg, workspace)
        except LLMError as e:
            send({"type": "error", "code": e.code, "message": str(e)})
        except BrokenPipeError:
            print(f"[agent] task={task_id} 客户端断开")
            return
        except Exception as e:  # 兜底：任何异常都以 error 事件收尾，保证流合同完整
            send({"type": "error", "code": "AGENT_INTERNAL", "message": f"agent 内部错误: {e}"})
        try:
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except Exception:
            pass

    def log_message(self, fmt, *args):
        pass  # 静默访问日志，业务日志走 print


if __name__ == "__main__":
    src = "env" if (os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_BASE_URL")
                    or os.environ.get("OPENAI_MODEL")) else CONFIG_PATH
    print(f"PR Review 裸 Agent listening on 0.0.0.0:{PORT}  mode={MODE}  config={src}"
          + (f"  model={MODEL}  base={BASE_URL}" if MODE == "openai"
             else "  （config.json 填 llm.api_key 或设 OPENAI_API_KEY 启用真实 LLM）"))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
