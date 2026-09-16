"""AgentFlow 参照 Agent：零依赖实现 SSE 合同（docs/protocol/sse.md）。

用纯标准库实现，演示"任何语言 30 行接入"的合同承诺。
启动: python agent.py [端口]  （默认 8081）

联调步骤:
  1. 启动本 Agent:            python agent.py 8081
  2. 启动控制面:              go run ./cmd/agentflow-server
  3. 注册:                    POST http://localhost:8080/api/v1/agents
     {"name":"demo","type":"chat","runtime":{"type":"python-http","host":"http://127.0.0.1:8081"}}
  4. 提交任务并收流:          curl -N http://localhost:8080/api/v1/tasks/<task_id>/stream
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081

REPLY = "你好，我是演示Agent。控制面负责调度，我只按合同吐token。"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/run/stream":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(length) or b"{}")

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

        def send(obj):
            self.wfile.write(f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.flush()

        # 按 v1 协议吐事件：event -> token* -> event* -> usage -> done -> [DONE]
        send({"type": "event", "event": "thought", "data": {"text": "收到任务 " + str(req.get("task_id"))}})
        for ch in REPLY:
            send({"type": "token", "content": ch})
            time.sleep(0.05)  # 模拟生成延迟，验证控制面是"真流式"而非攒够再发
        send({"type": "event", "event": "tool_call", "data": {"tool": "search", "args": {"q": "demo"}}})
        send({"type": "event", "event": "tool_result", "data": {"ok": True}})
        send({"type": "usage", "prompt_tokens": 42, "completion_tokens": len(REPLY), "model": "demo-model"})
        send({"type": "done", "task_id": req.get("task_id", "")})
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def log_message(self, fmt, *args):
        print("[agent]", fmt % args)


if __name__ == "__main__":
    # 必须绑 0.0.0.0：容器内绑 127.0.0.1 只有容器自己能访问，控制面容器会被拒绝连接
    print(f"demo agent listening on 0.0.0.0:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
