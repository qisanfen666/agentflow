package main

// github.go —— GitHub 集成（阶段 1 的"CI 触发"与拉取面）：
//
//	getPR / getPRDiff / listOpenPRs   PR 元数据 / diff 文本 / 开放列表（CI 轮询）
//	ensureWorkspace                   按 head SHA 下载 zipball 解出仓库快照
//	（裸 agent 的 read_file 在快照上工作，与真实仓库同构）
//	postComment                       阶段 4 回写：评审结论发到 GitHub PR 评论
//
// 认证：Bearer token（config github.token）。所有调用幂等可重试。

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type githubClient struct {
	base  string
	token string
	hc    *http.Client
}

func newGithubClient(base, token string) *githubClient {
	return &githubClient{
		base:  strings.TrimSuffix(base, "/"),
		token: token,
		hc:    &http.Client{Timeout: 60 * time.Second},
	}
}

// ghPR PR 元数据（拉取评审与 CI 轮询共用）。
type ghPR struct {
	Number  int    `json:"number"`
	Title   string `json:"title"`
	State   string `json:"state"`
	HeadRef string `json:"head_ref"` // 手工映射，见 getPR/listOpenPRs
	HeadSHA string `json:"head_sha"`
}

func (g *githubClient) call(ctx context.Context, method, path, accept string, body io.Reader) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, g.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+g.token)
	req.Header.Set("User-Agent", "agentflow-pr-review-demo")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 400 {
		msg := string(raw)
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return resp.StatusCode, raw, fmt.Errorf("github %s -> %d: %s", path, resp.StatusCode, msg)
	}
	return resp.StatusCode, raw, nil
}

// headPR 是 GitHub PR JSON 里 head 字段的形状。
type headPR struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

func (h headPR) flat() ghPR {
	return ghPR{Number: h.Number, Title: h.Title, HeadRef: h.Head.Ref, HeadSHA: h.Head.SHA}
}

func (g *githubClient) getPR(ctx context.Context, owner, repo string, num int) (ghPR, error) {
	_, raw, err := g.call(ctx, http.MethodGet,
		fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, num), "application/vnd.github+json", nil)
	if err != nil {
		return ghPR{}, err
	}
	var h headPR
	if err := json.Unmarshal(raw, &h); err != nil {
		return ghPR{}, err
	}
	return h.flat(), nil
}

// getPRDiff 取 PR 的 unified diff 文本（.diff媒体类型）。
func (g *githubClient) getPRDiff(ctx context.Context, owner, repo string, num int) (string, error) {
	_, raw, err := g.call(ctx, http.MethodGet,
		fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, num),
		"application/vnd.github.diff", nil)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (g *githubClient) listOpenPRs(ctx context.Context, owner, repo string) ([]ghPR, error) {
	_, raw, err := g.call(ctx, http.MethodGet,
		fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=50", owner, repo),
		"application/vnd.github+json", nil)
	if err != nil {
		return nil, err
	}
	var heads []headPR
	if err := json.Unmarshal(raw, &heads); err != nil {
		return nil, err
	}
	out := make([]ghPR, 0, len(heads))
	for _, h := range heads {
		out = append(out, h.flat())
	}
	return out, nil
}

// postComment 把评审结论回写到 GitHub PR（issue comment 端点）。
func (g *githubClient) postComment(ctx context.Context, owner, repo string, num int, body string) error {
	payload, _ := json.Marshal(map[string]string{"body": body})
	code, _, err := g.call(ctx, http.MethodPost,
		fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, repo, num),
		"application/vnd.github+json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("评论回写 (%d): %w", code, err)
	}
	return nil
}

// ensureWorkspace 按 head SHA 下载仓库 zipball 并解压为 agent 工作区。
// 同 SHA 幂等：已存在即复用（新提交 = 新目录，天然缓存）。
func (g *githubClient) ensureWorkspace(ctx context.Context, owner, repo, sha, root string) (string, error) {
	dir := filepath.Join(root, fmt.Sprintf("%s-%s-%s", owner, repo, sha[:min(8, len(sha))]))
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		if _, err := os.Stat(filepath.Join(dir, "README.md")); err == nil {
			return dir, nil // 粗粒度完整性标记：解压成功过
		}
	}
	_ = os.RemoveAll(dir)

	_, raw, err := g.call(ctx, http.MethodGet,
		fmt.Sprintf("/repos/%s/%s/zipball/%s", owner, repo, sha), "", nil)
	if err != nil {
		return "", fmt.Errorf("下载 zipball: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", fmt.Errorf("解压 zipball: %w", err)
	}
	if err := extractZip(zr, dir); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	return dir, nil
}

// extractZip 解压到 dest，剥掉 GitHub zipball 的顶层目录（repo-sha/）。
// 路径安全：剥顶后 join 的结果必须仍在 dest 内（zip slip 防护）。
func extractZip(zr *zip.Reader, dest string) error {
	root, err := filepath.Abs(dest)
	if err != nil {
		return err
	}
	for _, f := range zr.File {
		name := strings.TrimPrefix(f.Name, "/")
		parts := strings.SplitN(name, "/", 2)
		if len(parts) < 2 { // 顶层目录条目本身
			continue
		}
		target := filepath.Join(root, filepath.FromSlash(parts[1]))
		if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return fmt.Errorf("zip 条目越界: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode().Perm())
		if err != nil {
			rc.Close()
			return err
		}
		_, werr := io.Copy(out, rc)
		rc.Close()
		out.Close()
		if werr != nil {
			return werr
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
