package main

// board.go —— 中台业务数据：PR 注册表（假 PR 平台）与仓库目录。
// 控制面只认任务；PR 概念纯属中台业务——这正是控制面/业务面解耦的体现。

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// repoCat 仓库目录项。两种形态：
//   - GitHub 远端（Owner/Repo 非空）：PR 元数据与 diff 从 GitHub API 拉，
//     workspace 为 head SHA 的 zipball 快照
//   - 本地静态（Owner 空）：workspace/diff 来自 testdata（CI 侧产物的本地替身）
type repoCat struct {
	Name      string `json:"name"`
	Prod      bool   `json:"prod"`    // 生产仓库 -> pr-reviewer-prod（审批闸门）
	GitHub    bool   `json:"github"`  // true = GitHub 远端仓库
	Owner     string `json:"-"`       // GitHub owner
	Repo      string `json:"-"`       // GitHub repo 名
	Workspace string `json:"-"`       // 静态形态的工作区
	DiffFile  string `json:"-"`       // 静态形态的 diff 文件
}

// review 评审结果（回写到 PR 的"评论"）。
type review struct {
	Markdown         string    `json:"markdown"`
	Conclusion       string    `json:"conclusion"` // APPROVE / REQUEST_CHANGES / FAILED / UNKNOWN
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	PostedToGithub   string    `json:"posted_to_github,omitempty"` // "" 未回写 / ok / failed: 原因
	FinishedAt       time.Time `json:"finished_at"`
}

// pr 一条 PR 记录 + 其评审任务状态镜像。
type pr struct {
	Number     int       `json:"number"`
	Repo       string    `json:"repo"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	Prod       bool      `json:"prod"`
	GithubURL  string    `json:"github_url,omitempty"` // GitHub PR 页直链（远端形态）
	HeadSHA    string    `json:"-"`                    // 评审时锁定的 head（CI 重跑判定）
	Agent      string    `json:"agent"`
	AgentID    string    `json:"agent_id"`
	TaskID     string    `json:"task_id"`
	TaskStatus string    `json:"task_status"` // pending / pending_approval / running / 终态
	TaskError  string    `json:"task_error,omitempty"`
	Attempts   int       `json:"attempts"` // 提交次数（幂等 key 维度，见 pipeline.go）
	Review     *review   `json:"review,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type board struct {
	mu        sync.RWMutex
	cfg       envConfig
	repos     []repoCat
	prs       []*pr
	next      int
	agentIDs  map[string]string // agent name -> agent_id（bootstrap 后填充）
	agentMode string            // agent /health 上报的执行模式（mock | openai | down）
}

func newBoard(cfg envConfig) *board {
	root, _ := filepath.Abs(cfg.demoRoot)
	b := &board{cfg: cfg, next: 101, agentIDs: map[string]string{}, agentMode: "down"}

	// 静态形态目录（本地 testdata，CI 产物替身）：按名字供 config 空_owner 条目引用
	static := map[string]repoCat{
		"svc-orders": {
			Name:      "svc-orders",
			Workspace: filepath.Join(root, "examples", "pr-review-new", "testdata", "svc-orders"),
			DiffFile:  filepath.Join(root, "examples", "pr-review-new", "diffs", "pr-101.patch"),
		},
		"svc-payments": {
			Name:      "svc-payments",
			Prod:      true, // 生产仓库：任务级审批
			Workspace: filepath.Join(root, "examples", "pr-review-new", "testdata", "svc-payments"),
			DiffFile:  filepath.Join(root, "examples", "pr-review-new", "diffs", "pr-201.patch"),
		},
	}

	// 目录从 config.github.repos 构建：owner 非空 = GitHub 远端；空 = 静态回退
	b.repos = make([]repoCat, 0, len(cfg.githubRepos))
	for _, r := range cfg.githubRepos {
		if r.Owner != "" {
			b.repos = append(b.repos, repoCat{
				Name: r.Name, Prod: r.Prod, GitHub: true, Owner: r.Owner, Repo: r.Repo,
			})
			continue
		}
		if cat, ok := static[r.Name]; ok {
			b.repos = append(b.repos, cat)
		}
	}
	if len(b.repos) == 0 { // config 未配置任何仓库：退回静态全量（demo 保底）
		for _, cat := range static {
			b.repos = append(b.repos, cat)
		}
	}
	return b
}

func (b *board) repo(name string) (repoCat, bool) {
	for _, r := range b.repos {
		if r.Name == name {
			return r, true
		}
	}
	return repoCat{}, false
}

// create 新建 PR（demo：编号自增，作者固定 demo-dev）。
func (b *board) create(repoName, title string) *pr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.newLocked(repoName, title)
}

// findOrCreate CI 语义：带 PR 号的重复触发（webhook 重发）复用既有记录，
// 幂等由控制面 Idempotency-Key 保证，中台注册表同样不重复记账。
func (b *board) findOrCreate(repoName, title string, num int) *pr {
	if num > 0 {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i := range b.prs {
			if b.prs[i].Number == num && b.prs[i].Repo == repoName {
				return b.prs[i]
			}
		}
		p := b.newLocked(repoName, title)
		p.Number = num
		return p
	}
	return b.create(repoName, title)
}

func (b *board) newLocked(repoName, title string) *pr {
	cat, _ := b.repo(repoName)
	p := &pr{
		Number: b.next, Repo: repoName, Title: title, Author: "demo-dev",
		Prod: cat.Prod, CreatedAt: time.Now(),
	}
	if p.Title == "" {
		p.Title = "chore: demo change"
	}
	b.next++
	b.prs = append(b.prs, p)
	return p
}

func (b *board) get(num int) (*pr, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, p := range b.prs {
		if p.Number == num {
			return p, true
		}
	}
	return nil, false
}

// getByRepo 按 (repo, number) 精确查找——PR 身份是仓库内编号，
// 不同仓库可能撞号（GitHub PR 从 1 起，静态 demo 从 101 起），CI 轮询必须用它。
func (b *board) getByRepo(repoName string, num int) (*pr, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, p := range b.prs {
		if p.Repo == repoName && p.Number == num {
			return p, true
		}
	}
	return nil, false
}

// snapshot 返回拷贝（避免锁内外共享指针被并发改写；review 指针只整体替换）。
func (b *board) snapshot() []pr {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sort.Slice(b.prs, func(i, j int) bool { return b.prs[i].Number > b.prs[j].Number })
	out := make([]pr, 0, len(b.prs))
	for _, p := range b.prs {
		out = append(out, *p)
	}
	return out
}

// update 任务状态镜像更新（watcher 调用）。内联查找：Go mutex 不可重入，
// 不能在持写锁时调 get()（其内部取读锁会死锁）。
func (b *board) update(num int, fn func(*pr)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.prs {
		if p.Number == num {
			fn(p)
			return
		}
	}
}

func (b *board) setAgentMode(mode string) {
	b.mu.Lock()
	b.agentMode = mode
	b.mu.Unlock()
}

func (b *board) agentID(name string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.agentIDs[name]
}

// agentFor 选择评审 Agent：显式指定优先（auto-fixer 演练），否则按仓库分级。
func (b *board) agentFor(prod bool, explicit string) string {
	if explicit != "" {
		if _, ok := b.agentIDs[explicit]; ok {
			return explicit
		}
	}
	if prod {
		return agentReviewerProd
	}
	return agentReviewer
}

// loadDiff 读 PR 补丁（CI 产物）。容忍 UTF-8 BOM（PowerShell Out-File 惯例）。
func loadDiff(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(string(raw), "\uFEFF"), nil
}
