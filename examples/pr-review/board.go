package main

// board.go —— 中台业务数据：PR 注册表（假 PR 平台）与仓库目录。
// 控制面只认任务；PR 概念纯属中台业务（这正是控制面/业务面解耦的体现）。

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// repoCat 仓库目录项：PR 的 workspace 与 diff 来源（CI 负责产出，中台只引用）。
type repoCat struct {
	Name      string `json:"name"`
	Prod      bool   `json:"prod"` // 生产仓库 -> pr-reviewer-prod（审批闸门）
	Workspace string `json:"-"`
	DiffFile  string `json:"-"`
}

// review 评审结果（回写到 PR 的"评论"）。
type review struct {
	Markdown         string    `json:"markdown"`
	Conclusion       string    `json:"conclusion"` // APPROVE / REQUEST_CHANGES / FAILED
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	FinishedAt       time.Time `json:"finished_at"`
}

// pr 一条 PR 记录 + 其评审任务状态镜像。
type pr struct {
	Number     int       `json:"number"`
	Repo       string    `json:"repo"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	Prod       bool      `json:"prod"`
	AgentID    string    `json:"agent_id"`
	TaskID     string    `json:"task_id"`
	TaskStatus string    `json:"task_status"` // pending / pending_approval / running / 终态
	TaskError  string    `json:"task_error,omitempty"`
	Review     *review   `json:"review,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type board struct {
	mu       sync.RWMutex
	cfg      envConfig
	repos    []repoCat
	prs      []*pr
	next     int
	agentIDs map[string]string // agent name -> agent_id（bootstrap 后填充）
}

func newBoard(cfg envConfig) *board {
	root, _ := filepath.Abs(cfg.demoRoot)
	b := &board{cfg: cfg, next: 101, agentIDs: map[string]string{}}
	b.repos = []repoCat{
		{
			Name: "svc-orders", Prod: false,
			Workspace: filepath.Join(root, "examples", "pr-review", "testdata", "svc-orders"),
			DiffFile:  filepath.Join(root, "examples", "pr-review", "diffs", "pr-101.patch"),
		},
		{
			Name: "svc-payments", Prod: true,
			Workspace: filepath.Join(root, "examples", "pr-review", "testdata", "svc-payments"),
			DiffFile:  filepath.Join(root, "examples", "pr-review", "diffs", "pr-201.patch"),
		},
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
// 而不是另建一条同号记录——幂等由控制面保证，中台注册表同样不重复记账。
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

// newLocked 追加新记录（调用方持锁或经 create）。编号自增。
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

// loadDiff 读 PR 补丁（CI 产物）。容忍 UTF-8 BOM（PowerShell Out-File 惯例）。
func loadDiff(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := string(raw)
	s = strings.TrimPrefix(s, "\uFEFF") // PowerShell Out-File 的 UTF-8 BOM
	return s, nil
}

