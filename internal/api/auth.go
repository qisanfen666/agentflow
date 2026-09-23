// 内置认证与授权（M6）。治理边界说明：
// 认证回答"你是谁"（API Key），授权回答"你能做什么"（角色 × 方法权限矩阵）。
// 完整身份体系（用户/租户/审计归属）不属库职责——嵌入形态的宿主有自己的体系，
// 可通过 Dependencies.Auth 注入自定义中间件替换本实现。
package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// 角色常量。三级覆盖控制面语义：管理（改配置）/ 使用（提交消费）/ 观察（只读）。
const (
	RoleAdmin     = "admin"     // 全部端点
	RoleSubmitter = "submitter" // 提交/取消/订阅任务 + 读 Agent/Tool
	RoleReader    = "reader"    // 全部只读端点（不含 /metrics 运维端点）
)

// APIKeyEntry 内置鉴权的一把 key 及其角色（与根包 APIKey 解耦，api 包自持中间件入参）。
type APIKeyEntry struct {
	Key   string
	Roles []string
}

// 权限矩阵：路由模板 + 方法 -> 允许的角色集合。
// admin 不在表中也全放行；/health 探活豁免（不参与认证）。
var permissionMatrix = map[string]map[string][]string{
	"/api/v1/agents": {
		http.MethodGet:    {RoleAdmin, RoleSubmitter, RoleReader},
		http.MethodPost:   {RoleAdmin},
		http.MethodPut:    {RoleAdmin},
		http.MethodDelete: {RoleAdmin},
	},
	"/api/v1/agents/:id": {
		http.MethodGet:    {RoleAdmin, RoleSubmitter, RoleReader},
		http.MethodPut:    {RoleAdmin},
		http.MethodDelete: {RoleAdmin},
	},
	"/api/v1/agents/:id/versions": {
		http.MethodGet: {RoleAdmin, RoleSubmitter, RoleReader},
	},
	"/api/v1/tasks": {
		http.MethodGet:  {RoleAdmin, RoleSubmitter, RoleReader},
		http.MethodPost: {RoleAdmin, RoleSubmitter},
	},
	"/api/v1/tasks/:id": {
		http.MethodGet:    {RoleAdmin, RoleSubmitter, RoleReader},
		http.MethodDelete: {RoleAdmin, RoleSubmitter},
	},
	"/api/v1/tasks/:id/approve": {
		http.MethodPost: {RoleAdmin}, // 审批权归管理员（M6）
	},
	"/api/v1/tasks/:id/reject": {
		http.MethodPost: {RoleAdmin},
	},
	"/api/v1/tasks/:id/stream": {
		http.MethodGet: {RoleAdmin, RoleSubmitter, RoleReader},
	},
	"/api/v1/tools": {
		http.MethodGet:  {RoleAdmin, RoleSubmitter, RoleReader},
		http.MethodPost: {RoleAdmin},
	},
	"/api/v1/tools/validate": {
		http.MethodPost: {RoleAdmin, RoleSubmitter},
	},
	"/api/v1/tools/mcp": {
		http.MethodGet: {RoleAdmin, RoleSubmitter, RoleReader},
	},
	"/api/v1/tools/:id": {
		http.MethodGet: {RoleAdmin, RoleSubmitter, RoleReader},
	},
	"/api/v1/tools/:id/mcp": {
		http.MethodGet: {RoleAdmin, RoleSubmitter, RoleReader},
	},
	"/metrics": {
		http.MethodGet: {RoleAdmin},
	},
}

// NewAPIKeyAuth 内置认证授权中间件：
// 1) X-API-Key 匹配 -> 401；2) 角色对照权限矩阵 -> 403。
// 匹配到的角色写入 context（RolesKey），供后续审批流等治理环节取用。
func NewAPIKeyAuth(keys []APIKeyEntry) gin.HandlerFunc {
	byKey := make(map[string][]string, len(keys))
	for _, k := range keys {
		byKey[k.Key] = k.Roles
	}
	return func(c *gin.Context) {
		key := c.GetHeader("X-API-Key")
		roles, ok := byKey[key]
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code": "UNAUTHORIZED", "message": "missing or invalid API key",
			})
			return
		}
		c.Set(ContextRoles, roles)

		allowed, ok := permissionMatrix[c.FullPath()][c.Request.Method]
		if !ok || !hasAnyRole(roles, allowed) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"code": "FORBIDDEN", "message": "role not permitted for " + c.Request.Method + " " + c.FullPath(),
			})
			return
		}
		c.Next()
	}
}

// ContextRoles gin context key：当前请求的角色集合。
const ContextRoles = "agentflow.roles"

func hasAnyRole(owned, allowed []string) bool {
	for _, o := range owned {
		if o == RoleAdmin {
			return true // admin 全放行
		}
		for _, a := range allowed {
			if o == a {
				return true
			}
		}
	}
	return false
}

// ParseAPIKeys 解析 "key:role1,role2;key2:role" 形式（env 配置用）。
func ParseAPIKeys(s string) []APIKeyEntry {
	var out []APIKeyEntry
	for _, pair := range strings.Split(s, ";") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, roles, _ := strings.Cut(pair, ":")
		var rs []string
		for _, r := range strings.Split(roles, ",") {
			if r = strings.TrimSpace(r); r != "" {
				rs = append(rs, r)
			}
		}
		if key != "" && len(rs) > 0 {
			out = append(out, APIKeyEntry{Key: key, Roles: rs})
		}
	}
	return out
}
