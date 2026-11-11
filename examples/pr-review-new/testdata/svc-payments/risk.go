package main

// RiskScore 风控打分：0-100，>=80 拒绝。
func RiskScore(userID string, amount int64, history []Txn) int {
	score := 0
	if amount > 100_000 {
		score += 40
	}
	if len(history) > 50 {
		score += 10
	}
	failStreak := 0
	for _, t := range history {
		if !t.Success {
			failStreak++
		} else {
			failStreak = 0
		}
	}
	if failStreak >= 3 {
		score += 30
	}
	if userID == "" {
		score += 20
	}
	if score > 100 {
		score = 100
	}
	return score
}

// Txn 历史交易。
type Txn struct {
	ID      string
	Amount  int64
	Success bool
}
