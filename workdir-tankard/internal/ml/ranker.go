package ml

import "sort"

const (
	ModeNormal = "normal"
	ModeSlow   = "slow"
	ModeFail   = "fail"
)

type RankRequest struct {
	UserID       int64   `json:"user_id"`
	Surface      string  `json:"surface"`
	CandidateIDs []int64 `json:"candidate_ids"`
}

type RankResponse struct {
	RankedProductIDs []int64 `json:"ranked_product_ids"`
}

// Rank детерминированно ранжирует кандидатов для пользователя. Для одинаковых пользователя и списка кандидатов заглушка всегда возвращает один и тот же порядок.
func Rank(userID int64, candidates []int64) []int64 {
	ranked := make([]int64, len(candidates))
	copy(ranked, candidates)

	sort.SliceStable(ranked, func(i, j int) bool {
		return score(userID, ranked[i]) > score(userID, ranked[j])
	})

	return ranked
}

// score вычисляет стабильную псевдооценку на основе идентификаторов пользователя и товара.
func score(userID, productID int64) uint64 {
	h := uint64(userID)*2654435761 + uint64(productID)*40503
	return h % 10000
}
