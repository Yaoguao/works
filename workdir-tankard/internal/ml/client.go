package ml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const ModeHeader = "X-ML-Mode"

type HTTPRanker struct {
	client *http.Client
	url    string
}

// NewHTTPRanker создаёт ранжировщик, обращающийся к указанному адресу ML-заглушки.
func NewHTTPRanker(url string, timeout time.Duration) *HTTPRanker {
	return &HTTPRanker{
		client: &http.Client{Timeout: timeout},
		url:    url,
	}
}

// Rank вызывает POST /ml/rank и возвращает идентификаторы товаров в порядке ранжирования. Если задан режим, он передаётся через ModeHeader, чтобы вызывающая сторона могла включить медленный режим или ошибку.
func (r *HTTPRanker) Rank(
	ctx context.Context,
	userID int64,
	surface string,
	candidates []int64,
	mode string,
) ([]int64, error) {
	payload, err := json.Marshal(RankRequest{
		UserID:       userID,
		Surface:      surface,
		CandidateIDs: candidates,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal rank request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build rank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if mode != "" {
		req.Header.Set(ModeHeader, mode)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call ml rank: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ml rank returned status %d", resp.StatusCode)
	}

	var out RankResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode rank response: %w", err)
	}

	return out.RankedProductIDs, nil
}
