package mlclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ScoreRequest — вход скоринга ноу-шоу.
type ScoreRequest struct {
	PatientID     int64     `json:"patient_id"`
	ProviderID    int64     `json:"provider_id"`
	StartsAt      time.Time `json:"starts_at"`
	LeadTimeHours float64   `json:"lead_time_hours"`
}

// ScoreResult — результат скоринга.
type ScoreResult struct {
	Score        float64 `json:"score"`
	ModelVersion string  `json:"model_version"`
}

// Client — HTTP-клиент к внешнему ML-сервису ноу-шоу скоринга.
type Client struct {
	baseURL string
	http    *http.Client
}

// New создаёт клиент к ML-сервису. timeout ограничивает ожидание ответа:
// скоринг не должен висеть на медленном ML, запись живёт и без него.
func New(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// Score вызывает POST {baseURL}/ml/no-show/score и возвращает score + версию модели.
func (c *Client) Score(ctx context.Context, req ScoreRequest) (*ScoreResult, error) {
	const op = "mlclient.Score"

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal request: %w", op, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/ml/no-show/score", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", op, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%s: do request: %w", op, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: unexpected status %d", op, resp.StatusCode)
	}

	var result ScoreResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", op, err)
	}

	return &result, nil
}
