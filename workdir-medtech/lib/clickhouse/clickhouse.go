package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client — минимальный HTTP-клиент ClickHouse.
// Запись идёт батчем в формате JSONEachRow, чтение — через FORMAT JSON
// (см. HINT_03: для практики HTTP-интерфейса достаточно, отдельный драйвер не нужен).
type Client struct {
	endpoint string
	user     string
	password string
	http     *http.Client
}

func New(host string, port int, user, password string) *Client {
	return &Client{
		endpoint: "http://" + host + ":" + strconv.Itoa(port),
		user:     user,
		password: password,
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

// InsertJSONEachRow грузит батч строк: INSERT INTO <table> FORMAT JSONEachRow.
func (c *Client) InsertJSONEachRow(ctx context.Context, table string, rows []any) error {
	const op = "clickhouse.InsertJSONEachRow"

	if len(rows) == 0 {
		return nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("%s: encode row: %w", op, err)
		}
	}

	req, err := c.newRequest(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", &buf)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := c.do(req); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// Exec выполняет запрос без разбора результата (DDL, INSERT ... SELECT, TRUNCATE).
func (c *Client) Exec(ctx context.Context, query string) error {
	const op = "clickhouse.Exec"

	req, err := c.newRequest(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := c.do(req); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// Select выполняет запрос с FORMAT JSON и раскладывает data в dest (указатель на слайс).
func (c *Client) Select(ctx context.Context, query string, dest any) error {
	const op = "clickhouse.Select"

	req, err := c.newRequest(ctx, query+" FORMAT JSON", nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: do request: %w", op, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s: read body: %w", op, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d: %s", op, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("%s: decode response: %w", op, err)
	}
	if err := json.Unmarshal(payload.Data, dest); err != nil {
		return fmt.Errorf("%s: decode data: %w", op, err)
	}

	return nil
}

func (c *Client) newRequest(ctx context.Context, query string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}

	q := u.Query()
	q.Set("query", query)
	// 64-битные счётчики в JSON отдаются строкой по умолчанию — просим числа как числа,
	// чтобы они раскладывались в uint64/int64 без ручного парсинга.
	q.Set("output_format_json_quote_64bit_integers", "0")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	if c.password != "" {
		req.Header.Set("X-ClickHouse-Key", c.password)
	}

	return req, nil
}

func (c *Client) do(req *http.Request) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}
