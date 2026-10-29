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

type Client struct {
	endpoint string
	database string
	user     string
	password string
	http     *http.Client
}

// New создаёт клиент для указанного экземпляра ClickHouse и базы данных.
func New(host string, port int, database, user, password string) *Client {
	return &Client{
		endpoint: "http://" + host + ":" + strconv.Itoa(port),
		database: database,
		user:     user,
		password: password,
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

// Ping выполняет простой запрос, чтобы проверить, что сервер принимает заданные учётные данные.
func (c *Client) Ping(ctx context.Context) error {
	return c.Exec(ctx, "SELECT 1")
}

// InsertJSONEachRow записывает пакет строк командой INSERT INTO <table> FORMAT JSONEachRow.
func (c *Client) InsertJSONEachRow(ctx context.Context, table string, rows []any) error {
	if len(rows) == 0 {
		return nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("clickhouse: encode row: %w", err)
		}
	}

	req, err := c.newRequest(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", &buf)
	if err != nil {
		return err
	}

	return c.do(req)
}

// Exec выполняет команду без декодирования результата (DDL, INSERT ... SELECT, DELETE).
func (c *Client) Exec(ctx context.Context, query string) error {
	req, err := c.newRequest(ctx, query, nil)
	if err != nil {
		return err
	}

	return c.do(req)
}

// Select выполняет запрос с FORMAT JSON и декодирует данные в dest (указатель на срез).
func (c *Client) Select(ctx context.Context, query string, dest any) error {
	req, err := c.newRequest(ctx, query+" FORMAT JSON", nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse: do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("clickhouse: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("clickhouse: decode response: %w", err)
	}
	if err := json.Unmarshal(payload.Data, dest); err != nil {
		return fmt.Errorf("clickhouse: decode data: %w", err)
	}

	return nil
}

func (c *Client) newRequest(ctx context.Context, query string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse endpoint: %w", err)
	}

	q := u.Query()
	q.Set("query", query)
	q.Set("database", c.database)

	q.Set("output_format_json_quote_64bit_integers", "0")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: build request: %w", err)
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
		return fmt.Errorf("clickhouse: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}
