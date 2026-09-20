package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SPKI public key from the supplied AVE vemachine JavaScript (_0x415c7b).
const avePublicKey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp7rxCs+UF5QjAZWY63Ow1rNY4prtorIRawALlqGcWrDP2TKqC6XLybJCwOZ8HCGYzzHdQJFBLb8wlbaAJxg2/G+glwN/Hp1xNuYw6uJ7LTFMZCFsU5ReLxZ83uVs/uG80vyrpaiN+eU58B9j12+w4VbIv4dd0a5ILAQMLjJQiUgiGfD4JI9ic8qCNwOo2su3wdKthMeg5WYhYXtKJyUBJMn5odKd7XOQO7KmsuHy+dEbutSPuC2kTY+y2bzHUdTYeUp6U/GUZCjHirZCUCQyCBPE8nWoCRjhP9+ewSKSRPaTOG/uicrN1cUZC5Oal9PPigGAJ8gkKTPDgZHFPXTKuQIDAQAB"

var errAveAuthExpired = errors.New("AVE x-auth expired or rejected")

func (a *API) aveAuth(ctx context.Context) (string, error) {
	if a.Store == nil || a.Store.DB == nil {
		return "", fmt.Errorf("AVE authentication requires a database")
	}
	var value string
	err := a.Store.DB.QueryRow(ctx, "SELECT x_auth FROM ave_configs WHERE id=1").Scan(&value)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("read AVE x-auth: %w", err)
	}
	if value = strings.TrimSpace(value); value != "" {
		return value, nil
	}
	return strings.TrimSpace(a.Cfg.AveXAuth), nil
}

func (a *API) saveAveAuth(ctx context.Context, token string) error {
	if a.Store == nil || a.Store.DB == nil {
		return fmt.Errorf("AVE authentication requires a database")
	}
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("cannot save an empty AVE x-auth")
	}
	_, err := a.Store.DB.Exec(ctx, `INSERT INTO ave_configs(id,x_auth) VALUES(1,$1)
		ON CONFLICT(id) DO UPDATE SET x_auth=EXCLUDED.x_auth,updated_at=now()`, token)
	if err != nil {
		return fmt.Errorf("save AVE x-auth: %w", err)
	}
	return nil
}

// aveJSON makes at most two business requests. Refresh endpoints use aveDoJSON
// directly, so authentication failures there cannot recursively trigger refresh.
func (a *API) aveJSON(ctx context.Context, path string, q map[string]string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	auth, err := a.aveAuth(ctx)
	if err != nil {
		return nil, err
	}
	if auth == "" {
		auth, err = a.refreshAveAuth(ctx, auth)
		if err != nil {
			return nil, err
		}
		return a.aveDoJSON(ctx, http.MethodGet, path, q, nil, auth)
	}
	out, err := a.aveDoJSON(ctx, http.MethodGet, path, q, nil, auth)
	if !errors.Is(err, errAveAuthExpired) {
		return out, err
	}
	auth, err = a.refreshAveAuth(ctx, auth)
	if err != nil {
		return nil, err
	}
	return a.aveDoJSON(ctx, http.MethodGet, path, q, nil, auth)
}

func (a *API) aveDoJSON(ctx context.Context, method, path string, q map[string]string, body any, auth string) (map[string]any, error) {
	if a.Cfg.AveBaseURL == "" {
		return nil, fmt.Errorf("AVE_BASE_URL is not configured")
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, a.Cfg.AveBaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	vals := req.URL.Query()
	for k, v := range q {
		if v != "" {
			vals.Set(k, v)
		}
	}
	req.URL.RawQuery = vals.Encode()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Ave-Platform", "web")
	if auth != "" {
		req.Header.Set("x-auth", auth)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w (HTTP %d)", errAveAuthExpired, res.StatusCode)
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("AVE status %d", res.StatusCode)
	}
	var out map[string]any
	if err = json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode AVE response: %w", err)
	}
	if out == nil {
		return nil, fmt.Errorf("AVE returned an empty response")
	}
	if status, ok := out["status"]; ok {
		switch str(status) {
		case "10000", "10001":
			return nil, errAveAuthExpired
		case "1":
		default:
			return nil, fmt.Errorf("AVE request failed (status %v): %v", status, out["msg"])
		}
	}
	return out, nil
}

func (a *API) refreshAveAuth(ctx context.Context, rejectedToken string) (string, error) {
	a.aveAuthMu.Lock()
	defer a.aveAuthMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// Another request (or a manual update) may already have replaced this token.
	current, err := a.aveAuth(ctx)
	if err != nil {
		return "", err
	}
	if current != "" && current != rejectedToken {
		return current, nil
	}
	visitorID := strings.TrimSpace(a.Cfg.AveVisitorID)
	if visitorID == "" {
		return "", fmt.Errorf("AVE_VISITOR_ID is required to refresh x-auth; use visitorId from the AVE browser fingerprint library")
	}
	if len(visitorID) != 32 || strings.Trim(visitorID, "0123456789abcdefABCDEF") != "" {
		return "", fmt.Errorf("AVE_VISITOR_ID must be the 32-character hexadecimal browser visitorId")
	}
	clock, err := a.aveDoJSON(ctx, http.MethodGet, "/v1api/v2/settings/serverTime", nil, nil, "")
	if err != nil {
		return "", fmt.Errorf("get AVE server time: %w", err)
	}
	clockData, _ := clock["data"].(map[string]any)
	seconds := id(clockData["server_time"])
	if str(clock["status"]) != "1" || seconds <= 0 || seconds > (int64(1<<63-1)-999)/1000 {
		return "", fmt.Errorf("AVE returned an invalid server_time")
	}
	// Matches String(server_time) + String(Date.now()).slice(-3) in vemachine.
	timestamp := seconds*1000 + time.Now().UnixMilli()%1000
	requestID, err := aveRequestID(visitorID, timestamp)
	if err != nil {
		return "", fmt.Errorf("generate AVE request_id: %w", err)
	}
	result, err := a.aveDoJSON(ctx, http.MethodPost, "/v1api/v1/captcha/requestToken", nil, map[string]string{"request_id": requestID}, "")
	if err != nil {
		return "", fmt.Errorf("request AVE token: %w", err)
	}
	data, _ := result["data"].(map[string]any)
	token, _ := data["id"].(string)
	token = strings.TrimSpace(token)
	if str(result["status"]) != "1" || token == "" {
		return "", fmt.Errorf("AVE requestToken returned no valid token")
	}
	if image, _ := data["image"].(string); image != "" {
		return "", fmt.Errorf("AVE requires captcha verification before x-auth can be updated")
	}
	if err = a.saveAveAuth(ctx, token); err != nil {
		return "", err
	}
	return token, nil
}

func aveRequestID(visitorID string, timestamp int64) (string, error) {
	der, err := base64.StdEncoding.DecodeString(avePublicKey)
	if err != nil {
		return "", err
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return "", err
	}
	publicKey, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return "", fmt.Errorf("AVE public key is not RSA")
	}
	plaintext := fmt.Sprintf("%s$web$1.0.0$r1r$%d", visitorID, timestamp)
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, publicKey, []byte(plaintext), nil)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}
