package httpx

import (
	"fmt"
	"github.com/gofiber/fiber/v2"
	"time"
)

type Envelope struct {
	Success    bool           `json:"success"`
	StatusCode int            `json:"statusCode"`
	Message    string         `json:"message"`
	Data       any            `json:"data,omitempty"`
	Meta       map[string]any `json:"meta,omitempty"`
	Pagination any            `json:"pagination,omitempty"`
}

func OK(c *fiber.Ctx, data any) error {
	message := "Success"
	responseData := data
	var pagination any
	if m, ok := data.(map[string]any); ok {
		if x, exists := m["message"]; exists {
			message = fmt.Sprint(x)
			delete(m, "message")
			if len(m) == 0 {
				responseData = nil
			}
		}
		if x, exists := m["pagination"]; exists {
			pagination = x
			delete(m, "pagination")
		}
	}
	return c.Status(200).JSON(Envelope{Success: true, StatusCode: 200, Message: message, Data: responseData, Meta: map[string]any{"timestamp": now(), "path": c.Path(), "version": "1.0"}, Pagination: pagination})
}
func Error(c *fiber.Ctx, status int, msg string) error {
	return c.Status(status).JSON(map[string]any{"statusCode": status, "timestamp": now(), "message": msg, "path": c.Path()})
}
func now() string { return timeNow().Format("2006-01-02T15:04:05.000Z07:00") }

var timeNow = func() time.Time { return time.Now() }
