package logic

import (
	"context"
	"log/slog"
)

type logKey struct{}

// logger returns the request-scoped logger (request_id and method attached by
// the server's interceptors, Task 13), or the default logger.
func logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(logKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
