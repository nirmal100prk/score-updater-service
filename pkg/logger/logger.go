package logger

import (
	"log/slog"
	"os"
)

// Config holds options for customizing the logger.
type Config struct {
	Format string
	// Level is the minimum level to log (e.g. slog.LevelDebug, slog.LevelInfo).
	Level slog.Level
}

func NewLogger(cfg Config) *slog.Logger {
	var handler slog.Handler

	switch cfg.Format {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: cfg.Level,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: cfg.Level,
		})
	}

	return slog.New(handler)
}
