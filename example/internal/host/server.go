// Package host contains HTTP lifecycle policy for these example programs only.
package host

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// ServeHTTP runs the host-owned server until failure or cancellation, then gives
// active HTTP requests ten seconds to finish before closing remaining connections.
// Hijacked WebSocket connections are closed by Node.Run's cleanup.
func ServeHTTP(ctx context.Context, server *http.Server) error {
	return serveHTTPServer(ctx, server, server.ListenAndServe)
}

// 监听与退出控制分离，便于用离线替身验证故障取消，不绑定真实端口。
func serveHTTPServer(ctx context.Context, server *http.Server, serve func() error) error {
	if ctx.Err() != nil {
		return nil
	}
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		if err := serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
