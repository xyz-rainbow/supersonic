package backend

import (
	"context"
	"testing"
	"time"
)

func TestServerManager_TestConnectionAndAuthRace(t *testing.T) {
	sm := NewServerManager("test", "1.0", &Config{}, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer cancel()

	_ = sm.TestConnectionAndAuth(ctx, ServerConnection{}, "")
}
