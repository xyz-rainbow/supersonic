package backend

import (
	"context"
	"testing"
	"time"

	"github.com/dweymouth/supersonic/backend/player"
)

// Minimal mock to satisfy player.BasePlayer interface without import cycles
type testMockPlayer struct {
	player.BasePlayerCallbackImpl
}

func (m *testMockPlayer) Continue() error { return nil }
func (m *testMockPlayer) Pause() error { return nil }
func (m *testMockPlayer) Stop(force bool) error { return nil }
func (m *testMockPlayer) SeekSeconds(secs float64) error { return nil }
func (m *testMockPlayer) IsSeeking() bool { return false }
func (m *testMockPlayer) SetVolume(vol int) error { return nil }
func (m *testMockPlayer) GetVolume() int { return 100 }
func (m *testMockPlayer) GetStatus() player.Status { return player.Status{} }
func (m *testMockPlayer) Destroy() {}

func TestPlaybackEngine_ThreadSafety(t *testing.T) {
	ctx := context.Background()
	pe := NewPlaybackEngine(
		ctx,
		&ServerManager{},
		nil,
		&testMockPlayer{},
		&PlaybackConfig{},
		&ScrobbleConfig{},
		&TranscodingConfig{},
	)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ {
			pe.SetLoopMode(LoopAll)
			pe.GetLoopMode()
			pe.SetShuffle(true)
			pe.getPlayQueueLength()
			pe.NowPlaying()
			pe.Stop()
			pe.NowPlayingIndex()
		}
		close(done)
	}()

	for i := 0; i < 500; i++ {
		pe.SetLoopMode(LoopOne)
		pe.GetLoopMode()
		pe.SetShuffle(false)
		pe.getPlayQueueLength()
		pe.NowPlaying()
		pe.Stop()
		pe.NowPlayingIndex()
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for goroutines")
	}
}
