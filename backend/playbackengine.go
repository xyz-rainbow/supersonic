package backend

import (
	"sync"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/dweymouth/supersonic/backend/mediaprovider"
	"github.com/dweymouth/supersonic/backend/player"
	"github.com/dweymouth/supersonic/backend/player/mpv"
	"github.com/dweymouth/supersonic/backend/util"
	"github.com/dweymouth/supersonic/sharedutil"
)



var (
	ReplayGainNone  = player.ReplayGainNone.String()
	ReplayGainAlbum = player.ReplayGainAlbum.String()
	ReplayGainTrack = player.ReplayGainTrack.String()
	ReplayGainAuto  = "Auto"
)

type InsertQueueMode int

const (
	Replace InsertQueueMode = iota
	InsertNext
	Append
)

type QueueType int

const (
	PlayQueue QueueType = iota
	ShuffledPlayQueue
	Both
)

// The playback loop mode (LoopNone, LoopAll, LoopOne).
type LoopMode int

const (
	LoopNone LoopMode = iota
	LoopAll
	LoopOne
)

type PlaybackState = player.State

type PlaybackStatus struct {
	State    PlaybackState
	TimePos  float64
	Duration float64
}

type playbackEngine struct {
	mu            sync.RWMutex
	ctx           context.Context
	cancelPollPos context.CancelFunc
	sm            *ServerManager
	audiocache    *AudioCache
	player        player.BasePlayer

	playTimeStopwatch   util.Stopwatch
	curTrackDuration    float64
	latestTrackPosition float64 // cleared by checkScrobble
	callbacksDisabled   bool

	playQueue         []mediaprovider.MediaItem
	shuffledPlayQueue []mediaprovider.MediaItem

	nowPlayingIdx int
	isRadio       bool
	loopMode      LoopMode
	shuffle       bool

	pauseAfterCurrent bool // flag to pause playback after current track ends

	// flags for handleOnTrackChange / handleOnStopped callbacks - reset to false in the callbacks
	wasStopped       bool // true iff player was stopped before handleOnTrackChange invocation
	alreadyScrobbled bool // true iff the previously-playing track was already scrobbled

	// if >= 0, track number that was requested by PlayTrackAt
	// onTrackChange callback should set nowPlayingIdx to this,
	// and reset this to -1
	pendingTrackChangeNum int

	pendingPlayerChange       bool
	pendingPlayerChangeStatus player.Status

	// set when restoring session state: track is conceptually loaded+paused
	// but MPV hasn't been touched yet; cleared on Continue or playTrackAt
	pendingLoadPaused    bool
	pendingLoadStartTime float64

	// Whether we need to set the next track on the Player
	// before the current track completes (normally when 10 seconds remain
	// in the time pos polling function)
	needToSetNextTrack bool

	// to pass to onSongChange listeners; clear once listeners have been called
	lastScrobbled *mediaprovider.Track
	playbackCfg   *PlaybackConfig
	scrobbleCfg   *ScrobbleConfig
	transcodeCfg  *TranscodingConfig
	replayGainCfg ReplayGainConfig

	// registered callbacks
	onBeforeSongChange []func(next mediaprovider.MediaItem)
	onSongChange       []func(nowPlaying mediaprovider.MediaItem, justScrobbledIfAny *mediaprovider.Track)
	onPlayTimeUpdate   []func(float64, float64, bool)
	onLoopModeChange   []func(LoopMode)
	onShuffleChange    []func(bool)
	onVolumeChange     []func(int)
	onSeek             []func()
	onPaused           []func()
	onStopped          []func()
	onPlaying          []func()
	onQueueChange      []func()

	onRadioMetadataChange []func(radioName, title, artist string)
}

func NewPlaybackEngine(
	ctx context.Context,
	s *ServerManager,
	c *AudioCache,
	p player.BasePlayer,
	playbackCfg *PlaybackConfig,
	scrobbleCfg *ScrobbleConfig,
	transcodeCfg *TranscodingConfig,
) *playbackEngine {
	// clamp to 99% to avoid any possible rounding issues
	scrobbleCfg.ThresholdPercent = clamp(scrobbleCfg.ThresholdPercent, 0, 99)
	pm := &playbackEngine{
		ctx:           ctx,
		sm:            s,
		audiocache:    c,
		player:        p,
		playbackCfg:   playbackCfg,
		scrobbleCfg:   scrobbleCfg,
		transcodeCfg:  transcodeCfg,
		nowPlayingIdx: -1,
		wasStopped:    true,
	}
	switch playbackCfg.RepeatMode {
	case "All":
		pm.loopMode = LoopAll
	case "One":
		pm.loopMode = LoopOne
	}

	pm.shuffle = playbackCfg.Shuffle

	pm.registerPlayerCallbacks(p)
	s.OnLogout(func() {
		pm.StopAndClearPlayQueue()
	})

	return pm
}

func (p *playbackEngine) registerPlayerCallbacks(pl player.BasePlayer) {
	pl.OnTrackChange(p.handleOnTrackChange)
	pl.OnSeek(func() {
		p.handleTimePosUpdate(true)
		p.invokeNoArgCallbacks(p.onSeek)
		seekState := "playing"
		if p.PlaybackStatus().State == player.Paused {
			seekState = "paused"
		}
		p.reportPlayback(seekState)
	})
	pl.OnStopped(p.handleOnStopped)
	pl.OnPaused(func() {
		p.playTimeStopwatch.Stop()
		p.stopPollTimePos()
		p.invokeNoArgCallbacks(p.onPaused)
		p.reportPlayback("paused")
	})
	pl.OnPlaying(func() {
		p.playTimeStopwatch.Start()
		p.startPollTimePos()
		p.invokeNoArgCallbacks(p.onPlaying)
		p.reportPlayback("playing")
	})
}

func (p *playbackEngine) unregisterPlayerCallbacks(pl player.BasePlayer) {
	pl.OnPaused(nil)
	pl.OnPlaying(nil)
	pl.OnStopped(nil)
	pl.OnSeek(nil)
	pl.OnTrackChange(nil)
}

func (p *playbackEngine) SetPlayer(pl player.BasePlayer) error {
	needToUnpause := false

	stat := p.PlaybackStatus()
	p.mu.RLock()
	pendingPlayerChange := p.pendingPlayerChange
	pendingLoadPaused := p.pendingLoadPaused
	p.mu.RUnlock()
	if pendingPlayerChange || pendingLoadPaused {
		stat.State = player.Paused
		needToUnpause = pendingLoadPaused
	}

	var url string
	var meta mediaprovider.MediaItemMetadata
	var track *mediaprovider.Track
	var isTrack bool

	p.mu.Lock()
	p.pendingPlayerChange = true
	p.pendingPlayerChangeStatus = player.Status{State: stat.State, TimePos: stat.TimePos}
	p.mu.Unlock()

	p.unregisterPlayerCallbacks(p.player)
	p.player.Destroy()
	p.mu.Lock()
	p.player = pl
	p.mu.Unlock()
	p.registerPlayerCallbacks(pl)

	idx := p.NowPlayingIndex()
	if idx >= 0 {
		item := p.getPlayQueueItemAt(idx)
		url = p.getMediaURLForIdx(idx)
		meta = item.Metadata()
		track, isTrack = item.(*mediaprovider.Track)
		if isTrack && p.audiocache != nil {
			if filepath := p.audiocache.PathForCachedFile(track.ID); filepath != "" {
				url = filepath
			}
		}
	}

	var err error
	if urlP, ok := pl.(player.URLPlayer); ok {
		err = urlP.PlayFile(url, meta, stat.TimePos)
	} else if trackP, ok := pl.(player.TrackPlayer); ok && isTrack {
		err = trackP.PlayTrack(track, stat.TimePos)
	} else if stat.State != player.Stopped {
		return errors.New("unknown player type")
	}

	if err != nil {
		return err
	}

	if stat.State == player.Paused {
		err = pl.Pause()
	} else if stat.State == player.Playing {
		err = pl.Continue()
	}
	p.mu.Lock()
	p.pendingPlayerChange = false
	p.mu.Unlock()

	if needToUnpause {
		p.Continue()
	}

	return err
}

// ======================= START PLAY QUEUE FUNCS ===========================

func (p *playbackEngine) getPlayQueue() []mediaprovider.MediaItem {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.playQueue
}

func (p *playbackEngine) getShuffledPlayQueue() []mediaprovider.MediaItem {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.shuffledPlayQueue
}

func (p *playbackEngine) getActivePlayQueue() []mediaprovider.MediaItem {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getActivePlayQueueUnsafe()
}

func (p *playbackEngine) getActivePlayQueueUnsafe() []mediaprovider.MediaItem {
	if p.shuffle {
		return p.shuffledPlayQueue
	}
	return p.playQueue
}

func (p *playbackEngine) getPlayQueueLengthUnsafe() int {
	return len(p.getActivePlayQueueUnsafe())
}

func (p *playbackEngine) getPlayQueueItemAtUnsafe(idx int) mediaprovider.MediaItem {
	return p.getActivePlayQueueUnsafe()[idx]
}

func (p *playbackEngine) getPlayQueueLength() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getPlayQueueLengthUnsafe()
}

func (p *playbackEngine) clearPlayQueue() {
	p.checkScrobble()
	p.mu.Lock()
	p.playQueue = nil
	p.shuffledPlayQueue = nil
	p.nowPlayingIdx = -1
	p.isRadio = false
	p.mu.Unlock()
}

func (p *playbackEngine) setPlayQueue(items []mediaprovider.MediaItem) {
	p.mu.Lock()
	p.playQueue = items
	p.mu.Unlock()
}

func (p *playbackEngine) setShuffledPlayQueue(items []mediaprovider.MediaItem) {
	p.mu.Lock()
	p.shuffledPlayQueue = items
	p.mu.Unlock()
}

func (p *playbackEngine) getPlayQueueItemAt(idx int) mediaprovider.MediaItem {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getPlayQueueItemAtUnsafe(idx)
}

func (p *playbackEngine) insertItemsIntoPlayQueueAt(items []mediaprovider.MediaItem, idx int, queueType QueueType) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch queueType {
	case PlayQueue:
		p.playQueue = slices.Insert(p.playQueue, idx, items...)
	case ShuffledPlayQueue:
		p.shuffledPlayQueue = slices.Insert(p.shuffledPlayQueue, idx, items...)
	case Both:
		p.playQueue = append(p.playQueue, items...)
		p.shuffledPlayQueue = slices.Insert(p.shuffledPlayQueue, idx, items...)
	}
}

func (p *playbackEngine) GetPlayQueueDeepCopy() []mediaprovider.MediaItem {
	return deepCopyMediaItemSlice(p.getPlayQueue())
}

func (p *playbackEngine) GetShuffledPlayQueueDeepCopy() []mediaprovider.MediaItem {
	return deepCopyMediaItemSlice(p.getShuffledPlayQueue())
}

func (p *playbackEngine) GetActivePlayQueueDeepCopy() []mediaprovider.MediaItem {
	return deepCopyMediaItemSlice(p.getActivePlayQueue())
}

// ======================== END PLAY QUEUE FUNCS =============================

func (p *playbackEngine) PlayTrackAt(idx int) error {
	p.mu.Lock()
	if idx < 0 || idx >= p.getPlayQueueLengthUnsafe() {
		p.mu.Unlock()
		return errors.New("Play queue index out of bounds")
	}
	p.nowPlayingIdx = idx
	p.mu.Unlock()

	p.invokeNoArgCallbacks(p.onQueueChange)
	p.invokeOnSongChangeCallbacks()
	p.reportPlayback("playing")
	p.handleNextTrackUpdated()
	p.checkScrobble()
	return p.playTrackAt(idx, 0)
}

// loadTrackPaused sets up engine state as if the track at idx is loaded and
// paused at startTime, firing UI/OS-integration callbacks, but does NOT touch
// the underlying player. Call Continue() to actually begin playback.
func (p *playbackEngine) loadTrackPaused(idx int, startTime float64) error {
	p.mu.Lock()
	if l := p.getPlayQueueLengthUnsafe(); idx < 0 || idx >= l {
		p.mu.Unlock()
		return fmt.Errorf("track index (%d) out of range (0-%d)", idx, l)
	}
	p.nowPlayingIdx = idx
	nowPlaying := p.getPlayQueueItemAtUnsafe(idx)
	_, p.isRadio = nowPlaying.(*mediaprovider.RadioStation)
	p.wasStopped = false
	p.alreadyScrobbled = false
	p.curTrackDuration = nowPlaying.Metadata().Duration.Seconds()
	p.pendingLoadPaused = true
	p.pendingLoadStartTime = startTime
	p.mu.Unlock()

	// Pre-generate the waveform image for the paused track using the same hook
	// that normally pre-generates waveforms for the upcoming track.
	for _, cb := range p.onBeforeSongChange {
		cb(nowPlaying)
	}

	p.invokeOnSongChangeCallbacks()
	p.invokeNoArgCallbacks(p.onPaused)
	p.handleTimePosUpdate(false)
	p.handleNextTrackUpdated()
	return nil
}

func (p *playbackEngine) playTrackAt(idx int, startTime float64) error {
	p.mu.Lock()
	p.pendingLoadPaused = false
	p.pendingTrackChangeNum = idx
	p.mu.Unlock()
	err := p.setTrack(idx, false, startTime)
	return err
}

// Gets the curently playing media item, if any.
func (p *playbackEngine) NowPlaying() mediaprovider.MediaItem {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.nowPlayingIdx < 0 || p.getPlayQueueLengthUnsafe() == 0 {
		return nil
	}
	if !p.pendingLoadPaused && p.player.GetStatus().State == player.Stopped {
		return nil
	}
	return p.getPlayQueueItemAtUnsafe(p.nowPlayingIdx)
}

func (p *playbackEngine) NowPlayingIndex() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nowPlayingIdx
}

func (p *playbackEngine) SetLoopMode(loopMode LoopMode) {
	p.mu.Lock()
	p.loopMode = loopMode
	nowPlayingIdx := p.nowPlayingIdx
	p.mu.Unlock()
	if nowPlayingIdx >= 0 {
		p.handleNextTrackUpdated()
	}
	for _, cb := range p.onLoopModeChange {
		cb(loopMode)
	}
}

func (p *playbackEngine) GetLoopMode() LoopMode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.loopMode
}

func (p *playbackEngine) GetTrackIdxByIdFrom(items []mediaprovider.MediaItem, id string) int {
	foundIdx := -1
	for i, tr := range items {
		if tr.Metadata().ID == id {
			foundIdx = i
			break
		}
	}
	return foundIdx
}

func (p *playbackEngine) SetShuffle(shuffle bool) {
	p.mu.Lock()
	if p.shuffle == shuffle {
		p.mu.Unlock()
		return
	}
	p.shuffle = shuffle
	p.mu.Unlock()

	for _, cb := range p.onShuffleChange {
		cb(shuffle)
	}

	p.mu.Lock()
	if p.playQueue == nil || len(p.playQueue) == 0 {
		p.mu.Unlock()
		return
	}

	newNowPlayingIdx := 0
	if shuffle {
		shuffledQueue := deepCopyMediaItemSlice(p.playQueue)
		rand.Shuffle(len(shuffledQueue), func(i, j int) {
			shuffledQueue[i], shuffledQueue[j] = shuffledQueue[j], shuffledQueue[i]
		})
		if p.nowPlayingIdx >= 0 && len(p.playQueue) > p.nowPlayingIdx {
			nowPlayingID := p.playQueue[p.nowPlayingIdx].Metadata().ID
			p.shuffledPlayQueue = sharedutil.ReorderItems(shuffledQueue, []int{p.GetTrackIdxByIdFrom(shuffledQueue, nowPlayingID)}, 0)
		} else {
			p.mu.Unlock()
			return
		}
	} else {
		if p.nowPlayingIdx >= 0 && len(p.shuffledPlayQueue) > p.nowPlayingIdx {
			nowPlayingID := p.shuffledPlayQueue[p.nowPlayingIdx].Metadata().ID
			newNowPlayingIdx = p.GetTrackIdxByIdFrom(p.playQueue, nowPlayingID)
		} else {
			p.mu.Unlock()
			return
		}
	}
	p.nowPlayingIdx = newNowPlayingIdx
	p.mu.Unlock()

	p.handleNextTrackUpdated()
	p.invokeNoArgCallbacks(p.onQueueChange)
}

func (p *playbackEngine) PlaybackStatus() PlaybackStatus {
	p.mu.RLock()
	pendingLoadPaused := p.pendingLoadPaused
	pendingLoadStartTime := p.pendingLoadStartTime
	pendingPlayerChange := p.pendingPlayerChange
	pendingPlayerChangeStatus := p.pendingPlayerChangeStatus
	curTrackDuration := p.curTrackDuration
	currentPlayer := p.CurrentPlayerUnsafe()
	p.mu.RUnlock()
	if pendingLoadPaused {
		return PlaybackStatus{
			State:    player.Paused,
			TimePos:  pendingLoadStartTime,
			Duration: curTrackDuration,
		}
	}

	var stat player.Status
	if pendingPlayerChange {
		stat = pendingPlayerChangeStatus
	} else if currentPlayer != nil {
		stat = currentPlayer.GetStatus()
	}
	return PlaybackStatus{
		State:    stat.State,
		TimePos:  stat.TimePos,
		Duration: curTrackDuration,
	}
}

func (p *playbackEngine) SetVolume(vol int) error {
	return p.CurrentPlayer().SetVolume(vol)
}

func (p *playbackEngine) CurrentPlayerUnsafe() player.BasePlayer {
	return p.player
}
func (p *playbackEngine) CurrentPlayer() player.BasePlayer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.player
}

func (p *playbackEngine) SeekNext() error {
	if p.PlaybackStatus().State == player.Stopped {
		return nil
	}
	return p.PlayTrackAt(p.NowPlayingIndex() + 1)
}

func (p *playbackEngine) SeekBackOrPrevious() error {
	idx := p.NowPlayingIndex()
	if idx == 0 || p.PlaybackStatus().TimePos > 3 {
		return p.player.SeekSeconds(0)
	}
	return p.PlayTrackAt(idx - 1)
}

func (p *playbackEngine) SeekFwdBackN(n int) error {
	idx := p.NowPlayingIndex()
	if n < 0 && p.PlaybackStatus().TimePos > 3 {
		n += 1 // first seek back is just seek to beginning of current
	}
	return p.PlayTrackAt(idx + n)
}

// Seek to given absolute position in the current track by seconds.
func (p *playbackEngine) SeekSeconds(sec float64) error {
	return p.CurrentPlayer().SeekSeconds(sec)
}

func (p *playbackEngine) IsSeeking() bool {
	return p.CurrentPlayer().IsSeeking()
}

func (p *playbackEngine) Stop() error {
	p.mu.Lock()
	p.pendingLoadPaused = false
	p.mu.Unlock()
	return p.CurrentPlayer().Stop(true)
}

func (p *playbackEngine) SetPauseAfterCurrent(pauseAfterCurrent bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pauseAfterCurrent = pauseAfterCurrent
}

func (p *playbackEngine) Pause() error {
	stat := p.PlaybackStatus()
	if stat.State == player.Paused {
		return nil
	}
	p.mu.Lock()
	if p.pendingPlayerChange {
		p.pendingPlayerChangeStatus.State = player.Paused
	}
	p.mu.Unlock()
	return p.CurrentPlayer().Pause()
}

func (p *playbackEngine) Continue() error {
	stat := p.PlaybackStatus()
	p.mu.RLock()
	idx := p.nowPlayingIdx
	p.mu.RUnlock()
	if stat.State == player.Stopped && idx >= 0 {
		return p.PlayTrackAt(idx)
	}
	p.mu.Lock()
	p.pendingLoadPaused = false
	if p.pendingPlayerChange {
		p.pendingPlayerChangeStatus.State = player.Playing
	}
	p.mu.Unlock()
	return p.CurrentPlayer().Continue()
}

// Load items into the specified queue(s). This overrides the queue. For standard inserts or replaces use p.LoadItems
// This is used on program startup to populate both the playQueue and shuffledPlayQueue with the previously saved client state.
func (p *playbackEngine) SetQueueState(tracks []*mediaprovider.Track, queueType QueueType) error {
	newTracks := sharedutil.CopyTrackSliceToMediaItemSlice(tracks)
	switch queueType {
	case PlayQueue:
		p.setPlayQueue(newTracks)
	case ShuffledPlayQueue:
		p.setShuffledPlayQueue(newTracks)
	case Both:
		p.setPlayQueue(newTracks)
		p.setShuffledPlayQueue(newTracks)
	}
	p.invokeNoArgCallbacks(p.onQueueChange)
	return nil
}

// Load items into the play queue.
// If replacing the current queue (!appendToQueue), playback will be stopped.
func (p *playbackEngine) LoadItems(items []mediaprovider.MediaItem, insertQueueMode InsertQueueMode, shuffle bool) error {
	newItems := deepCopyMediaItemSlice(items)
	return p.doLoaditems(newItems, insertQueueMode, shuffle)
}

// Load items into the play queue and play the track at idx.
// Replaces the playQueue, shuffledPlayQueue, or both
func (p *playbackEngine) LoadItemsAndPlayAtIdx(items []mediaprovider.MediaItem, shuffle bool, idx int) error {
	newItems := deepCopyMediaItemSlice(items)

	p.mu.Lock()
	isShuffle := p.shuffle
	p.mu.Unlock()

	if isShuffle || shuffle {
		p.clearPlayQueue()
		rand.Shuffle(len(newItems), func(i, j int) {
			newItems[i], newItems[j] = newItems[j], newItems[i]
		})
		if idx < len(items) {
			nowPlayingID := items[idx].Metadata().ID
			p.setPlayQueue(deepCopyMediaItemSlice(items))
			p.setShuffledPlayQueue(sharedutil.ReorderItems(newItems, []int{p.GetTrackIdxByIdFrom(newItems, nowPlayingID)}, 0))
			p.PlayTrackAt(0)
		}
	} else {
		p.doLoaditems(newItems, Replace, shuffle)
		p.PlayTrackAt(idx)
	}

	p.handleNextTrackUpdated()
	p.invokeNoArgCallbacks(p.onQueueChange)

	return nil
}

// Load tracks into the play queue.
// If replacing the current queue (!appendToQueue), playback will be stopped.
func (p *playbackEngine) LoadTracks(tracks []*mediaprovider.Track, insertQueueMode InsertQueueMode, shuffle bool) error {
	newTracks := sharedutil.CopyTrackSliceToMediaItemSlice(tracks)
	return p.doLoaditems(newTracks, insertQueueMode, shuffle)
}

func (p *playbackEngine) doLoaditems(items []mediaprovider.MediaItem, insertQueueMode InsertQueueMode, shuffle bool) error {
	queueType := PlayQueue

	if insertQueueMode == Replace {
		p.clearPlayQueue()
	}
	p.mu.RLock()
	nowPlayingIdx := p.nowPlayingIdx
	queueLen := p.getPlayQueueLengthUnsafe()
	isShuffle := p.shuffle
	p.mu.RUnlock()
	if nextChanged := len(items) > 0 && (insertQueueMode != Append || (nowPlayingIdx == queueLen-1)); nextChanged {
		defer p.handleNextTrackUpdated()
	}

	if shuffle {
		rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	}

	insertIdx := queueLen

	if insertQueueMode == Replace {
		if isShuffle {
			shuffledItems := deepCopyMediaItemSlice(items)
			rand.Shuffle(len(shuffledItems), func(i, j int) { shuffledItems[i], shuffledItems[j] = shuffledItems[j], shuffledItems[i] })

			p.insertItemsIntoPlayQueueAt(items, insertIdx, PlayQueue)
			p.insertItemsIntoPlayQueueAt(shuffledItems, insertIdx, ShuffledPlayQueue)
		} else {
			p.insertItemsIntoPlayQueueAt(items, insertIdx, queueType)
		}
	} else {
		if insertQueueMode == InsertNext {
			insertIdx = nowPlayingIdx + 1
		}
		if isShuffle {
			queueType = Both
		}
		p.insertItemsIntoPlayQueueAt(items, insertIdx, queueType)
	}

	p.invokeNoArgCallbacks(p.onQueueChange)
	return nil
}

func (p *playbackEngine) LoadRadioStation(radio *mediaprovider.RadioStation, insertMode InsertQueueMode) {
	p.mu.Lock()
	p.isRadio = true
	p.shuffle = false
	p.mu.Unlock()
	if insertMode == Replace {
		p.clearPlayQueue()
	}
	p.mu.RLock()
	nowPlayingIdx := p.nowPlayingIdx
	queueLen := p.getPlayQueueLengthUnsafe()
	p.mu.RUnlock()
	if nextChanged := insertMode == InsertNext || (insertMode == Append && nowPlayingIdx == queueLen-1); nextChanged {
		p.handleNextTrackUpdated()
	}
	insertIdx := queueLen
	if insertMode == InsertNext {
		insertIdx = nowPlayingIdx + 1
	}
	p.insertItemsIntoPlayQueueAt([]mediaprovider.MediaItem{radio}, insertIdx, PlayQueue)

	p.invokeNoArgCallbacks(p.onQueueChange)
	if insertMode == Replace {
		p.PlayTrackAt(0)
	}
}

// Stop playback and clear the play queue.
func (p *playbackEngine) StopAndClearPlayQueue() {
	p.Stop()
	p.clearPlayQueue()
	p.invokeNoArgCallbacks(p.onQueueChange)
	p.invokeOnSongChangeCallbacks()
	p.handleNextTrackUpdated()
}

// Any time the user changes the favorite status of a track elsewhere in the app,
// this should be called to ensure the in-memory track model is updated.
func (p *playbackEngine) OnTrackFavoriteStatusChanged(id string, fav bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if item := sharedutil.FindMediaItemByID(id, p.getActivePlayQueueUnsafe()); item != nil {
		if tr, ok := item.(*mediaprovider.Track); ok {
			tr.Favorite = fav
		}
	}
}

// Any time the user changes the rating of a track elsewhere in the app,
// this should be called to ensure the in-memory track model is updated.
func (p *playbackEngine) OnTrackRatingChanged(id string, rating int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if item := sharedutil.FindMediaItemByID(id, p.getActivePlayQueueUnsafe()); item != nil {
		if tr, ok := item.(*mediaprovider.Track); ok {
			tr.Rating = rating
		}
	}
}

// Replaces the play queue with the given set of tracks.
// Does not stop playback if the currently playing track is in the new queue,
// but updates the now playing index to point to the first instance of the track in the new queue.
func (p *playbackEngine) UpdatePlayQueue(items []mediaprovider.MediaItem) error {
	p.mu.Lock()
	newQueue := deepCopyMediaItemSlice(items)
	newNowPlayingIdx := -1
	if p.nowPlayingIdx >= 0 {
		nowPlayingID := p.getActivePlayQueueUnsafe()[p.nowPlayingIdx].Metadata().ID
		for i, tr := range newQueue {
			if tr.Metadata().ID == nowPlayingID {
				newNowPlayingIdx = i
				break
			}
		}
	}
	if p.shuffle {
		p.shuffledPlayQueue = newQueue
	} else {
		p.playQueue = newQueue
	}
	wasPlaying := p.nowPlayingIdx >= 0
	p.nowPlayingIdx = newNowPlayingIdx
	p.mu.Unlock()
	if wasPlaying && newNowPlayingIdx == -1 {
		return p.Stop()
	}
	if wasPlaying {
		p.handleNextTrackUpdated()
	}

	p.invokeNoArgCallbacks(p.onQueueChange)
	return nil
}

func (p *playbackEngine) RemoveTracksFromQueue(idxs []int) {
	p.mu.Lock()
	isPlayingTrackRemoved := false
	isNextPlayingTrackremoved := false
	nowPlaying := p.nowPlayingIdx
	newNowPlaying := nowPlaying

	if p.shuffle {
		ids := p.getTrackIdsFromIdxUnsafe(idxs)
		newPlayQueue := make([]mediaprovider.MediaItem, 0, p.getPlayQueueLengthUnsafe()-len(idxs))
		for _, tr := range p.playQueue {
			if slices.Contains(ids, tr.Metadata().ID) {
				idx := slices.Index(ids, tr.Metadata().ID)
				ids = slices.Delete(ids, idx, idx+1)
			} else {
				newPlayQueue = append(newPlayQueue, tr)
			}
		}
		p.playQueue = newPlayQueue

		newShuffledQueue := make([]mediaprovider.MediaItem, 0, p.getPlayQueueLengthUnsafe()-len(idxs))
		p.removeTracksFromQueueByIdxUnsafe(idxs, &newShuffledQueue, &newNowPlaying, &isPlayingTrackRemoved, &isNextPlayingTrackremoved)
		p.shuffledPlayQueue = newShuffledQueue
	} else {
		newPlayQueue := make([]mediaprovider.MediaItem, 0, p.getPlayQueueLengthUnsafe()-len(idxs))
		p.removeTracksFromQueueByIdxUnsafe(idxs, &newPlayQueue, &newNowPlaying, &isPlayingTrackRemoved, &isNextPlayingTrackremoved)
		p.playQueue = newPlayQueue
	}

	p.nowPlayingIdx = newNowPlaying
	newQueueLength := p.getPlayQueueLengthUnsafe()
	p.mu.Unlock()

	if isPlayingTrackRemoved {
		if newNowPlaying == newQueueLength {
			p.Stop()
		} else {
			p.mu.Lock()
			p.nowPlayingIdx -= 1
			p.mu.Unlock()
			p.setTrack(newNowPlaying, false, 0)
		}
	} else if isNextPlayingTrackremoved {
		if newNowPlaying < newQueueLength-1 {
			p.handleNextTrackUpdated()
		} else {
			p.setNextTrack(-1)
		}
	}

	p.invokeNoArgCallbacks(p.onQueueChange)
}

func (p *playbackEngine) removeTracksFromQueueByIdxUnsafe(idxs []int, newQueue *[]mediaprovider.MediaItem, newNowPlaying *int, isPlayingTrackRemoved *bool, isNextPlayingTrackRemoved *bool) {
	idxSet := sharedutil.ToSet(idxs)
	nowPlaying := p.nowPlayingIdx

	for i, tr := range p.getActivePlayQueueUnsafe() {
		if _, ok := idxSet[i]; ok {
			if i < nowPlaying {
				*newNowPlaying--
			} else if i == nowPlaying {
				*isPlayingTrackRemoved = true
				// Note: checkScrobble takes a lock, so we temporarily release it
				p.mu.Unlock()
				p.checkScrobble()
				p.mu.Lock()
				p.alreadyScrobbled = true
			} else if nowPlaying >= 0 && i == nowPlaying+1 {
				*isNextPlayingTrackRemoved = true
			}
		} else {
			*newQueue = append(*newQueue, tr)
		}
	}
}

func (p *playbackEngine) GetTrackIdsFromIdx(idx []int) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getTrackIdsFromIdxUnsafe(idx)
}
func (p *playbackEngine) getTrackIdsFromIdxUnsafe(idx []int) []string {
	ids := make([]string, len(idx))
	for i, id := range idx {
		ids[i] = p.getActivePlayQueueUnsafe()[id].Metadata().ID
	}
	return ids
}

func (p *playbackEngine) SetReplayGainOptions(config ReplayGainConfig) {
	rGainPlayer, ok := p.CurrentPlayer().(player.ReplayGainPlayer)
	if !ok {
		log.Println("Error: player doesn\x27t support ReplayGain")
		return
	}

	p.mu.Lock()
	p.replayGainCfg = config
	p.mu.Unlock()
	mode := player.ReplayGainNone
	switch config.Mode {
	case ReplayGainAuto:
		mode = player.ReplayGainTrack
	case ReplayGainTrack:
		mode = player.ReplayGainTrack
	case ReplayGainAlbum:
		mode = player.ReplayGainAlbum
	}

	rGainPlayer.SetReplayGainOptions(player.ReplayGainOptions{
		Mode:            mode,
		PreventClipping: config.PreventClipping,
		PreampGain:      config.PreampGainDB,
	})
}

func (p *playbackEngine) SetReplayGainMode(mode player.ReplayGainMode) {
	rGainPlayer, ok := p.CurrentPlayer().(player.ReplayGainPlayer)
	if !ok {
		log.Println("Error: player doesn\x27t support ReplayGain")
		return
	}
	p.mu.RLock()
	cfg := p.replayGainCfg
	p.mu.RUnlock()
	rGainPlayer.SetReplayGainOptions(player.ReplayGainOptions{
		PreventClipping: cfg.PreventClipping,
		PreampGain:      cfg.PreampGainDB,
		Mode:            mode,
	})
}

func (p *playbackEngine) cacheNextTracks() {
	if p.audiocache != nil {
		// fetch up to the 2 next tracks in the queue to the cache
		fetch := make([]AudioCacheRequest, 0, 3)
		// if nothing is playing (index = -1), treat the beginning of the queue as
		// the "currently" playing track, since we're probably about to play it
		p.mu.RLock()
		npI := max(p.nowPlayingIdx, 0)
		queueLen := p.getPlayQueueLengthUnsafe()
		for _, idx := range [3]int{npI, npI + 1, npI + 2} {
			if idx >= 0 && idx < queueLen {
				item := p.getPlayQueueItemAtUnsafe(idx)
				if item.Metadata().Type == mediaprovider.MediaItemTypeTrack {
					fetch = append(fetch, AudioCacheRequest{
						ID:          item.Metadata().ID,
						DownloadURL: p.getMediaURLForIdxUnsafe(idx),
					})
				}
			}
		}
		var np mediaprovider.MediaItem
		if p.nowPlayingIdx >= 0 && p.nowPlayingIdx < queueLen && p.getPlayQueueLengthUnsafe() > 0 {
			np = p.getPlayQueueItemAtUnsafe(p.nowPlayingIdx)
		}
		p.mu.RUnlock()
		id := ""
		if np != nil {
			id = np.Metadata().ID
		}
		p.audiocache.CacheOnly(id, fetch)
	}
}

func (p *playbackEngine) handleOnTrackChange() {
	// scrobble the previous song if needed
	p.mu.RLock()
	alreadyScrobbled := p.alreadyScrobbled
	p.mu.RUnlock()
	if !alreadyScrobbled {
		p.checkScrobble()
	}

	if p.PlaybackStatus().State == player.Playing {
		p.playTimeStopwatch.Start()
	}
	p.mu.Lock()
	if p.pendingTrackChangeNum < 0 && (p.wasStopped || p.loopMode != LoopOne) {
		p.nowPlayingIdx++
		if p.loopMode == LoopAll && p.nowPlayingIdx == p.getPlayQueueLengthUnsafe() {
			p.nowPlayingIdx = 0 // wrapped around
		}
	} else if p.pendingTrackChangeNum >= 0 {
		p.nowPlayingIdx = p.pendingTrackChangeNum
		p.pendingTrackChangeNum = -1
	}
	nowPlaying := p.getPlayQueueItemAtUnsafe(p.nowPlayingIdx)
	_, isRadio := nowPlaying.(*mediaprovider.RadioStation)
	p.isRadio = isRadio

	// reset flags
	p.wasStopped = false
	p.alreadyScrobbled = false

	p.curTrackDuration = nowPlaying.Metadata().Duration.Seconds()
	pauseAfter := p.pauseAfterCurrent
	p.mu.Unlock()

	p.sendNowPlayingScrobble() // Must come before invokeOnChangeCallbacks b/c track may immediately be scrobbled
	p.invokeOnSongChangeCallbacks()
	p.handleTimePosUpdate(false)
	p.handleNextTrackUpdated()

	if pauseAfter {
		p.Pause()
		p.SetPauseAfterCurrent(false)
	}
}

func (p *playbackEngine) handleOnStopped() {
	p.playTimeStopwatch.Stop()
	p.mu.RLock()
	alreadyScrobbled := p.alreadyScrobbled
	p.mu.RUnlock()
	if !alreadyScrobbled {
		p.checkScrobble()
	}
	p.stopPollTimePos()
	p.handleTimePosUpdate(false)
	p.invokeOnSongChangeCallbacks()
	p.invokeNoArgCallbacks(p.onStopped)
	p.reportPlayback("stopped")
	p.mu.Lock()
	p.alreadyScrobbled = false
	p.wasStopped = true
	p.nowPlayingIdx = -1
	p.pauseAfterCurrent = false
	p.mu.Unlock()
}

// to be invoked as soon as the next item in the queue that should play changes
func (p *playbackEngine) handleNextTrackUpdated() {
	p.cacheNextTracks()
	p.mu.Lock()
	p.needToSetNextTrack = true
	p.mu.Unlock()
	for _, cb := range p.onBeforeSongChange {
		var item mediaprovider.MediaItem
		if idx := p.nextPlayingIndex(); idx >= 0 {
			p.mu.RLock()
			item = p.getPlayQueueItemAtUnsafe(idx)
			p.mu.RUnlock()
		}
		cb(item)
	}
}

func (p *playbackEngine) nextPlayingIndex() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch p.loopMode {
	case LoopNone:
		if p.nowPlayingIdx >= p.getPlayQueueLengthUnsafe()-1 {
			return -1
		}
		return p.nowPlayingIdx + 1
	case LoopOne:
		return p.nowPlayingIdx
	case LoopAll:
		if p.nowPlayingIdx >= p.getPlayQueueLengthUnsafe()-1 {
			return 0
		}
		return p.nowPlayingIdx + 1
	}
	return -1 // unreached
}

func (p *playbackEngine) setTrack(idx int, next bool, startTime float64) error {
	var item mediaprovider.MediaItem
	var url string
	if idx >= 0 {
		p.mu.RLock()
		item = p.getPlayQueueItemAtUnsafe(idx)
		url = p.getMediaURLForIdxUnsafe(idx)
		p.mu.RUnlock()
	}
	var track *mediaprovider.Track
	var isTrack bool
	if item != nil {
		track, isTrack = item.(*mediaprovider.Track)
		if p.audiocache != nil && isTrack {
			p.audiocache.CacheFile(item.Metadata().ID, url)
		}
	}

	currentPlayer := p.CurrentPlayer()
	if urlP, ok := currentPlayer.(player.URLPlayer); ok {
		var meta mediaprovider.MediaItemMetadata
		if idx >= 0 {
			meta = item.Metadata()
			if isTrack && p.audiocache != nil {
				if filepath := p.audiocache.PathForCachedFile(track.ID); filepath != "" {
					url = filepath
				}
			}
			if mpvP, ok := currentPlayer.(*mpv.Player); ok && !isTrack {
				mpvP.ObserveIcyRadioTitle(func(icytitle string) {
					var title, artist string
					if s := strings.Split(icytitle, " - "); len(s) == 2 {
						title, artist = s[1], s[0]
					} else {
						title = icytitle
					}
					for _, cb := range p.onRadioMetadataChange {
						cb(meta.Name, title, artist)
					}
				})
			} else if ok {
				mpvP.UnobserveIcyRadioTitle()
			}
			if url == "" {
				return errors.New("no stream URL")
			}
		}
		if next {
			return urlP.SetNextFile(url, meta)
		}
		return urlP.PlayFile(url, meta, startTime)
	} else if trP, ok := currentPlayer.(player.TrackPlayer); ok {
		if idx >= 0 && !isTrack {
			return errors.New("cannot play non-Track media item with TrackPlayer")
		}
		if next {
			return trP.SetNextTrack(track)
		}
		return trP.PlayTrack(track, startTime)
	}
	panic("Unsupported player type")
}

func (p *playbackEngine) getMediaURLForIdxUnsafe(idx int) string {
	var url string
	item := p.getPlayQueueItemAtUnsafe(idx)
	if tr, ok := item.(*mediaprovider.Track); ok {
		var ts *mediaprovider.TranscodeSettings
		if p.transcodeCfg.RequestTranscode {
			ts = &mediaprovider.TranscodeSettings{
				Codec:       p.transcodeCfg.Codec,
				BitRateKBPS: p.transcodeCfg.MaxBitRateKBPS,
			}
		}
		url, _ = p.sm.Server.GetStreamURL(tr.ID, ts, p.transcodeCfg.ForceRawFile)
	} else {
		url = item.(*mediaprovider.RadioStation).StreamURL
	}
	return url
}
func (p *playbackEngine) getMediaURLForIdx(idx int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getMediaURLForIdxUnsafe(idx)
}

func (p *playbackEngine) setNextTrack(idx int) error {
	return p.setTrack(idx, true, 0)
}

// call BEFORE updating p.nowPlayingIdx
func (p *playbackEngine) checkScrobble() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.scrobbleCfg.Enabled || p.getPlayQueueLengthUnsafe() == 0 || p.nowPlayingIdx < 0 {
		return
	}
	track, ok := p.getPlayQueueItemAtUnsafe(p.nowPlayingIdx).(*mediaprovider.Track)
	if !ok {
		return // radio stations are not scrobbled
	}

	playDur := p.playTimeStopwatch.Elapsed()
	if playDur.Seconds() < 0.1 || p.curTrackDuration < 0.1 {
		return
	}
	pcnt := playDur.Seconds() / p.curTrackDuration * 100
	timeThresholdMet := p.scrobbleCfg.ThresholdTimeSeconds >= 0 &&
		playDur.Seconds() >= float64(p.scrobbleCfg.ThresholdTimeSeconds)

	var submission bool
	server := p.sm.Server
	if server.ClientDecidesScrobble() && (timeThresholdMet || pcnt >= float64(p.scrobbleCfg.ThresholdPercent)) {
		track.PlayCount += 1
		p.lastScrobbled = track
		submission = true
	}
	go server.TrackEndedPlayback(track.ID, int(p.latestTrackPosition), submission)
	p.latestTrackPosition = 0
	p.playTimeStopwatch.Reset()
}

func (p *playbackEngine) sendNowPlayingScrobble() {
	p.mu.Lock()
	if !p.scrobbleCfg.Enabled || p.getPlayQueueLengthUnsafe() == 0 || p.nowPlayingIdx < 0 {
		p.mu.Unlock()
		return
	}
	track, ok := p.getPlayQueueItemAtUnsafe(p.nowPlayingIdx).(*mediaprovider.Track)
	if !ok {
		p.mu.Unlock()
		return // radio stations are not scrobbled
	}
	server := p.sm.Server
	if !server.ClientDecidesScrobble() {
		// server will count track as scrobbled as soon as it starts playing
		p.lastScrobbled = track
		track.PlayCount += 1
	}
	p.mu.Unlock()
	go server.TrackBeganPlayback(track.ID)
	p.reportPlayback("starting")
}

// creates a deep copy of the track info so that we can maintain our own state
// (play count increases, favorite, and rating) without messing up other views' track models
func deepCopyMediaItemSlice(tracks []mediaprovider.MediaItem) []mediaprovider.MediaItem {
	newTracks := make([]mediaprovider.MediaItem, len(tracks))
	for i, tr := range tracks {
		newTracks[i] = tr.Copy()
	}
	return newTracks
}

func (p *playbackEngine) invokeOnSongChangeCallbacks() {
	if p.callbacksDisabled {
		return
	}
	for _, cb := range p.onSongChange {
		cb(p.NowPlaying(), p.lastScrobbled)
	}
	p.lastScrobbled = nil
}

func (p *playbackEngine) reportPlayback(state string) {
	reporter, ok := p.sm.Server.(mediaprovider.CanReportPlayback)
	if !ok {
		return
	}
	if p.getPlayQueueLength() == 0 || p.nowPlayingIdx < 0 {
		return
	}
	np := p.NowPlaying()
	if np == nil || np.Metadata().Type != mediaprovider.MediaItemTypeTrack {
		return
	}
	posMs := int64(p.latestTrackPosition * 1000)
	go reporter.ReportPlayback(np.Metadata().ID, posMs, state)
}

func (pm *playbackEngine) invokeNoArgCallbacks(cbs []func()) {
	if pm.callbacksDisabled {
		return
	}
	for _, cb := range cbs {
		cb()
	}
}

func (p *playbackEngine) startPollTimePos() {
	ctx, cancel := context.WithCancel(p.ctx)
	p.cancelPollPos = cancel
	pollFrequency := 250 * time.Millisecond
	if p.playbackCfg.UseWaveformSeekbar {
		pollFrequency = 100 * time.Millisecond
	}
	pollingTick := time.NewTicker(pollFrequency)

	go func() {
		for {
			select {
			case <-ctx.Done():
				pollingTick.Stop()
				return
			case <-pollingTick.C:
				p.handleTimePosUpdate(false)
			}
		}
	}()
}

func (p *playbackEngine) stopPollTimePos() {
	if p.cancelPollPos != nil {
		p.cancelPollPos()
		p.cancelPollPos = nil
	}
}

func (p *playbackEngine) handleTimePosUpdate(seeked bool) {
	s := p.PlaybackStatus()
	var meta mediaprovider.MediaItemMetadata
	if np := p.NowPlaying(); np != nil {
		meta = np.Metadata()
	}
	isNearEnd := meta.Type != mediaprovider.MediaItemTypeRadioStation && s.TimePos > meta.Duration.Seconds()-10
	if p.needToSetNextTrack && isNearEnd {
		p.needToSetNextTrack = false
		if nextIdx := p.nextPlayingIndex(); nextIdx >= 0 && nextIdx < len(p.playQueue) {
			p.setNextTrack(nextIdx)
		} else {
			// no next track to play, ensure player knows this
			p.setNextTrack(-1)
		}
	}
	if p.callbacksDisabled {
		return
	}
	if s.TimePos > p.latestTrackPosition {
		p.latestTrackPosition = s.TimePos
	}
	duration := s.Duration
	if p.isRadio {
		// MPV reports buffered duration - we don't want to show this
		duration = 0
	}
	for _, cb := range p.onPlayTimeUpdate {
		cb(s.TimePos, duration, seeked)
	}
}
