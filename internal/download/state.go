package download

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Checkpoint is the persisted download state. It is the analogue of IDM's
// per-task ChList registry tree, but stored as an atomic JSON file next to
// the payload so it travels with the data instead of living in a registry.
type Checkpoint struct {
	Version      int            `json:"version"`
	URL          string         `json:"url"`
	FinalURL     string         `json:"final_url"`
	Size         int64          `json:"size"`
	ETag         string         `json:"etag"`
	LastModified string         `json:"last_modified"`
	SavePath     string         `json:"save_path"`
	Segments     []SegmentState `json:"segments"`
	SavedAt      time.Time      `json:"saved_at"`
}

const checkpointVersion = 1

// ValidFor reports whether the checkpoint can resume the probed transfer.
// Like IDM's ETag/Last-Modified check, any validator mismatch forces a fresh
// start: resuming against a changed object would silently corrupt the file.
func (c *Checkpoint) ValidFor(p Probe) bool {
	if c == nil {
		return false
	}
	if c.Version != checkpointVersion {
		return false
	}
	if c.URL != p.URL && c.FinalURL != p.FinalURL {
		return false
	}
	if c.Size != p.Size || c.Size <= 0 {
		return false
	}
	if p.Validator() != "" {
		mine := c.ETag
		if c.LastModified != "" {
			mine = c.LastModified
		}
		if mine != "" && mine != p.Validator() {
			return false
		}
		// If the checkpoint has no validator but the server offers one now,
		// treat the state as untrusted.
		if mine == "" {
			return false
		}
	}
	var covered int64
	for i := range c.Segments {
		s := &c.Segments[i]
		if s.Done {
			covered += s.End - s.Start + 1
			continue
		}
		if s.Off < s.Start || s.Off > s.End+1 {
			return false
		}
		covered += s.Off - s.Start
	}
	return covered >= 0 && covered <= c.Size
}

// StateDir returns the checkpoint directory, deriving it from the engine
// config when StateDirectory is unset.
func StateDir(stateDir, downloadDir string) string {
	if strings.TrimSpace(stateDir) != "" {
		return stateDir
	}
	if strings.TrimSpace(downloadDir) != "" {
		return filepath.Join(downloadDir, ".checkpoints")
	}
	return filepath.Join(os.TempDir(), "idm-checkpoints")
}

// CheckpointPath derives a stable file name from the task identity. The hex
// digest keeps arbitrary URLs filesystem-safe.
func CheckpointPath(dir, taskKey string) string {
	sum := sha256.Sum256([]byte(taskKey))
	return filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
}

// LoadCheckpoint reads and validates a checkpoint file. A nil checkpoint with
// a nil error means "nothing to resume".
func LoadCheckpoint(path string, p Probe) (*Checkpoint, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("download: read checkpoint: %w", err)
	}
	var c Checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("download: decode checkpoint: %w", err)
	}
	if !c.ValidFor(p) {
		return nil, nil
	}
	return &c, nil
}

// SaveCheckpoint persists state atomically: write to a temp sibling and
// rename over the target, so a crash can never leave a half-written file.
func SaveCheckpoint(path string, c *Checkpoint) error {
	if c == nil {
		return nil
	}
	c.SavedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("download: encode checkpoint: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("download: create checkpoint dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ckpt-*")
	if err != nil {
		return fmt.Errorf("download: create checkpoint temp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("download: write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("download: sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("download: close checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("download: commit checkpoint: %w", err)
	}
	return nil
}

// DropCheckpoint removes the checkpoint after a transfer completes.
func DropCheckpoint(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("download: remove checkpoint: %w", err)
	}
	return nil
}

// SnapshotCheckpoint builds a checkpoint from the live table.
func SnapshotCheckpoint(p Probe, table *SegmentTable, savePath string) *Checkpoint {
	return &Checkpoint{
		Version:      checkpointVersion,
		URL:          p.URL,
		FinalURL:     p.FinalURL,
		Size:         p.Size,
		ETag:         p.ETag,
		LastModified: p.LastModified,
		SavePath:     savePath,
		Segments:     table.Segments(),
	}
}
