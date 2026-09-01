package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// stateVersion 2 added the per-mirror source table. A v1 sidecar is rejected
// rather than guessed at, which costs a restart once and never risks mixing
// mirrors whose validators were never recorded.
const stateVersion = 2

// validator carries whatever the server gave us to detect that the remote
// file changed under a resumed download. An ETag is authoritative;
// Last-Modified is a weaker fallback.
type validator struct {
	etag         string
	lastModified string
}

type stateSegment struct {
	Start int64 `json:"start"`
	Pos   int64 `json:"pos"`
	End   int64 `json:"end"`
}

// stateSource records one mirror and the validator it issued. Validators are
// stored per mirror because they are per mirror: two servers hosting the same
// bytes will hand out different ETags, and comparing one against the other
// would reject every resume.
type stateSource struct {
	URL          string `json:"url"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
}

// downloadState is the .pearl sidecar written next to the output file. It is
// small (a few hundred bytes) and rewritten atomically, so an interrupted
// download resumes from the last checkpoint instead of from zero.
type downloadState struct {
	Version    int            `json:"version"`
	Output     string         `json:"output"`
	TotalSize  int64          `json:"total_size"`
	Downloaded int64          `json:"downloaded"`
	Updated    time.Time      `json:"updated"`
	Sources    []stateSource  `json:"sources"`
	Segments   []stateSegment `json:"segments"`
}

func statePathFor(outputPath string) string { return outputPath + ".pearl" }

func loadState(path string) (*downloadState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s downloadState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if s.Version != stateVersion {
		return nil, fmt.Errorf("%s: unsupported state version %d", path, s.Version)
	}
	return &s, nil
}

// resumable reports whether a saved state describes the same bytes the
// servers are offering now. A changed ETag, Last-Modified, or size means the
// partial file on disk is a mix of two different files and must be discarded.
//
// Mirrors are matched by URL: a mirror present in both runs must still be
// serving what it served before, and at least one has to overlap. A resume
// against an entirely new set of mirrors is refused, because nothing then
// ties the bytes already on disk to the bytes about to be fetched.
func (s *downloadState) resumable(size int64, sources []source) bool {
	if s.TotalSize != size || len(s.Segments) == 0 {
		return false
	}
	overlap := 0
	for _, current := range sources {
		for _, saved := range s.Sources {
			if saved.URL != current.url {
				continue
			}
			overlap++
			if !saved.matches(current.validator) {
				return false
			}
		}
	}
	return overlap > 0
}

func (s stateSource) matches(v validator) bool {
	switch {
	case s.ETag != "" && v.etag != "":
		return s.ETag == v.etag
	case s.LastModified != "" && v.lastModified != "":
		return s.LastModified == v.lastModified
	case s.ETag != "" || s.LastModified != "":
		// We had a validator when the download started but the server is no
		// longer sending one. Size alone is too weak to trust here.
		return false
	}
	return true
}

func (s *downloadState) views() []segView {
	views := make([]segView, 0, len(s.Segments))
	for i, seg := range s.Segments {
		views = append(views, segView{id: i, owner: -1, start: seg.Start, pos: seg.Pos, end: seg.End})
	}
	return views
}

func (s *downloadState) completed() int64 {
	var done int64
	for _, seg := range s.Segments {
		done += seg.Pos - seg.Start
	}
	return done
}

// save writes the state file atomically: a temp file in the same directory,
// then a rename. A crash mid-write can therefore never leave a truncated
// state file that would poison the next resume.
func (s *downloadState) save(path string) error {
	s.Version = stateVersion
	s.Updated = time.Now()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// checkpoint captures the coordinator's current segment table into the state
// file. The caller is responsible for having flushed the data file first:
// state that claims bytes the kernel has not written would resume past a
// hole.
func (d *downloader) checkpoint() error {
	views, _ := d.co.snapshot(nil)
	segments := make([]stateSegment, 0, len(views))
	var downloaded int64
	for _, v := range views {
		segments = append(segments, stateSegment{Start: v.start, Pos: v.pos, End: v.end})
		downloaded += v.pos - v.start
	}
	sources := make([]stateSource, 0, d.sources.count())
	for i := 0; i < d.sources.count(); i++ {
		src := d.sources.at(i)
		sources = append(sources, stateSource{
			URL:          src.url,
			ETag:         src.validator.etag,
			LastModified: src.validator.lastModified,
		})
	}
	state := &downloadState{
		Output:     d.outputPath,
		TotalSize:  d.totalSize,
		Downloaded: downloaded,
		Sources:    sources,
		Segments:   segments,
	}
	return state.save(d.statePath)
}

// syncAndCheckpoint flushes written data to stable storage before recording
// how far we got, so the two can never disagree in the dangerous direction.
func (d *downloader) syncAndCheckpoint() error {
	if err := d.file.Sync(); err != nil {
		return err
	}
	return d.checkpoint()
}
