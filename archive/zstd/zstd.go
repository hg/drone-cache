package zstd

import (
	"fmt"
	"io"

	"github.com/meltwater/drone-cache/archive/tar"
	"github.com/meltwater/drone-cache/internal"

	"github.com/go-kit/kit/log"
	"github.com/klauspost/compress/zstd"
)

type Archive struct {
	logger log.Logger

	root             string
	compressionLevel zstd.EncoderLevel
	skipSymlinks     bool
}

// New creates an archive that uses the .tar.zst file format.
func New(logger log.Logger, root string, skipSymlinks bool, compressionLevel int) *Archive {
	var level zstd.EncoderLevel
	if compressionLevel < 0 {
		// No valid value supplied — revert to default.
		level = zstd.SpeedDefault
	} else {
		// The library uses its own compression levels incompatible with what the upstream does.
		// We don't really care about its internals and want to use the same levels as the rest of the world does.
		// This function converts the upstream levels to what the library expects.
		level = zstd.EncoderLevelFromZstd(compressionLevel)
	}
	return &Archive{logger, root, level, skipSymlinks}
}

// Create writes content of the given source to an archive, returns written bytes.
func (a *Archive) Create(srcs []string, w io.Writer) (int64, error) {
	zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(a.compressionLevel))
	if err != nil {
		return 0, fmt.Errorf("create archive writer, %w", err)
	}

	defer internal.CloseWithErrLogf(a.logger, zw, "zstd writer")

	return tar.New(a.logger, a.root, a.skipSymlinks).Create(srcs, zw)
}

// Extract reads content from the given archive reader and restores it to the destination, returns written bytes.
func (a *Archive) Extract(dst string, r io.Reader) (int64, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return 0, err
	}

	defer internal.CloseWithErrLogf(a.logger, zr.IOReadCloser(), "zstd reader")

	return tar.New(a.logger, a.root, a.skipSymlinks).Extract(dst, zr)
}
