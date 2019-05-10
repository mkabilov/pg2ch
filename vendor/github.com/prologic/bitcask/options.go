package bitcask

const (
	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 1 << 20 // 1MB

	// DefaultMaxKeySize is the default maximum key size in bytes
	DefaultMaxKeySize = 64 // 64 bytes

	// DefaultMaxValueSize is the default value size in bytes
	DefaultMaxValueSize = 1 << 16 // 65KB
)

// Option is a function that takes a config struct and modifies it
type Option func(*config) error

type config struct {
	maxDatafileSize int
	maxKeySize      int
	maxValueSize    int
}

func newDefaultConfig() *config {
	return &config{
		maxDatafileSize: DefaultMaxDatafileSize,
		maxKeySize:      DefaultMaxKeySize,
		maxValueSize:    DefaultMaxValueSize,
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) Option {
	return func(cfg *config) error {
		cfg.maxDatafileSize = size
		return nil
	}
}

// WithMaxKeySize sets the maximum key size option
func WithMaxKeySize(size int) Option {
	return func(cfg *config) error {
		cfg.maxKeySize = size
		return nil
	}
}

// WithMaxValueSize sets the maximum value size option
func WithMaxValueSize(size int) Option {
	return func(cfg *config) error {
		cfg.maxValueSize = size
		return nil
	}
}
