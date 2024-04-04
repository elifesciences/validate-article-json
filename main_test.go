package main

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_format_ms(t *testing.T) {
	cases := map[int64]string{
		1:     "1ms",
		100:   "100ms",
		1000:  "1s",
		60000: "1m",
	}
	for given, expected := range cases {
		assert.Equal(t, expected, format_ms(given))
	}
}

func Test_path_exists(t *testing.T) {
	tmp := t.TempDir()
	assert.True(t, path_exists(tmp))
}

func Test_path_is_dir(t *testing.T) {
	tmp := t.TempDir()
	assert.True(t, path_is_dir(tmp))

	tmp_file := path.Join(tmp, "foo")
	os.WriteFile(tmp_file, []byte("bar"), 0644)
	assert.False(t, path_is_dir(tmp_file))
}

func Test_assert_panic_on_err(t *testing.T) {
	assert.NotPanics(t, func() {
		panic_on_err(nil, "pressing a red button")
	})
	assert.Panics(t, func() {
		panic_on_err(errors.New("kaboom"), "pressing a red button")
	})
}
