package framework_test

import (
	"embed"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
)

// Go compiles white-box tests in the package they exercise. The overlay gives
// the toolchain those paths while keeping the test sources together under test/.
// Embedding also invalidates the outer test cache when a white-box test changes.
//
//go:embed testdata/wskit/*_test.go testdata/example/node/*_test.go
var whiteBoxFiles embed.FS

func TestPackageInternals(t *testing.T) {
	runPackageTests(t, "testdata/wskit/*_test.go", ".")
}

func TestExampleNode(t *testing.T) {
	runPackageTests(t, "testdata/example/node/*_test.go", "example/node")
}

func runPackageTests(t *testing.T, pattern, packageDir string) {
	t.Helper()
	testDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(testDir, "..", filepath.FromSlash(packageDir))
	files, err := fs.Glob(whiteBoxFiles, pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no tests matched %s", pattern)
	}
	replace := make(map[string]string, len(files))
	for _, name := range files {
		replace[filepath.Join(dir, filepath.Base(name))] = filepath.Join(testDir, filepath.FromSlash(name))
	}
	overlay, err := json.Marshal(struct{ Replace map[string]string }{replace})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "overlay.json")
	if err := os.WriteFile(path, overlay, 0600); err != nil {
		t.Fatal(err)
	}

	args := []string{"test", "-count=1", "-overlay", path}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				args = append(args, "-race")
			}
		}
	}
	if testing.Short() {
		args = append(args, "-short")
	}
	if testing.Verbose() {
		args = append(args, "-v")
	}
	cmd := exec.CommandContext(t.Context(), filepath.Join(runtime.GOROOT(), "bin", "go"), append(args, ".")...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("package tests: %v\n%s", err, output)
	}
	t.Logf("\n%s", output)
}
