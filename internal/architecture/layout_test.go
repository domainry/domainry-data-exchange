package architecture

import (
	"os"
	"testing"
)

func TestRequiredDeploymentPackagesExist(t *testing.T) {
	for _, path := range []string{"../../module", "../../remote"} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("required package %s is missing", path)
		}
	}
}
