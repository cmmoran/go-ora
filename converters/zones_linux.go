//go:build linux && !windows && !aix && !android && !darwin && !dragonfly && !freebsd && !hu && !illumos && !ios && !js && !na && !netbsd && !openbsd && !plan9 && !solaris

package converters

import (
	"os"
	"strings"
	"sync"
	"time"
)

func init() {
	canonicalDetector = &localCanonicalDetector{}
}

type localCanonicalDetector struct {
	local string
	once  sync.Once
}

func (l *localCanonicalDetector) DetectCanonicalLocal() string {
	l.once.Do(func() {
		// 1. Try readlink on /etc/localtime
		if target, err := os.Readlink("/etc/localtime"); err == nil {
			if i := strings.Index(target, "zoneinfo/"); i >= 0 {
				l.local = target[i+len("zoneinfo/"):]
				return
			}
		}

		// 2. Try /etc/timezone file (Debian/Ubuntu)
		if b, err := os.ReadFile("/etc/timezone"); err == nil {
			l.local = strings.TrimSpace(string(b))
			return
		}

		// 4. Fallback
		l.local = time.Local.String() // "Local" or offset-only
	})

	return l.local
}
