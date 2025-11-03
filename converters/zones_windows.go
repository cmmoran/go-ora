//go:build windows && !linux && !aix && !android && !darwin && !dragonfly && !freebsd && !hu && !illumos && !ios && !js && !na && !netbsd && !openbsd && !plan9 && !solaris

package converters

import (
	"sync"

	"golang.org/x/sys/windows/registry"
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
		key, err := registry.OpenKey(registry.LOCAL_MACHINE, `SYSTEM\CurrentControlSet\Control\TimeZoneInformation`, registry.QUERY_VALUE)
		if err != nil {
			l.local = ""
			return
		}
		defer key.Close()
		l.local, _, err = key.GetStringValue("TimeZoneKeyName")
	})

	return l.local
}
