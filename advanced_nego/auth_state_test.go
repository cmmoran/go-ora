package advanced_nego

import (
	"bytes"
	"sync"
	"testing"
)

type testNTSAuth struct {
	marker byte
}

func (auth *testNTSAuth) NewNegotiateMessage(string, string) ([]byte, error) {
	return []byte{auth.marker}, nil
}

func (auth *testNTSAuth) ProcessChallenge([]byte, string, string) ([]byte, error) {
	return []byte{auth.marker}, nil
}

type testKerberosAuth int

func (testKerberosAuth) Authenticate(string, string) ([]byte, error) { return nil, nil }

func TestNTSPacketUsesExplicitAuthenticator(t *testing.T) {
	global := &testNTSAuth{marker: 1}
	explicit := &testNTSAuth{marker: 2}
	SetNTSAuth(global)
	t.Cleanup(func() { SetNTSAuth(&NTSAuthDefault{}) })

	packet, err := createNTSNegoPacket(explicit, "domain", "machine")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet[len(packet)-1:], []byte{explicit.marker}) {
		t.Fatal("packet did not use the session-specific authenticator")
	}
}

func TestLegacyAuthSettersAreRaceSafe(t *testing.T) {
	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			SetNTSAuth(&testNTSAuth{marker: byte(i)})
			if getNTSAuth() == nil {
				t.Error("NTS authenticator unexpectedly nil")
			}
			SetKerberosAuth(testKerberosAuth(i))
			if getKerberosAuth() == nil {
				t.Error("Kerberos authenticator unexpectedly nil")
			}
		}(i)
	}
	wg.Wait()
}
