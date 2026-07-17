package configurations

import "testing"

func TestValidatePreservesExplicitAuthenticationTypeWithoutPassword(t *testing.T) {
	for _, authType := range []AuthType{Kerberos, TCPS} {
		config := &ConnectionConfig{
			DatabaseInfo: DatabaseInfo{
				ServiceName: "service",
				AuthType:    authType,
			},
		}
		if err := config.validate(); err != nil {
			t.Fatal(err)
		}
		if config.AuthType != authType {
			t.Fatalf("explicit auth type %v was changed to %v", authType, config.AuthType)
		}
	}
}

func TestValidateDefaultsIncompleteNormalCredentialsToOSAuthentication(t *testing.T) {
	config := &ConnectionConfig{
		DatabaseInfo: DatabaseInfo{
			ServiceName: "service",
			UserID:      "user",
			AuthType:    Normal,
		},
	}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	if config.AuthType != OS {
		t.Fatalf("expected OS authentication fallback, got %v", config.AuthType)
	}
}
