package jobs

import "testing"

func TestValidID(t *testing.T) {
	for _, id := range []string{"00000000-0000-0000-0000-000000000001", "ABCDEF12-0000-0000-0000-000000000001"} {
		if !ValidID(id) {
			t.Fatalf("rejected %q", id)
		}
	}
	for _, id := range []string{"", "invalid", "000000--0000-0000-0000-0000000000--", "00000000-0000-0000-0000-00000000000g"} {
		if ValidID(id) {
			t.Fatalf("accepted %q", id)
		}
	}
}
