package executors

import "testing"

func TestCorruptPayload_ChangesBytes(t *testing.T) {
	original := []byte("this is a perfectly normal payload of decent length")
	corrupted := CorruptPayload(original, 30)

	if len(corrupted) != len(original) {
		t.Fatalf("length changed: got %d, want %d", len(corrupted), len(original))
	}

	diff := 0
	for i := range original {
		if original[i] != corrupted[i] {
			diff++
		}
	}
	if diff == 0 {
		t.Fatal("expected at least some bytes to differ, got none")
	}

	// Original slice must be untouched — CorruptPayload must not mutate in place.
	if string(original) != "this is a perfectly normal payload of decent length" {
		t.Fatal("original data was mutated, expected a copy to be returned")
	}
}

func TestCorruptPayload_ZeroPctIsNoOp(t *testing.T) {
	original := []byte("unchanged")
	result := CorruptPayload(original, 0)
	if string(result) != "unchanged" {
		t.Fatal("expected 0 pct to leave data unchanged")
	}
}

func TestCorruptPayload_EmptyInput(t *testing.T) {
	result := CorruptPayload([]byte{}, 50)
	if len(result) != 0 {
		t.Fatal("expected empty input to remain empty")
	}
}
