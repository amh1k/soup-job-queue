package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInvalidClaimInputs(t *testing.T) {
	s := NewStore(nil)
	ctx := context.Background()
	for _, lease := range []time.Duration{0, -time.Second, time.Nanosecond, 25 * time.Hour} {
		if _, err := NewStoreWithLease(nil, lease); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("lease %s: %v", lease, err)
		}
	}
	for _, worker := range []string{"", " "} {
		if _, err := s.ClaimNext(ctx, worker); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("worker: %v", err)
		}
	}
	if err := s.Renew(ctx, Claim{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, Claim{}, Result{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, Claim{}, Failure{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1001} {
		if _, err := s.RecoverExpired(ctx, n); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
}
