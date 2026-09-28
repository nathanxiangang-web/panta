package jobs

import "testing"

func TestTerminalStates(t *testing.T) {
	tests := []struct {
		state    State
		terminal bool
	}{
		{StateQueued, false},
		{StateRunning, false},
		{StateRetryWait, false},
		{StateRecoveryRequired, false},
		{StateSucceeded, true},
		{StateFailed, true},
		{StateCanceled, true},
	}
	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Fatalf("%s.Terminal() = %t, want %t", test.state, got, test.terminal)
		}
	}
}
