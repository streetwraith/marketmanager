package region

import "testing"

func TestPriority(t *testing.T) {
	tests := []struct {
		name string
		id   int64
		want int
	}{
		{"Domain is first", Domain, 1},
		{"The Forge is second", TheForge, 2},
		{"Heimatar is third", Heimatar, 3},
		{"Metropolis is fourth", Metropolis, 4},
		{"Sinq Laison is fifth", SinqLaison, 5},
		{"the global PLEX market is not a hub", GlobalPLEX, Rest},
		{"an unlisted region falls to Rest", 10000069, Rest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Priority(tc.id); got != tc.want {
				t.Errorf("Priority(%d) = %d, want %d", tc.id, got, tc.want)
			}
		})
	}
}

// Retries cost tokens, so the owner's rule is that only priorities 1-4 get them.
func TestCanRetry(t *testing.T) {
	tests := []struct {
		name string
		id   int64
		want bool
	}{
		{"Domain retries", Domain, true},
		{"The Forge retries", TheForge, true},
		{"Heimatar retries", Heimatar, true},
		{"Metropolis retries", Metropolis, true},
		{"Sinq Laison does not", SinqLaison, false},
		{"a non-hub region does not", 10000069, false},
		{"the global PLEX market does not", GlobalPLEX, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Region{ID: tc.id, Priority: Priority(tc.id)}
			if got := r.CanRetry(); got != tc.want {
				t.Errorf("CanRetry() for %d = %v, want %v", tc.id, got, tc.want)
			}
		})
	}
}
