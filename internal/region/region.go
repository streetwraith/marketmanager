// Package region resolves which regions to ingest and in what order.
package region

// Region ids that carry a fetch priority. Everything else is Rest.
const (
	Domain     int64 = 10000043
	TheForge   int64 = 10000002
	Heimatar   int64 = 10000030
	Metropolis int64 = 10000042
	SinqLaison int64 = 10000032
	GlobalPLEX int64 = 19000001 // GPMR-01, the global PLEX market
)

// Rest is the priority of every region without an explicit one. Regions are
// fetched in ascending priority order, so a lower number wins a contended slot.
const Rest = 6

// RetryMaxPriority is the lowest priority still allowed to retry a failure.
// Priorities above this abandon the cycle and wait for the next Expires tick,
// because a retry costs tokens and their data matters least.
const RetryMaxPriority = 4

// priorities is the owner's ordering. Domain first, then Jita.
var priorities = map[int64]int{
	Domain:     1,
	TheForge:   2,
	Heimatar:   3,
	Metropolis: 4,
	SinqLaison: 5,
}

// Region is one ingestible market region.
type Region struct {
	ID       int64
	Name     string
	Priority int
}

// CanRetry reports whether a failure in this region may be retried. Retries cost
// tokens, so only the four highest-priority regions get them.
func (r Region) CanRetry() bool { return r.Priority <= RetryMaxPriority }

// Priority returns the fetch priority for a region id.
func Priority(id int64) int {
	if p, ok := priorities[id]; ok {
		return p
	}
	return Rest
}
