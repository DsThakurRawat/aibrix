/*
Copyright 2026 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ejection is a LOCAL STRAW-MAN prototype for the discussion in RFC #3390
// (gateway-side pod ejection with pluggable reasons). It is NOT approved
// architecture: the RFC is under maintainer review, and every design choice here is
// marked ASSUMPTION so a review can strike it without touching call sites. Nothing
// in the gateway imports this package yet; wiring is itself an open RFC question
// (enforcement point, config surface, cross-gateway state).
package ejection

import (
	"sync"
	"time"
)

// Outcome is one request-path result a reason may count. Attribution policy is per
// reason (RFC property 1): client-side outcomes (Canceled, setup errors, 4xx) must
// never eject a pod, so they are distinct values reasons can ignore.
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeTransportError
	OutcomeTimeout
	OutcomeHTTP5xx
	OutcomeHTTP429
	OutcomeCanceled // client-side: never pod-attributable
	OutcomeClient4xx
	OutcomeSetupError
)

// EjectionReason is one pod-ejection trigger (RFC #3390 proposed interface,
// unchanged). ASSUMPTION: per-process in-memory state, no Redis sync (RFC Open
// Question 1, "in-process first" lean).
type EjectionReason interface {
	Name() string
	// Attributable reports whether an outcome counts against the pod for this
	// reason. The framework drops non-attributable outcomes before recording.
	Attributable(outcome Outcome) bool
	// Threshold is the consecutive attributable failures that trigger ejection.
	// ASSUMPTION: consecutive-count rather than sliding window, matching #3252's
	// tracker semantics so it ports as the seed reason.
	Threshold() int
}

type podRecord struct {
	failures    map[string]int // reason -> consecutive attributable failures
	ejectedTill time.Time
	probation   bool
}

// Registry is the framework-owned state plane (RFC property: one state plane, many
// reasons). ASSUMPTION: a single capacity guard shared by all reasons
// (maxEjectionPercent of the ready pool, default 50, after Envoy outlier
// detection).
type Registry struct {
	mu                 sync.RWMutex
	records            map[string]*podRecord // key: namespace/name
	reasons            []EjectionReason
	maxEjectionPercent int
	baseBackoff        time.Duration
	maxBackoff         time.Duration
	now                func() time.Time // test hook
}

func NewRegistry(reasons []EjectionReason, maxEjectionPercent int, baseBackoff, maxBackoff time.Duration) *Registry {
	if maxEjectionPercent <= 0 || maxEjectionPercent > 100 {
		maxEjectionPercent = 50
	}
	if baseBackoff > maxBackoff { // RFC property 2: config validation, not runtime surprise
		baseBackoff = maxBackoff
	}
	return &Registry{
		records:            map[string]*podRecord{},
		reasons:            reasons,
		maxEjectionPercent: maxEjectionPercent,
		baseBackoff:        baseBackoff,
		maxBackoff:         maxBackoff,
		now:                time.Now,
	}
}

// Observe records one request-path outcome for a pod and returns the reason name if
// the observation ejected the pod. Non-attributable outcomes are dropped per reason.
// A success resets consecutive-failure counts (RFC property: successful request
// resets), and during probation one attributable failure re-ejects immediately.
func (r *Registry) Observe(podKey string, ready int, outcome Outcome) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[podKey]
	if rec == nil {
		rec = &podRecord{failures: map[string]int{}}
		r.records[podKey] = rec
	}
	if outcome == OutcomeSuccess {
		for k := range rec.failures {
			rec.failures[k] = 0
		}
		rec.probation = false
		return ""
	}
	for _, reason := range r.reasons {
		if !reason.Attributable(outcome) {
			continue
		}
		rec.failures[reason.Name()]++
		threshold := reason.Threshold()
		if rec.probation {
			threshold = 1 // half-open: one attributable failure re-ejects
		}
		if rec.failures[reason.Name()] >= threshold && r.tryEjectLocked(podKey, ready) {
			return reason.Name()
		}
	}
	return ""
}

// tryEjectLocked ejects unless doing so would push ejected pods past the capacity
// guard. Extend-never-stack (RFC property 2): while ejected, further failures only
// extend the current window up to maxBackoff.
func (r *Registry) tryEjectLocked(podKey string, ready int) bool {
	if ready <= 0 {
		return false
	}
	ejected := 0
	for _, p := range r.records {
		if r.now().Before(p.ejectedTill) {
			ejected++
		}
	}
	rec := r.records[podKey]
	alreadyEjected := r.now().Before(rec.ejectedTill)
	if !alreadyEjected && (ejected+1)*100 > ready*r.maxEjectionPercent {
		return false // capacity guard: a serving set always remains
	}
	window := r.baseBackoff
	if alreadyEjected {
		window = 2 * time.Until(rec.ejectedTill)
		if window > r.maxBackoff {
			window = r.maxBackoff
		}
	}
	rec.ejectedTill = r.now().Add(window)
	rec.probation = false
	return true
}

// IsEjected reports whether routing should skip the pod now. Expired ejections enter
// probation: the pod is routable, but one attributable failure re-ejects (RFC
// property 6, half-open return).
func (r *Registry) IsEjected(podKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[podKey]
	if rec == nil {
		return false
	}
	if r.now().Before(rec.ejectedTill) {
		return true
	}
	if !rec.ejectedTill.IsZero() && !rec.probation {
		rec.probation = true
	}
	return false
}

// Drop removes all state for a pod leaving the cache (RFC property 5: a re-added
// pod starts clean).
func (r *Registry) Drop(podKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, podKey)
}
