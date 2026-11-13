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

package ejection

import (
	"testing"
	"time"
)

// failureRate is the straw-man seed reason: #3252's consecutive-failure tracker
// ported to the EjectionReason interface. Client-side outcomes never count.
type failureRate struct{ threshold int }

func (f failureRate) Name() string { return "request-failure-rate" }
func (f failureRate) Attributable(o Outcome) bool {
	switch o {
	case OutcomeTransportError, OutcomeTimeout, OutcomeHTTP5xx:
		return true
	}
	return false // 429, cancellations, 4xx, setup errors never count (RFC property 1)
}
func (f failureRate) Threshold() int { return f.threshold }

func newTestRegistry(now func() time.Time) *Registry {
	r := NewRegistry([]EjectionReason{failureRate{threshold: 3}}, 50, 10*time.Second, 60*time.Second)
	r.now = now
	return r
}

func TestRegistry_EjectsAfterThreshold(t *testing.T) {
	r := newTestRegistry(time.Now)
	for i := 0; i < 2; i++ {
		if got := r.Observe("ns/pod-a", 4, OutcomeHTTP5xx); got != "" {
			t.Fatalf("ejected before threshold: %q", got)
		}
	}
	if got := r.Observe("ns/pod-a", 4, OutcomeHTTP5xx); got != "request-failure-rate" {
		t.Fatalf("expected ejection at threshold, got %q", got)
	}
	if !r.IsEjected("ns/pod-a") {
		t.Fatal("pod should be ejected")
	}
}

func TestRegistry_ClientOutcomesNeverEject(t *testing.T) {
	r := newTestRegistry(time.Now)
	for i := 0; i < 10; i++ {
		for _, o := range []Outcome{OutcomeCanceled, OutcomeClient4xx, OutcomeSetupError, OutcomeHTTP429} {
			if got := r.Observe("ns/pod-a", 4, o); got != "" {
				t.Fatalf("client-side outcome %v ejected a pod", o)
			}
		}
	}
}

func TestRegistry_SuccessResets(t *testing.T) {
	r := newTestRegistry(time.Now)
	r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	r.Observe("ns/pod-a", 4, OutcomeSuccess)
	r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	if got := r.Observe("ns/pod-a", 4, OutcomeHTTP5xx); got != "request-failure-rate" {
		t.Fatalf("consecutive count should have restarted after success; got %q", got)
	}
}

func TestRegistry_CapacityGuardKeepsServingSet(t *testing.T) {
	r := newTestRegistry(time.Now) // 50% guard over a ready pool of 2: at most 1 ejected
	for i := 0; i < 3; i++ {
		r.Observe("ns/pod-a", 2, OutcomeHTTP5xx)
	}
	for i := 0; i < 3; i++ {
		if got := r.Observe("ns/pod-b", 2, OutcomeHTTP5xx); got != "" {
			t.Fatalf("capacity guard should have refused a second ejection, got %q", got)
		}
	}
	if !r.IsEjected("ns/pod-a") || r.IsEjected("ns/pod-b") {
		t.Fatal("guard: pod-a ejected, pod-b must stay routable")
	}
}

func TestRegistry_ProbationRejectsOnFirstFailure(t *testing.T) {
	now := time.Now()
	cur := &now
	r := newTestRegistry(func() time.Time { return *cur })
	for i := 0; i < 3; i++ {
		r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	}
	*cur = now.Add(11 * time.Second) // past the 10s base backoff
	if r.IsEjected("ns/pod-a") {
		t.Fatal("ejection should have expired")
	}
	if got := r.Observe("ns/pod-a", 4, OutcomeHTTP5xx); got != "request-failure-rate" {
		t.Fatalf("probation: one attributable failure should re-eject, got %q", got)
	}
}

func TestRegistry_DropStartsClean(t *testing.T) {
	r := newTestRegistry(time.Now)
	for i := 0; i < 3; i++ {
		r.Observe("ns/pod-a", 4, OutcomeHTTP5xx)
	}
	r.Drop("ns/pod-a")
	if r.IsEjected("ns/pod-a") {
		t.Fatal("dropped pod must start clean")
	}
}
