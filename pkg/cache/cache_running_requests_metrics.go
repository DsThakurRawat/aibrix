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

package cache

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"

	"github.com/vllm-project/aibrix/pkg/constants"
)

// Reasons aibrix_gateway_running_requests_admit_fallback_total is incremented: the
// admission decision fell back to this gateway's local atomic count. That count only
// learns about a concurrent request when request tracking runs after admission, so
// while the fallback is deciding, the requestsInflight cap is best-effort per gateway
// rather than a hard cross-gateway cap (see admitRunningRequest).
const (
	// admitFallbackReasonEvalError: the admit script's Redis round trip failed inside
	// runningRequestsReadTimeout (timeout, dropped connection, Redis error reply).
	admitFallbackReasonEvalError = "eval_error"
	// admitFallbackReasonBadReply: Redis answered, but the reply did not match the
	// admit script's contract -- a version-skew or data-corruption signal worth
	// noticing on its own.
	admitFallbackReasonBadReply = "bad_reply"
)

var (
	admitFallbackMetricsOnce sync.Once
	admitFallbackTotal       *prometheus.CounterVec
)

func initAdmitFallbackMetrics() {
	admitFallbackMetricsOnce.Do(func() {
		admitFallbackTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: constants.AibrixSubsystemName,
				Name:      "gateway_running_requests_admit_fallback_total",
				Help:      "Running-requests admissions decided by the local-count fallback instead of the atomic Redis script, by cause. While this is nonzero the requestsInflight cap is best-effort per gateway, not a hard cross-gateway cap.",
			},
			[]string{"reason"},
		)
		if err := prometheus.Register(admitFallbackTotal); err != nil {
			registered, ok := err.(prometheus.AlreadyRegisteredError)
			if !ok {
				klog.ErrorS(err, "failed to register running-requests admit fallback metric",
					"metric", "gateway_running_requests_admit_fallback_total")
				admitFallbackTotal = nil
				return
			}
			admitFallbackTotal = registered.ExistingCollector.(*prometheus.CounterVec)
		}
	})
}

// incAdmitFallback records one admission decided by the local-count fallback. The nil
// guard covers registration failure, not the nil-client path: a gateway without Redis
// configured decides every admission locally by design and is not counted.
func incAdmitFallback(reason string) {
	initAdmitFallbackMetrics()
	if admitFallbackTotal == nil {
		return
	}
	admitFallbackTotal.WithLabelValues(reason).Inc()
}
