package cnpgi

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Actual observer/predicate/ConfigMap transitions on an explicitly scheduled
// Kubernetes I/O seam. Not a claim of measured CNPG cleanup timing.
func TestTerminalPodEventChecksCompletionBeforeCleanup(t *testing.T) {
	for _, delay := range []time.Duration{500 * time.Millisecond, 3 * time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				api, c, job, pod := completionFixture(t)
				var releases atomic.Int32
				api.recoveryMu.Lock()
				api.recoveryLifetime = func(_ context.Context, _ Cluster, _ configuration.Spec, release bool) error {
					if release {
						releases.Add(1)
					}
					return nil
				}
				api.recoveryMu.Unlock()
				putCompletion(t, api, c, job, pod)
				synctest.Wait()
				time.Sleep(delay)
				synctest.Wait()
				now := meta.Now()
				job.DeletionTimestamp = &now
				pod.DeletionTimestamp = &now
				pod.ResourceVersion = "8"
				if _, err := api.Client.Resource(jobsResource).Namespace(c.Metadata.Namespace).Update(context.Background(), unstruct(job), meta.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := api.Client.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).Update(context.Background(), unstruct(pod), meta.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				cm, err := api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
				if err != nil {
					t.Fatal(err)
				}
				state, err := operationFrom(cm, c)
				if err != nil || state.State != "completed" || !state.LifetimeReleased || releases.Load() != 1 {
					t.Fatal("delivered valid completion was not evaluated before cleanup", state, err, releases.Load())
				}
			})
		})
	}
}

func TestJobCompleteAfterLastPodEventRetainsPeriodicFallbackAndUncertainty(t *testing.T) {
	for _, cleanupEarly := range []bool{false, true} {
		name := "periodic-fallback"
		if cleanupEarly {
			name = "missing-before-proof"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				api, c, job, pod := completionFixture(t)
				complete := job.Status
				job.Status.Conditions = nil
				job.Status.Succeeded = 0
				job.Status.Active = 1
				var releases atomic.Int32
				api.recoveryMu.Lock()
				api.recoveryLifetime = func(_ context.Context, _ Cluster, _ configuration.Spec, release bool) error {
					if release {
						releases.Add(1)
					}
					return nil
				}
				api.recoveryMu.Unlock()
				putCompletion(t, api, c, job, pod)
				synctest.Wait()
				time.Sleep(100 * time.Millisecond)
				job.Status = complete
				if _, err := api.Client.Resource(jobsResource).Namespace(c.Metadata.Namespace).Update(context.Background(), unstruct(job), meta.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				synctest.Wait() // no later Pod event is promised by JobController
				if cleanupEarly {
					time.Sleep(500 * time.Millisecond)
					if err := api.Client.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).Delete(context.Background(), pod.Name, meta.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				} else {
					time.Sleep(3 * time.Second)
				}
				synctest.Wait()
				cm, err := api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
				if err != nil {
					t.Fatal(err)
				}
				state, err := operationFrom(cm, c)
				if err != nil {
					t.Fatal(err)
				}
				if cleanupEarly {
					if state.State != "uncertain" || state.LifetimeReleased || releases.Load() != 0 {
						t.Fatal("certified disappeared evidence", state)
					}
				} else if state.State != "completed" || !state.LifetimeReleased || releases.Load() != 1 {
					t.Fatal("periodic fallback lost", state)
				}
			})
		})
	}
}

func TestRunningPodDisappearanceNeverReleasesLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		api, c, job, pod := completionFixture(t)
		pod.Status.Phase = core.PodRunning
		pod.Status.InitContainerStatuses[len(pod.Status.InitContainerStatuses)-1].State = core.ContainerState{Running: &core.ContainerStateRunning{}}
		job.Status.Conditions = nil
		job.Status.Succeeded = 0
		job.Status.Active = 1
		var releases atomic.Int32
		api.recoveryMu.Lock()
		api.recoveryLifetime = func(_ context.Context, _ Cluster, _ configuration.Spec, release bool) error {
			if release {
				releases.Add(1)
			}
			return nil
		}
		api.recoveryMu.Unlock()
		putCompletion(t, api, c, job, pod)
		synctest.Wait()
		if err := api.Client.Resource(coreResource("pods")).Namespace(c.Metadata.Namespace).Delete(context.Background(), pod.Name, meta.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(24 * time.Hour)
		cm, err := api.Get(context.Background(), coreResource("configmaps"), c.Metadata.Namespace, operationName(c))
		if err != nil {
			t.Fatal(err)
		}
		state, err := operationFrom(cm, c)
		if err != nil || state.State != "uncertain" || state.LifetimeReleased || releases.Load() != 0 {
			t.Fatal(state, err)
		}
	})
}
