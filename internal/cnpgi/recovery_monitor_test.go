package cnpgi

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/cnpg_backup/internal/configuration"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"github.com/djosh34/cnpg_backup/internal/repository"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestLostObserversRetireOnlyAfterDurableUncertainty(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		t.Run(fmt.Sprint("persistence-fails=", failWrite), func(t *testing.T) {
			api, original, _ := fixture(t, true)
			source, err := api.Repository(context.Background(), "test", "source")
			if err != nil {
				t.Fatal(err)
			}
			var releases atomic.Int32
			api.recoveryLifetime = func(_ context.Context, _ Cluster, _ configuration.Spec, release bool) error {
				if release {
					releases.Add(1)
				}
				return nil
			}
			client := api.Client.(*dynamicfake.FakeDynamicClient)
			if failWrite {
				client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("persist uncertainty failed")
				})
			}
			count := 17
			if failWrite {
				count = 1
			}
			for i := 0; i < count; i++ {
				c := original
				c.Metadata = meta.ObjectMeta{Name: fmt.Sprintf("fresh-%d", i), Namespace: "test", UID: types.UID(repository.UUID())}
				placement := RecoveryPlacement{ClusterUID: string(c.Metadata.UID), Namespace: "test", Cluster: c.Metadata.Name, OperationID: c.OperationUID(), BootstrapSHA256: c.BootstrapFingerprint(), Source: "source", Destination: "destination", SourceConfigSHA256: source.Hash()}
				guard := recoveryguard.Config{ClusterUID: string(c.Metadata.UID), OperationUID: c.OperationUID(), Targets: []recoveryguard.Target{{PVCUID: repository.UUID(), Mount: "/var/lib/postgresql/data"}}}
				stream := watch.NewRaceFreeFake()
				client.PrependWatchReactor("pods", func(ktesting.Action) (bool, watch.Interface, error) { return true, stream, nil })
				if err := api.ensureRecovery(context.Background(), c, placement, guard, source); err != nil {
					stream.Stop()
					t.Fatal(i, err)
				}
				stream.Stop()
				deadline := time.Now().Add(2 * time.Second)
				for {
					co := api.recoveryState()
					co.mu.Lock()
					m := co.monitors[c.OperationUID()]
					finished := m == nil || m.done
					co.mu.Unlock()
					if finished {
						if failWrite && m == nil {
							t.Fatal("lost closed in-memory fence before durable ack")
						}
						if !failWrite && m != nil {
							t.Fatal("durably closed observer still consumes capacity")
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("observer did not terminate")
					}
					time.Sleep(time.Millisecond)
				}
				cm, err := api.Get(context.Background(), coreResource("configmaps"), "test", operationName(c))
				if err != nil {
					t.Fatal(err)
				}
				state, err := operationFrom(cm, c)
				want := "uncertain"
				if failWrite {
					want = "active"
				}
				if err != nil || state.State != want || state.LifetimeReleased {
					t.Fatal(state, err)
				}
				if err := api.ensureRecovery(context.Background(), c, placement, guard, source); !errors.Is(err, errRecoveryClosed) {
					t.Fatal("closed operation reopened", err)
				}
			}
			if releases.Load() != 0 {
				t.Fatal("watch loss released source hold")
			}
		})
	}
}
