package cnpgi

import (
	"context"
	"errors"
	"testing"

	"github.com/djosh34/cnpg_backup/internal/repository"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestCompletionConsumesEveryPodPage(t *testing.T) {
	for _, mode := range []string{"complete", "lost second page", "repeated cursor", "missing old Pod"} {
		t.Run(mode, func(t *testing.T) {
			api, c, job, pod := completionFixture(t)
			job.Status.Failed = 1
			old := pod.DeepCopy()
			old.Name = "failed-attempt"
			old.UID = types.UID(repository.UUID())
			old.Status.Phase = core.PodFailed
			old.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1
			client := api.Client.(*dynamicfake.FakeDynamicClient)
			// Fake reactor registration does not lock itself. Configure before
			// publishing completion events, also excluding the periodic observer.
			client.Lock()
			client.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				opts := action.(interface{ GetListOptions() meta.ListOptions }).GetListOptions()
				if opts.Limit != 1 {
					t.Fatal("unbounded Pod page")
				}
				if opts.Continue != "" && mode == "lost second page" {
					return true, nil, errors.New("lost second page")
				}
				obj := pod
				next := "next"
				if mode == "missing old Pod" {
					next = ""
				}
				if opts.Continue != "" {
					obj = *old
					next = ""
					if mode == "repeated cursor" {
						next = opts.Continue
					}
				}
				mapped, e := runtime.DefaultUnstructuredConverter.ToUnstructured(&obj)
				if e != nil {
					t.Fatal(e)
				}
				list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{{Object: mapped}}}
				list.SetContinue(next)
				list.SetResourceVersion("99")
				return true, list, nil
			})
			client.Unlock()
			putCompletion(t, api, c, job, pod, *old)
			uid, ids, e := api.recoveryTermination(context.Background(), c, map[string]string{string(pod.UID): pod.ResourceVersion, string(old.UID): old.ResourceVersion})
			if mode == "complete" {
				if e != nil || uid != string(job.UID) || len(ids) != 2 {
					t.Fatal(uid, ids, e)
				}
			} else if uid != "" || e == nil {
				t.Fatal("certified incomplete pagination", uid, ids, e)
			}
		})
	}
}
