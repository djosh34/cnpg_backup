package cnpgi

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRepositoryValidationReadsSharedSecretOnce(t *testing.T) {
	a, _, _ := fixture(t, false)
	client := a.Client.(*dynamicfake.FakeDynamicClient)
	reads := 0
	client.PrependReactor("get", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		data := map[string]any{"access": base64.StdEncoding.EncodeToString([]byte("access")), "secret": base64.StdEncoding.EncodeToString([]byte("secret"))}
		// A rotation after the first GET removes a key. Validation must use
		// the same Secret object for both keys, just like storage operations.
		if reads > 1 {
			delete(data, "secret")
		}
		return true, &unstructured.Unstructured{Object: map[string]any{"data": data}}, nil
	})
	if _, err := a.Repository(context.Background(), "test", "destination"); err != nil {
		t.Fatal(err)
	}
	if reads != 1 {
		t.Fatalf("read shared Secret %d times", reads)
	}
}

func TestRepositoryCredentialsValidation(t *testing.T) {
	for name, encoded := range map[string]string{
		"empty": "", "malformed": "not base64",
		"decoded limit": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", (64<<10)+1))),
		"encoded limit": strings.Repeat("x", (90<<10)+1),
	} {
		t.Run(name, func(t *testing.T) {
			a, c, _ := fixture(t, false)
			_, spec, err := a.backupConfiguration(context.Background(), unstruct(c))
			if err != nil {
				t.Fatal(err)
			}
			client := a.Client.(*dynamicfake.FakeDynamicClient)
			client.PrependReactor("get", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, &unstructured.Unstructured{Object: map[string]any{"data": map[string]any{"access": encoded, "secret": encoded}}}, nil
			})
			if _, err := a.Repository(context.Background(), "test", "destination"); err == nil {
				t.Error("configuration validation accepted invalid credentials")
			}
			if _, err := a.repositorySnapshot(context.Background(), "test", spec); err == nil {
				t.Error("storage snapshot accepted invalid credentials")
			}
		})
	}
}
