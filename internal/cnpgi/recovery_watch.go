package cnpgi

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// Recovery observation lasts through replay. Ordinary request deadlines must
// not expire this stream; its manager-owned context and stream EOF own closure.
func newRecoveryWatchClient(config *rest.Config) (dynamic.Interface, error) {
	copy := rest.CopyConfig(config)
	copy.Timeout = 0
	return dynamic.NewForConfig(copy)
}
