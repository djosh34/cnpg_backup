package s3store

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

func TestXMLGrammarOutcomes(t *testing.T) {
	for _, tc := range []struct{ name, prefix, attrs string }{
		{"missing-version", `<?xml?>`, ""},
		{"garbage-declaration", `<?xml garbage?>`, ""},
		{"unquoted-version", `<?xml version=1.0?>`, ""},
		{"duplicate-attribute", "", ` a="1" a="2"`},
		{"duplicate-expanded-attribute", "", ` xmlns:a="urn:test" xmlns:b="urn:test" a:id="1" b:id="2"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"read", "list", "match"} {
				t.Run(op, func(t *testing.T) {
					s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if op == "list" {
							fmt.Fprintf(w, "%s<ListBucketResult%s><IsTruncated>false</IsTruncated></ListBucketResult>", tc.prefix, tc.attrs)
						} else {
							w.WriteHeader(404)
							fmt.Fprintf(w, "%s<Error%s><Code>NoSuchKey</Code></Error>", tc.prefix, tc.attrs)
						}
					}))
					switch op {
					case "read":
						_, _, err := s.Read(context.Background(), "key", 100)
						mustKind(t, err, Unknown, false)
					case "list":
						err := s.List(context.Background(), "", 10, func(Info) error { return nil })
						mustKind(t, err, Corrupt, false)
					case "match":
						f, expected := spool(t, []byte("replacement"))
						_, err := s.PutFile(context.Background(), "key", f, expected, Condition{Match: "old-etag"}, nil)
						mustKind(t, err, Unknown, true)
					}
				})
			}
		})
	}
}
