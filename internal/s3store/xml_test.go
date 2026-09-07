package s3store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

// Exercise complete HTTP bodies through the real SDK/adapter, not just an XML
// parser. A valid first root must not hide an invalid remainder of the response.
var xmlEnvelopes = []struct {
	name, prefix, suffix string
	valid                bool
}{
	{"plain", "", "", true},
	{"declaration-comments", `<?xml version="1.0" encoding="UTF-8"?><!--before-->`, "<!--after-->\r\n\t ", true},
	{"whitespace-comments", " \r\n\t<!--before-->", " <!--after-->\n", true},
	{"trailing-junk", "", "junk", false},
	{"extra-root", "", "<Extra/>", false},
	{"unterminated-root", "", "<unterminated", false},
	{"unterminated-comment", "", "<!--unterminated", false},
	{"unterminated-instruction", "", "<?unterminated", false},
	{"leading-junk", "junk", "", false},
	{"trailing-declaration", "", `<?xml version="1.0"?>`, false},
	{"trailing-cdata", "", "<![CDATA[ ]]>", false},
	{"leading-character-reference", "&#32;", "", false},
}

func TestXMLDocumentInventories(t *testing.T) {
	for _, kind := range []string{"objects", "uploads", "parts"} {
		for _, tc := range xmlEnvelopes {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				root := map[string]string{"objects": "ListBucketResult", "uploads": "ListMultipartUploadsResult", "parts": "ListPartsResult"}[kind]
				s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintf(w, "%s<%s><IsTruncated>false</IsTruncated></%s>%s", tc.prefix, root, root, tc.suffix)
				}))
				var err error
				count := 0
				switch kind {
				case "objects":
					err = s.List(context.Background(), "", 10, func(Info) error { count++; return nil })
				case "uploads":
					err = s.ListUploads(context.Background(), "", 10, func(Upload) error { count++; return nil })
				case "parts":
					err = s.ListParts(context.Background(), Upload{Key: "key", ID: "id"}, func(Part) error { count++; return nil })
				}
				if tc.valid {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					mustKind(t, err, Corrupt, false)
				}
				if count != 0 {
					t.Fatal("unexpected inventory entry")
				}
			})
		}
	}
}

func TestXMLDocumentBucketSafety(t *testing.T) {
	for _, config := range []struct {
		name, query, body string
		status            int
	}{
		{"versioning", "versioning", "<VersioningConfiguration/>", 200},
		{"lifecycle", "lifecycle", "<LifecycleConfiguration/>", 200},
		{"object-lock", "object-lock", "<ObjectLockConfiguration/>", 200},
		{"no-lifecycle", "lifecycle", "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>", 404},
		{"no-object-lock", "object-lock", "<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>", 404},
	} {
		for _, tc := range xmlEnvelopes {
			t.Run(config.name+"/"+tc.name, func(t *testing.T) {
				s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					q := r.URL.Query()
					if q.Has(config.query) {
						w.WriteHeader(config.status)
						fmt.Fprint(w, tc.prefix+config.body+tc.suffix)
						return
					}
					switch {
					case q.Has("versioning"):
						fmt.Fprint(w, "<VersioningConfiguration/>")
					case q.Has("lifecycle"):
						s3error(w, 404, "NoSuchLifecycleConfiguration")
					case q.Has("object-lock"):
						s3error(w, 404, "ObjectLockConfigurationNotFoundError")
					default:
						t.Error("unexpected bucket request")
					}
				}))
				err := s.CheckBucketSafety(context.Background())
				if tc.valid {
					if err != nil {
						t.Fatal(err)
					}
				} else if config.status == 404 {
					mustKind(t, err, Unknown, false)
				} else {
					mustKind(t, err, Corrupt, false)
				}
			})
		}
	}
}

func TestXMLDocumentMultipart(t *testing.T) {
	const initiation = "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test/key</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>"
	const completion = "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test/key</Key><ETag>completed</ETag></CompleteMultipartUploadResult>"
	for _, stage := range []string{"initiation", "completion", "embedded-error"} {
		for _, tc := range xmlEnvelopes {
			t.Run(stage+"/"+tc.name, func(t *testing.T) {
				var initiations, parts, completions, aborts atomic.Int32
				s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					body := ""
					switch {
					case r.Method == "DELETE":
						aborts.Add(1)
						w.WriteHeader(204)
						return
					case r.Method == "PUT":
						parts.Add(1)
						w.Header().Set("ETag", "part-etag")
						return
					case r.URL.Query().Has("uploads"):
						initiations.Add(1)
						body = initiation
						if stage == "initiation" {
							body = tc.prefix + body + tc.suffix
						}
					default:
						completions.Add(1)
						body = completion
						if stage == "embedded-error" {
							body = "<Error><Code>PreconditionFailed</Code></Error>"
						}
						if stage != "initiation" {
							body = tc.prefix + body + tc.suffix
						}
					}
					fmt.Fprint(w, body)
				}))
				f, expected := spool(t, []byte("final part"))
				u, _, err := s.UploadFile(context.Background(), "key", f, expected, nil)
				if !tc.valid {
					mustKind(t, err, Corrupt, true)
				} else if stage == "embedded-error" {
					mustKind(t, err, Precondition, false)
				} else if err != nil {
					t.Fatal(err)
				}
				wantParts := int32(1)
				wantID := "id"
				if stage == "initiation" && !tc.valid {
					wantParts, wantID = 0, ""
				}
				if initiations.Load() != 1 || parts.Load() != wantParts || completions.Load() != wantParts || aborts.Load() != 0 || u.ID != wantID {
					t.Fatalf("unexpected MPU continuation/retry/cleanup: init=%d parts=%d complete=%d abort=%d ID=%q", initiations.Load(), parts.Load(), completions.Load(), aborts.Load(), u.ID)
				}
			})
		}
	}
}

func TestXMLDocumentMutationErrors(t *testing.T) {
	for _, outcome := range []struct {
		operation, code string
		status          int
		kind            Kind
	}{
		{"match", "NoSuchKey", 404, Precondition},
		{"create", "PreconditionFailed", 412, Precondition},
		{"delete", "ConditionalRequestConflict", 409, Conflict},
		{"abort", "AccessDenied", 403, Auth},
	} {
		for _, tc := range xmlEnvelopes {
			t.Run(outcome.operation+"/"+tc.name, func(t *testing.T) {
				var requests atomic.Int32
				s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					io.Copy(io.Discard, r.Body)
					w.WriteHeader(outcome.status)
					fmt.Fprintf(w, "%s<Error><Code>%s</Code></Error>%s", tc.prefix, outcome.code, tc.suffix)
				}))
				var err error
				switch outcome.operation {
				case "delete":
					err = s.Delete(context.Background(), "key")
				case "abort":
					err = s.Abort(context.Background(), Upload{Key: "key", ID: "id"})
				default:
					f, expected := spool(t, []byte("replacement"))
					condition := Condition{Create: true}
					if outcome.operation == "match" {
						condition = Condition{Match: "old-etag"}
					}
					_, err = s.PutFile(context.Background(), "key", f, expected, condition, nil)
				}
				if tc.valid {
					mustKind(t, err, outcome.kind, false)
				} else {
					mustKind(t, err, Unknown, true)
				}
				if requests.Load() != 1 {
					t.Fatal("mutation retried")
				}
			})
		}
	}
}

func TestXMLDocumentAbsence(t *testing.T) {
	for _, tc := range xmlEnvelopes {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
				fmt.Fprint(w, tc.prefix+"<Error><Code>NoSuchKey</Code></Error>"+tc.suffix)
			}))
			_, _, err := s.Read(context.Background(), "key", 100)
			want := Unknown
			if tc.valid {
				want = NotFound
			}
			mustKind(t, err, want, false)
		})
	}
}
