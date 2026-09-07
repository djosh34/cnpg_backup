package s3store

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultipartCompletionOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "lost-init", "lost-complete", "embedded-error", "conflict", "cancel", "wrong-init", "wrong-complete"} {
		t.Run(mode, func(t *testing.T) {
			var initiations, completions, aborts, parts atomic.Int32
			var committed atomic.Bool
			s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					aborts.Add(1)
					w.WriteHeader(204)
					return
				}
				if r.Method == "POST" && r.URL.Query().Has("uploads") {
					initiations.Add(1)
					if mode == "lost-init" {
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
					if mode == "wrong-init" {
						fmt.Fprint(w, "<InitiateMultipartUploadResult><Bucket>other-bucket</Bucket><Key>other</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>")
						return
					}
					fmt.Fprint(w, "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test/key</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>")
					return
				}
				if r.Method == "PUT" {
					parts.Add(1)
					io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", "part-etag")
					return
				}
				if r.Method == "POST" {
					completions.Add(1)
					var v struct {
						Parts []struct {
							Number int `xml:"PartNumber"`
							ETag   string
						} `xml:"Part"`
					}
					if xml.NewDecoder(r.Body).Decode(&v) != nil || len(v.Parts) != 1 || v.Parts[0].Number != 1 || v.Parts[0].ETag != "part-etag" {
						t.Error("incorrect ordered completion parts")
					}
					if mode == "embedded-error" {
						fmt.Fprint(w, "<Error><Code>InternalError</Code></Error>")
						return
					}
					if mode == "conflict" {
						s3error(w, 409, "ConditionalRequestConflict")
						return
					}
					if mode == "cancel" {
						<-r.Context().Done()
						return
					}
					committed.Store(true)
					if mode == "lost-complete" {
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
					if mode == "wrong-complete" {
						fmt.Fprint(w, "<CompleteMultipartUploadResult><Bucket>other-bucket</Bucket><Key>other</Key><ETag>completed</ETag></CompleteMultipartUploadResult>")
						return
					}
					fmt.Fprint(w, "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test/key</Key><ETag>completed</ETag></CompleteMultipartUploadResult>")
					return
				}
			}))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			f, v := spool(t, []byte("small final part"))
			u, _, e := s.UploadFile(ctx, "key", f, v, nil)
			if mode == "success" {
				if e != nil || !committed.Load() {
					t.Fatal(e)
				}
			} else if mode == "wrong-init" || mode == "wrong-complete" {
				mustKind(t, e, Corrupt, true)
			} else if mode == "conflict" {
				mustKind(t, e, Conflict, false)
			} else if mode == "cancel" {
				mustKind(t, e, Canceled, true)
			} else {
				mustKind(t, e, Transient, true)
			}
			if initiations.Load() != 1 || completions.Load() > 1 || parts.Load() > 1 || aborts.Load() != 0 {
				t.Fatal("hidden retry/abort")
			}
			if mode == "lost-init" && u.ID != "" {
				t.Fatal("invented upload ID")
			}
			if mode == "lost-complete" && !committed.Load() {
				t.Fatal("did not commit before response loss")
			}
		})
	}
}

func TestMultipartPagination(t *testing.T) {
	for _, kind := range []string{"uploads", "parts"} {
		for _, mode := range []string{"complete", "error", "repeat", "malformed"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				var pages atomic.Int32
				s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := pages.Add(1)
					if mode == "error" && n == 2 {
						s3error(w, 403, "AccessDenied")
						return
					}
					if mode == "malformed" {
						fmt.Fprint(w, "<wrong/>")
						return
					}
					if kind == "uploads" {
						if n == 1 || mode == "repeat" {
							fmt.Fprint(w, "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>test/a</NextKeyMarker><NextUploadIdMarker>id1</NextUploadIdMarker><Upload><Key>test/a</Key><UploadId>id1</UploadId></Upload></ListMultipartUploadsResult>")
						} else {
							if r.URL.Query().Get("key-marker") != "test/a" || r.URL.Query().Get("upload-id-marker") != "id1" {
								t.Error("markers missing")
							}
							fmt.Fprint(w, "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>test/b</Key><UploadId>id2</UploadId></Upload></ListMultipartUploadsResult>")
						}
					} else {
						if n == 1 || mode == "repeat" {
							fmt.Fprint(w, "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>1</NextPartNumberMarker><Part><PartNumber>1</PartNumber><ETag>\"part1\"</ETag><Size>1</Size></Part></ListPartsResult>")
						} else {
							if r.URL.Query().Get("part-number-marker") != "1" {
								t.Error("part marker missing")
							}
							fmt.Fprint(w, "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>2</PartNumber><ETag>\"part2\"</ETag><Size>2</Size></Part></ListPartsResult>")
						}
					}
				}))
				count := 0
				var e error
				if kind == "uploads" {
					e = s.ListUploads(context.Background(), "", 10, func(u Upload) error { count++; return nil })
				} else {
					e = s.ListParts(context.Background(), Upload{Key: "key", ID: "id"}, func(p Part) error { count++; return nil })
				}
				if mode == "complete" {
					if e != nil || count != 2 || pages.Load() != 2 {
						t.Fatalf("%v %d %d", e, count, pages.Load())
					}
				} else if mode == "error" {
					mustKind(t, e, Auth, false)
				} else {
					mustKind(t, e, Corrupt, false)
				}
			})
		}
	}
}

func TestInterruptedGETRetriesAndDeadline(t *testing.T) {
	var calls atomic.Int32
	s, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("ETag", "opaque-not-sha256")
		w.Header().Set("Last-Modified", "Mon, 07 Sep 2026 00:00:00 GMT")
		w.Header().Set("Content-Length", "4")
		if calls.Load() == 1 {
			fmt.Fprint(w, "ab")
			return
		}
		fmt.Fprint(w, "abcd")
	}))
	dst, v := spool(t, []byte("abcd"))
	_, e := s.Download(context.Background(), "key", dst, v)
	if e != nil || calls.Load() != 2 {
		t.Fatalf("stream retry %v %d", e, calls.Load())
	}
	s2, _ := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	s2.metadataTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	e = s2.Delete(ctx, "key")
	mustKind(t, e, Canceled, true)
	if strings.Contains(e.Error(), "test-secret-secret") {
		t.Fatal("secret leak")
	}
}
