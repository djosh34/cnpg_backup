package s3store

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"

	"github.com/minio/minio-go/v7"
)

// Kind is a bounded public error vocabulary. Error text never includes server
// messages, keys, URLs, credentials, upload IDs, or underlying SDK errors.
type Kind string

const (
	Invalid  Kind = "InvalidStorageInput"
	NotFound Kind = "NoSuchKey"
	// HEAD404 lacks an authenticated error body. Only this result may trigger
	// a confirming GET; it is never itself an allowed archive absence.
	HeadMissing  Kind = "UnconfirmedHeadMissing"
	Precondition Kind = "PreconditionFailed"
	Conflict     Kind = "Conflict"
	Auth         Kind = "StorageAuthenticationFailed"
	TLS          Kind = "StorageTLSFailed"
	Transient    Kind = "TransientStorageFailure"
	Canceled     Kind = "StorageCanceled"
	Corrupt      Kind = "StorageCorruption"
	Limit        Kind = "StorageCapacityExceeded"
	Unsupported  Kind = "UnsupportedStorageSemantics"
	Unknown      Kind = "StorageFailure"
	LocalIO      Kind = "LocalIOFailure"
)

// Ambiguous means a mutation may have applied (or may still apply). It is not
// safe to retry blindly or release destructive ownership, even after HEAD.
type Error struct {
	Kind      Kind
	Ambiguous bool
}

func (e *Error) Error() string {
	if e.Ambiguous {
		return string(e.Kind) + " (mutation outcome ambiguous)"
	}
	return string(e.Kind)
}
func failure(k Kind) *Error     { return &Error{Kind: k} }
func Is(err error, k Kind) bool { var e *Error; return errors.As(err, &e) && e.Kind == k }
func classify(err error, mutation bool) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return &Error{Kind: e.Kind, Ambiguous: e.Ambiguous || mutation && !definitive(e.Kind)}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: Canceled, Ambiguous: mutation}
	}
	var cert *url.Error
	if errors.As(err, &cert) {
		err = cert.Err
	}
	var trust x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var host x509.HostnameError
	if errors.As(err, &trust) || errors.As(err, &invalid) || errors.As(err, &host) {
		return &Error{Kind: TLS, Ambiguous: mutation}
	}
	r := minio.ToErrorResponse(err)
	k := Unknown
	switch r.Code {
	case "PreconditionFailed":
		k = Precondition
	case "ConditionalRequestConflict", "OperationAborted":
		k = Conflict
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "AuthorizationHeaderMalformed", "InvalidRegion", "ExpiredToken", "InvalidToken":
		k = Auth
	case "SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable":
		k = Transient
	}
	return &Error{Kind: k, Ambiguous: mutation && !definitive(k)}
}
func definitive(k Kind) bool { return k == Precondition || k == Auth || k == Conflict }
