package configuration

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pem  []byte
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "test CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{parsed, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}
func (ca *testCA) leaf(t *testing.T, name string, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, public, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}
func TestManagerMTLSLeafAndPrivateCARotation(t *testing.T) {
	oldCA, newCA := newCA(t), newCA(t)
	serverCert, serverKey := oldCA.leaf(t, "manager.test", x509.ExtKeyUsageServerAuth)
	clientCert, clientKey := oldCA.leaf(t, "intended-client", x509.ExtKeyUsageClientAuth)
	pair, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := CAPool(oldCA.pem, false)
	if err != nil {
		t.Fatal(err)
	}
	client := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "manager.test", RootCAs: root, Certificates: []tls.Certificate{pair}}
	dir := t.TempDir()
	files := map[string][]byte{"tls.crt": serverCert, "tls.key": serverKey, "client-ca.crt": oldCA.pem}
	publish(t, dir, "old", files)
	oldSnapshot, err := LoadManagerTLS(dir, "intended-client")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("TCP unavailable: %v", err)
	}
	defer listener.Close()
	serverResult := make(chan error, 1)
	handshake := func() error {
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				serverResult <- err
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			secure := tls.Server(conn, &tls.Config{MinVersion: tls.VersionTLS13, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return LoadManagerTLS(dir, "intended-client") }})
			serverResult <- secure.Handshake()
		}()
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", listener.Addr().String(), client)
		if conn != nil {
			conn.Close()
		}
		serverErr := <-serverResult
		if err != nil {
			return err
		}
		return serverErr
	}
	if err := handshake(); err != nil {
		t.Fatal("initial mTLS", err)
	}
	files["tls.crt"], files["tls.key"] = oldCA.leaf(t, "manager.test", x509.ExtKeyUsageServerAuth)
	publish(t, dir, "new-leaf", files)
	if err := handshake(); err != nil {
		t.Fatal("leaf reload", err)
	}
	files["tls.key"] = []byte("invalid rotation")
	publish(t, dir, "broken", files)
	if err := handshake(); err == nil {
		t.Fatal("invalid key silently retained old leaf")
	}
	if len(oldSnapshot.Certificates) != 1 {
		t.Fatal("old snapshot lost")
	}
	files["tls.crt"], files["tls.key"] = newCA.leaf(t, "manager.test", x509.ExtKeyUsageServerAuth)
	files["client-ca.crt"] = append(append([]byte(nil), oldCA.pem...), newCA.pem...)
	publish(t, dir, "overlap", files)
	client.RootCAs, err = CAPool(files["client-ca.crt"], false)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(); err != nil {
		t.Fatal("CA overlap", err)
	}
	files["client-ca.crt"] = newCA.pem
	publish(t, dir, "retired-old", files)
	if err := handshake(); err == nil {
		t.Fatal("retired client CA accepted")
	}
	cert, key := newCA.leaf(t, "intended-client", x509.ExtKeyUsageClientAuth)
	client.Certificates[0], err = tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(); err != nil {
		t.Fatal("new CA client rejected", err)
	}
	cert, key = newCA.leaf(t, "other-workload", x509.ExtKeyUsageClientAuth)
	client.Certificates[0], err = tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(); err == nil {
		t.Fatal("other workload client identity accepted")
	}
}
