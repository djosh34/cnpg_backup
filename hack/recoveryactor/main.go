// Test-only CNPG command observer / admission arrangement. Never shipped in a
// subject image. The actual pinned CNPG command still performs preflight/replay.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"fmt"
	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	admission "k8s.io/api/admission/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const root = "/controller/campaign"
const socket = "/plugins/cnpg-backup.djosh34.github.io"
const socketRoot = "/campaign-sockets"
const upstreamSocket = socketRoot + "/upstream.sock"

var eventMu sync.Mutex

func must(e error) {
	if e != nil {
		panic("campaign actor failed (details withheld)")
	}
}
func mark(name string)        { must(os.WriteFile(filepath.Join(root, name), []byte("observed\n"), 0600)) }
func exists(name string) bool { _, e := os.Stat(filepath.Join(root, name)); return e == nil }
func wait(ctx context.Context, name string) error {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		if exists(name) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
func event(facts map[string]any) {
	eventMu.Lock()
	defer eventMu.Unlock()
	facts["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	f, e := os.OpenFile(root+"/rpc.jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	must(json.NewEncoder(f).Encode(facts))
	must(f.Sync())
	must(f.Close())
}
func copyFile(src, dst string) { b, e := os.ReadFile(src); must(e); must(os.WriteFile(dst, b, 0555)) }
func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "retention":
			retentionBatch()
			return
		case "webhook":
			webhook()
			return
		case "install":
			must(os.Mkdir(root, 0700))
			must(os.Rename("/controller/manager", root+"/cnpg-original"))
			copyFile("/actor", "/controller/manager")
			return
		case "proxy":
			serveProxy()
			return
		case "stale":
			stale()
			return
		case "processes":
			must(json.NewEncoder(os.Stdout).Encode(processes(false)))
			return
		case "stop-cnpg":
			must(json.NewEncoder(os.Stdout).Encode(processes(true)))
			return
		}
	}
	if len(os.Args) < 3 || os.Args[1] != "instance" || os.Args[2] != "restore" {
		must(syscall.Exec(root+"/cnpg-original", append([]string{"/controller/manager"}, os.Args[1:]...), os.Environ()))
		return
	}
	if os.Getpid() == 1 {
		panic("actual guard must remain PID1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	mark("before-rpc") // guard Begin has already succeeded; original CNPG has not run.
	must(wait(ctx, "release-before"))
	if exists("fail-before-preflight") {
		// Genuine failed first Job attempt, without target materialization.
		// Returning normally lets the actual guard drain and release its markers.
		event(map[string]any{"event": "injected-preflight-exit", "exit": 42})
		os.Exit(42)
	}
	must(relocateSocket(socket, socketRoot))
	event(map[string]any{"event": "socket-relocated-same-mount", "source": socketRoot + socket, "destination": upstreamSocket})
	proxy := exec.Command("/controller/manager", "proxy")
	proxy.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	proxy.Stdout, proxy.Stderr = os.Stdout, os.Stderr
	must(proxy.Start())
	must(wait(ctx, "proxy-ready"))
	// This wrapper deliberately stays a child of the REAL product PID1 guard.
	// Its proxy is an actual detached descendant, not a simulated guard task.
	command := exec.Command(root+"/cnpg-original", os.Args[1:]...)
	command.Stdout, command.Stderr, command.Stdin = os.Stdout, os.Stderr, os.Stdin
	e := command.Run()
	code := 0
	if e != nil {
		var exit *exec.ExitError
		if errors.As(e, &exit) {
			code = exit.ExitCode()
		} else {
			code = 254
		}
	}
	event(map[string]any{"event": "actual-cnpg-exit", "exit": code})
	mark("cnpg-exited")
	must(wait(ctx, "release-shutdown"))
	writer, e := os.ReadFile(root + "/release-shutdown")
	must(e)
	writerPID, e := strconv.Atoi(string(writer))
	must(e)
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, 30*time.Second)
	defer cancelShutdown()
	must(waitProcessExit(shutdownCtx, writerPID))
	// Do NOT reap proxy: product guard must adopt/reap detached descendants.
	os.Exit(code)
}

// The shutdown marker's writer is an exec process in the guarded namespace.
// Let it exit before this wrapper exits and PID1 signals every remaining process;
// marker visibility alone otherwise races the writer's successful exec result.
func waitProcessExit(ctx context.Context, pid int) error {
	if pid <= 1 {
		return errors.New("invalid shutdown marker writer")
	}
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// /plugins is a subPath bind mount. Rename via the full volume alias for BOTH
// paths: merely using another alias as the destination still gives EXDEV.
func relocateSocket(discovery, volumeRoot string) error {
	source := filepath.Join(volumeRoot, "plugins", filepath.Base(discovery))
	a, e := os.Stat(discovery)
	if e != nil {
		return e
	}
	b, e := os.Stat(source)
	if e != nil {
		return e
	}
	if !os.SameFile(a, b) || a.Mode()&os.ModeSocket == 0 {
		return errors.New("observer socket aliases do not identify the same real listener")
	}
	return os.Rename(source, filepath.Join(volumeRoot, "upstream.sock"))
}

type rawCodec struct{}

func (rawCodec) Name() string                  { return "proto" }
func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(b []byte, v any) error {
	*v.(*[]byte) = append((*v.(*[]byte))[:0], b...)
	return nil
}
func serveProxy() {
	conn, e := grpc.NewClient("unix://"+upstreamSocket, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{}), grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)))
	must(e)
	defer conn.Close()
	listener, e := net.Listen("unix", socket)
	must(e)
	must(os.Chmod(socket, 0600))
	server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20), grpc.UnknownServiceHandler(func(_ any, down grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(down)
		ctx, cancel := context.WithCancel(down.Context())
		defer cancel()
		up, e := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method)
		if e != nil {
			return e
		}
		errors := make(chan error, 2)
		names := make(chan string, 1)
		go func() {
			for {
				var b []byte
				e := down.RecvMsg(&b)
				if e == io.EOF {
					errors <- up.CloseSend()
					return
				}
				if e != nil {
					errors <- e
					return
				}
				if strings.HasSuffix(method, "/Restore") && strings.Contains(method, "WAL") {
					var request wire.WALRestoreRequest
					if proto.Unmarshal(b, &request) == nil {
						event(map[string]any{"event": "wal-request", "name": request.SourceWalName})
						names <- request.SourceWalName
						if exists("hold-replay") {
							mark("replay-held")
							if e = wait(ctx, "release-replay"); e != nil {
								errors <- e
								return
							}
						}
					}
				}
				if e = up.SendMsg(&b); e != nil {
					errors <- e
					return
				}
			}
		}()
		go func() {
			name := ""
			if method == wire.WAL_Restore_FullMethodName {
				select {
				case name = <-names:
				case <-ctx.Done():
					errors <- ctx.Err()
					return
				}
			}
			for {
				var b []byte
				e := up.RecvMsg(&b)
				if e != nil {
					if strings.HasSuffix(method, "/Restore") {
						code := status.Code(e)
						if e == io.EOF { // successful upstream stream completion, not an RPC error
							code = codes.OK
						}
						event(map[string]any{"event": "RPC-result", "method": method, "status": code.String()})
					}
					if method == wire.WAL_Restore_FullMethodName && e != io.EOF {
						if exists("incorrect-EOF") && status.Code(e) != codes.NotFound {
							e = status.Error(codes.NotFound, "deliberately incorrect EOF negative control")
						}
						if exists("incorrect-all-fatal") && status.Code(e) == codes.NotFound && bundled(name) {
							e = status.Error(codes.Unavailable, "deliberately incorrect duplicate-miss negative control")
						}
					}
					errors <- e
					return
				}
				if strings.HasSuffix(method, "/Restore") && strings.Contains(method, "RestoreJob") {
					mark("restore-response")
					if e = wait(ctx, "release-response"); e != nil {
						errors <- e
						return
					}
				}
				if method == wire.WAL_Restore_FullMethodName {
					observedName, _ := os.ReadFile(root + "/observe-wal")
					if string(observedName) == name {
						digest, size := walDigest("/var/lib/postgresql/wal/pg_wal/RECOVERYXLOG")
						event(map[string]any{"event": "actual-upstream-WAL", "name": name, "sha256": digest, "bytes": size})
					}
					if exists("incorrect-bundle-success") && bundled(name) {
						copyBundle(name)
					}
				}
				if e = down.SendMsg(&b); e != nil {
					errors <- e
					return
				}
			}
		}()
		// Sending CloseSend is not stream completion: await upstream response/status.
		for range 2 {
			e := <-errors
			if e == io.EOF {
				return nil
			}
			if e != nil {
				return e
			}
		}
		return nil
	}))
	mark("proxy-ready")
	must(server.Serve(listener))
}

// Inspect only the actual synthetic recovery namespace, emitting identities,
// never arbitrary process argv/environments. stop-cnpg simulates its death while
// PostgreSQL survives, forcing the real guard's orphan adoption/reaping path.
func processes(stop bool) []map[string]any {
	entries, e := os.ReadDir("/proc")
	must(e)
	out := []map[string]any{}
	killed := 0
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid <= 1 || pid == os.Getpid() {
			continue
		}
		cmd, e := os.ReadFile("/proc/" + entry.Name() + "/cmdline")
		if e != nil || len(cmd) > 8192 {
			continue
		}
		parts := strings.Split(string(cmd), "\x00")
		if len(parts) == 0 {
			continue
		}
		kind := ""
		if parts[0] == root+"/cnpg-original" && len(parts) > 2 && parts[1] == "instance" && parts[2] == "restore" {
			kind = "cnpg"
		}
		if filepath.Base(parts[0]) == "postgres" || strings.HasPrefix(parts[0], "postgres: ") {
			kind = "postgres"
		}
		switch filepath.Base(parts[0]) {
		case "pg_basebackup", "pg_verifybackup", "pg_waldump", "pg_combinebackup":
			kind = "native"
		}
		if kind == "" {
			continue
		}
		stat, e := os.ReadFile("/proc/" + entry.Name() + "/stat")
		if e != nil {
			continue
		}
		end := strings.LastIndex(string(stat), ")")
		if end < 0 {
			continue
		}
		fields := strings.Fields(string(stat)[end+1:])
		if len(fields) < 4 {
			continue
		}
		ppid, _ := strconv.Atoi(fields[1])
		group, _ := strconv.Atoi(fields[2])
		session, _ := strconv.Atoi(fields[3])
		out = append(out, map[string]any{"pid": pid, "parent": ppid, "group": group, "session": session, "kind": kind, "state": fields[0]})
		if stop && kind == "cnpg" {
			must(syscall.Kill(pid, syscall.SIGKILL))
			killed++
		}
	}
	if stop && killed != 1 {
		panic("ineffective CNPG death injection")
	}
	return out
}

func bundled(name string) bool {
	b, e := os.ReadFile("/cnpg-backup/state/recovery.json")
	if e != nil {
		return false
	}
	var p struct {
		Bundled map[string]json.RawMessage `json:"bundled"`
	}
	if json.Unmarshal(b, &p) != nil {
		return false
	}
	_, ok := p.Bundled[name]
	return ok
}

// Observe delivered bytes before the deliberately wrong negative can replace
// them. Bounded read-only test evidence, never a subject-image service.
func walDigest(path string) (string, int64) {
	f, e := os.Open(path)
	must(e)
	defer f.Close()
	hash := sha256.New()
	n, e := io.Copy(hash, io.LimitReader(f, (64<<20)+1))
	must(e)
	if n > 64<<20 {
		panic("observed WAL exceeds campaign bound")
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), n
}

func copyBundle(name string) {
	if !bundled(name) || len(name) != 24 {
		panic("invalid controlled bundle name")
	}
	src, e := os.Open("/var/lib/postgresql/wal/pg_wal/" + name)
	must(e)
	defer src.Close()
	dst, e := os.OpenFile("/var/lib/postgresql/wal/pg_wal/RECOVERYXLOG", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	must(e)
	_, e = io.Copy(dst, io.LimitReader(src, 64<<20))
	must(e)
	must(dst.Sync())
	must(dst.Close())
	event(map[string]any{"event": "deliberately-incorrect-bundle-as-archive-success", "name": name})
}

// Exercise the existing real Unix WAL RPC with a structurally valid but stale
// incarnation. No plan rewrite or product admission test hook is involved.
func stale() {
	b, e := os.ReadFile("/cnpg-backup/state/recovery.json")
	must(e)
	var p struct {
		Tuple   map[string]json.RawMessage `json:"tuple"`
		Cluster json.RawMessage            `json:"cluster_definition"`
		Bundled map[string]json.RawMessage `json:"bundled"`
	}
	must(json.Unmarshal(b, &p))
	p.Tuple["sidecarUID"] = json.RawMessage(`"ffffffff-ffff-4fff-8fff-ffffffffffff"`)
	tuple, e := json.Marshal(p.Tuple)
	must(e)
	conn, e := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(e)
	defer conn.Close()
	name := ""
	for n := range p.Bundled {
		if name == "" || n < name {
			name = n
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, e = wire.NewWALClient(conn).Restore(ctx, &wire.WALRestoreRequest{ClusterDefinition: p.Cluster, SourceWalName: name, DestinationFileName: "pg_wal/RECOVERYXLOG", Mode: wire.WALRestoreRequest_MODE_RECOVERY, Parameters: map[string]string{"recoveryID": string(tuple)}})
	if status.Code(e) != codes.FailedPrecondition {
		panic("stale tuple was not rejected by actual sidecar")
	}
	fmt.Println("stale tuple rejected: FailedPrecondition")
}

func webhook() {
	image := os.Getenv("ACTOR_IMAGE")
	if !strings.Contains(image, "@sha256:") {
		panic("immutable actor image required")
	}
	server := http.Server{Addr: ":9443", Handler: observerHandler(image), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 32 << 10}
	must(server.ListenAndServeTLS("/tls/tls.crt", "/tls/tls.key"))
}

func observerHandler(image string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var review admission.AdmissionReview
		if json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&review) != nil || review.Request == nil {
			http.Error(w, "invalid", 400)
			return
		}
		response := &admission.AdmissionResponse{UID: review.Request.UID, Allowed: true}
		var pod core.Pod
		if json.Unmarshal(review.Request.Object.Raw, &pod) != nil {
			response.Allowed = false
		} else if pod.Namespace == "campaign-target" || review.Request.Namespace == "campaign-target" {
			for index, c := range pod.Spec.Containers {
				if c.Name != "full-recovery" {
					continue
				}
				mounts := []core.VolumeMount{}
				var sockets *core.VolumeMount
				for _, m := range c.VolumeMounts {
					if m.MountPath == "/plugins" && m.SubPath == "plugins" && !m.ReadOnly {
						sockets = &core.VolumeMount{Name: m.Name, MountPath: socketRoot}
					}
					if m.MountPath == "/controller" {
						m.ReadOnly = false
						mounts = append(mounts, m)
					}
				}
				if len(mounts) != 1 || sockets == nil {
					response.Allowed = false
					response.Result = &meta.Status{Message: "test actor requires actual CNPG controller mount"}
					break
				}
				user := int64(26)
				no := false
				yes := true
				installer := core.Container{Name: "campaign-observer-install", Image: image, Command: []string{"/actor", "install"}, VolumeMounts: mounts,
					SecurityContext: &core.SecurityContext{RunAsUser: &user, RunAsGroup: &user, RunAsNonRoot: &yes, ReadOnlyRootFilesystem: &yes, AllowPrivilegeEscalation: &no, Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}}}
				// Replacements copy an already observed Pod, including its init
				// containers and mounts. Admission must not append either twice.
				installed := false
				for _, init := range pod.Spec.InitContainers {
					installed = installed || init.Name == installer.Name
				}
				mounted := false
				for _, mount := range c.VolumeMounts {
					mounted = mounted || mount.MountPath == socketRoot
				}
				var patch []map[string]any
				if !installed {
					patch = append(patch, map[string]any{"op": "add", "path": "/spec/initContainers/-", "value": installer})
				}
				if !mounted {
					patch = append(patch, map[string]any{"op": "add", "path": fmt.Sprintf("/spec/containers/%d/volumeMounts/-", index), "value": sockets})
				}
				if len(patch) == 0 {
					continue
				}
				b, e := json.Marshal(patch)
				must(e)
				kind := admission.PatchTypeJSONPatch
				response.Patch = b
				response.PatchType = &kind
			}
		}
		review.Response = response
		review.Request = nil
		w.Header().Set("Content-Type", "application/json")
		must(json.NewEncoder(w).Encode(review))
	})
}
