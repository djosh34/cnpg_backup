package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNativeProcessHelper(t *testing.T) {
	mode := os.Getenv("NATIVE_PROCESS_TEST")
	if mode == "" {
		return
	}
	switch mode {
	case "grandchild":
		for {
			time.Sleep(time.Hour)
		}
	case "parent":
		child := exec.Command(os.Args[0], "-test.run=^TestNativeProcessHelper$")
		child.Env = []string{"NATIVE_PROCESS_TEST=grandchild"}
		if e := child.Start(); e != nil {
			os.Exit(5)
		}
		if os.WriteFile(os.Getenv("PID_FILE"), []byte(strconv.Itoa(child.Process.Pid)), 0600) != nil {
			os.Exit(6)
		}
		for {
			time.Sleep(time.Hour)
		}
	case "output":
		for {
			_, _ = os.Stdout.Write([]byte(strings.Repeat("x", 64<<10)))
		}
	case "success":
		os.Stdout.WriteString("bounded native output")
		os.Exit(0)
	}
	os.Exit(7)
}
func TestNativeCancellationKillsAndReapsStreamingChild(t *testing.T) {
	file := filepath.Join(t.TempDir(), "child")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, e := runNative(ctx, []string{"NATIVE_PROCESS_TEST=parent", "PID_FILE=" + file}, os.Args[0], "-test.run=^TestNativeProcessHelper$")
		result <- e
	}()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		b, e := os.ReadFile(file)
		if e == nil {
			pid, _ = strconv.Atoi(string(b))
			break
		}
		time.Sleep(time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("child precondition not established")
	}
	if e := syscall.Kill(pid, 0); e != nil {
		t.Fatal("child not alive before cancellation", e)
	}
	cancel()
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("canceled process succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process drain hung")
	}
	if e := syscall.Kill(pid, 0); e != syscall.ESRCH {
		t.Fatal("native streaming child survives or is zombie", pid, e)
	}
}
func TestNativeOutputLimitAndAllowlist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e := runNative(ctx, []string{"NATIVE_PROCESS_TEST=output"}, os.Args[0], "-test.run=^TestNativeProcessHelper$"); e == nil {
		t.Fatal("output overflow succeeded")
	}
	b, e := runNative(ctx, []string{"NATIVE_PROCESS_TEST=success"}, os.Args[0], "-test.run=^TestNativeProcessHelper$")
	if e != nil || string(b) != "bounded native output" {
		t.Fatal(e)
	}
	if _, e = runTool(ctx, nil, "/bin/sh", "-c", "exit 0"); e == nil {
		t.Fatal("runtime shell accepted")
	}
}
