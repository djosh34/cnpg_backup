"""Bounded commands and readiness observations for the disposable campaign.

Only operation labels, never argv/input, enter the timeline. Output is drained
concurrently with a byte cap; children cannot fill disk or bypass a wait budget.
"""
import contextlib
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import tempfile
import threading
import time
import base64


def redact(text):
    text = re.sub(r'-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|\Z)',
                  '<REDACTED>', text, flags=re.S)
    for value in ('disposable-test-only-access', 'disposable-test-only-secret'):
        text = text.replace(value, '<REDACTED>').replace(base64.b64encode(value.encode()).decode(), '<REDACTED>')
    text = re.sub(r'(?i)(\"(?:password|token|secret|auth|tls\.key|private\.key)\"\s*:\s*\")[^\"]*', r'\1<REDACTED>', text)
    text = re.sub(r'(?i)(authorization\s*[:=]\s*)(?:Bearer |Basic )?[^\s,\"}]+', r'\1<REDACTED>', text)
    text = re.sub(r'(?i)((?:password|token|secret|access.?key)\s*[=:]\s*)[^\s,\"}]+', r'\1<REDACTED>', text)
    return re.sub(r'://[^/\s]+@', '://<REDACTED>@', text)


class CommandFailure(RuntimeError):
    pass


@contextlib.contextmanager
def cleanup(actions, report=None):
    """Run every owned cleanup independently; primary always precedes cleanup."""
    primary = None
    errors = []
    def record(error):
        if report:
            try:
                report(error)
            except BaseException as recording_error:
                recording_error.campaign_phase = 'failure-recording'
                errors.append(recording_error)
    try:
        yield
    except BaseException as error:
        primary = error
        record(error)  # durable first failure BEFORE any potentially failing reset
        raise
    finally:
        for phase, action in actions:
            try:
                action()
            except BaseException as error:
                error.campaign_phase = phase
                error.campaign_primary = primary
                errors.append(error)
                record(error)
        if errors:
            raise BaseExceptionGroup('operation/cleanup failures', ([primary] if primary else []) + errors) from None


@dataclass(frozen=True)
class CommandResult:
    operation: str
    returncode: int
    stdout: str
    stderr: str
    duration: float
    timed_out: bool
    dropped_bytes: int

    @property
    def ok(self):
        return self.returncode == 0 and not self.timed_out

    def require(self):
        if self.dropped_bytes:
            raise CommandFailure(f'{self.operation}: output cap exceeded; refusing incomplete oracle input ({self.dropped_bytes} dropped bytes)')
        if not self.ok:
            raise CommandFailure(f'{self.operation}: ' + ('deadline' if self.timed_out else f'exit={self.returncode}')
                                 + ': ' + (self.stderr or self.stdout)[-4000:])
        return self


class Commands:
    def __init__(self, out, *, cwd=None, deadline=float('inf'), output_limit=1 << 20):
        self.out, self.cwd, self.deadline = out, cwd, deadline
        self.output_limit = output_limit
        self.last = None
        self.sequence = 0
        self.cancelled = threading.Event()
        out.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def budget(self, seconds):
        previous = self.deadline
        self.deadline = min(previous, time.monotonic() + seconds)
        try:
            yield
        finally:
            self.deadline = previous

    def record(self, data):
        path = self.out / 'commands.jsonl'
        # Stop appending, never erase initial evidence. Manifest reports the cap.
        if path.exists() and path.stat().st_size >= 10 << 20:
            return
        with path.open('a') as f:
            f.write(json.dumps({'epoch': time.time(), 'monotonic': time.monotonic(), **data}) + '\n')
            f.flush()

    def command(self, operation, *argv, timeout=300, input=None):
        end = min(self.deadline, time.monotonic() + timeout)
        if time.monotonic() >= end or self.cancelled.is_set():
            raise CommandFailure(operation + ': deadline/cancellation before command')
        started = time.monotonic()
        self.sequence += 1
        self.record({'id': self.sequence, 'operation': operation, 'state': 'started'})
        buffers = {'stdout': bytearray(), 'stderr': bytearray()}
        dropped = 0
        expired = False
        # File-backed input avoids a blocked stdin writer deadlocking output.
        with tempfile.TemporaryFile() as stdin:
            if input is not None:
                stdin.write(input.encode())
                stdin.seek(0)
            child = subprocess.Popen([str(a) for a in argv], cwd=self.cwd, stdin=stdin,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(child.stdout, selectors.EVENT_READ, 'stdout')
                    selector.register(child.stderr, selectors.EVENT_READ, 'stderr')
                    while selector.get_map() or child.poll() is None:
                        if time.monotonic() >= end or self.cancelled.is_set():
                            expired = True
                            break
                        for key, _ in selector.select(min(.1, max(0, end - time.monotonic()))):
                            chunk = os.read(key.fileobj.fileno(), 65536)
                            if not chunk:
                                selector.unregister(key.fileobj)
                                continue
                            buffer = buffers[key.data]
                            keep = min(len(chunk), max(0, self.output_limit - len(buffer)))
                            buffer.extend(chunk[:keep])
                            dropped += len(chunk) - keep
                if expired:
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        child.wait(timeout=.5)
                    except subprocess.TimeoutExpired:
                        pass
                else:
                    child.wait(timeout=max(.01, end - time.monotonic()))
            finally:
                # Kill descendants even if their leader exited but left pipes open.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=1)
                child.stdout.close()
                child.stderr.close()
        result = CommandResult(operation, child.returncode,
                               redact(buffers['stdout'].decode(errors='replace')),
                               redact(buffers['stderr'].decode(errors='replace')),
                               time.monotonic() - started, expired, dropped)
        self.last = result
        self.record({'id': self.sequence, 'operation': operation, 'exit': result.returncode,
                     'duration': result.duration, 'timed_out': expired, 'dropped_bytes': dropped,
                     'diagnostic': result.stderr[-4000:]})
        return result

    def run(self, *argv, check=True, timeout=300, input=None, expect_failure=False, operation=None):
        label = operation or Path(str(argv[0])).name
        result = self.command(label, *argv, timeout=timeout, input=input)
        if result.timed_out or result.dropped_bytes:
            result.require()
        if expect_failure:
            if result.ok:
                raise CommandFailure(label + ': expected rejection but exit=0')
        elif check:
            result.require()
        # Compatibility for existing text oracles. Status predicates must use
        # command().ok; text never represents successful execution by absence.
        return result.stdout + result.stderr

    @contextlib.contextmanager
    def background(self, operation, *argv, timeout=300, save_log, report):
        # The one asynchronous exec caller uses the SAME bounded capture and
        # TERM/KILL/reap implementation as foreground commands. Its cancellation
        # is independent of the parent's expired remote-command budget.
        from concurrent.futures import ThreadPoolExecutor
        owned = Commands(self.out / operation, cwd=self.cwd, deadline=self.deadline, output_limit=self.output_limit)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(owned.command, operation, *argv, timeout=timeout)
            def reap():
                owned.cancelled.set()
                # command() cancellation polls at 100ms, TERM .5s, KILL/reap 1s.
                result = future.result(timeout=3)
                self.record({'background': operation, 'exit': result.returncode,
                             'timed_out': result.timed_out, 'dropped_bytes': result.dropped_bytes})
                save_log(operation + '.log', result.stdout + result.stderr)
                if result.dropped_bytes:
                    result.require()
            with cleanup([('child-reap', reap)], report):
                yield future

    def wait(self, predicate, description, seconds=180):
        with self.budget(seconds):
            while time.monotonic() < self.deadline:
                if predicate():
                    self.record({'barrier': description, 'state': 'observed'})
                    return
                time.sleep(min(.5, max(0, self.deadline - time.monotonic())))
            last = self.last
            raise CommandFailure(f'{description}: deadline; last operation={last.operation if last else "none"}'
                                 f' exit={last.returncode if last else "none"}; '
                                 + (last.stderr[-2000:] if last else 'no command observation'))
