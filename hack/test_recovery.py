"""Exercise the harness's real curl argv against disposable loopback HTTP."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest

from recovery import s3_command, create_bucket


class S3IsolationTests(unittest.TestCase):
    def test_bucket_startup_retries_only_exact_MinIO_error(self):
        for code in ('XMinioServerNotInitialized', 'AccessDenied', 'ServiceUnavailable'):
            observed = []
            class Handler(BaseHTTPRequestHandler):
                def do_PUT(self):
                    observed.append(self.path)
                    body = (f'<Error><Code>{code}</Code></Error>').encode() if len(observed) == 1 else b''
                    self.send_response(503 if body else 200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                def log_message(self, *_args):
                    pass
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                with tempfile.TemporaryDirectory() as directory:
                    config = Path(directory) / 'curl.conf'
                    config.write_text('noproxy = "*"\n')
                    outputs = []
                    def run(command, check):
                        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=10)
                        outputs.append(result.stdout.strip())
                        return result.returncode, result.stdout.strip()
                    command = s3_command(config, f'http://127.0.0.1:{server.server_port}', 'PUT', '')
                    if code == 'XMinioServerNotInitialized':
                        create_bucket(run, command, sleep=lambda _: None)
                        self.assertEqual(len(observed), 2)
                    else:
                        with self.assertRaises(RuntimeError):
                            create_bucket(run, command, sleep=lambda _: None)
                        self.assertEqual(len(observed), 1)
                    self.assertIn(code, outputs[0], 'first failure body must be retained')
            finally:
                server.shutdown()
                thread.join()
                server.server_close()

    def test_ambient_curl_configuration_cannot_add_targets_or_traces(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_PUT(self):
                requests.append(self.path)
                self.send_response(200)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            endpoint = f'http://127.0.0.1:{server.server_port}'
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trace = root / 'ambient.trace'
                (root / '.curlrc').write_text(f'url = "{endpoint}/ambient-target"\ntrace = "{trace}"\n')
                config = root / 'explicit.conf'
                config.write_text('user = "synthetic:synthetic"\naws-sigv4 = "aws:amz:us-east-1:s3"\nnoproxy = "*"\n')
                env = os.environ | {'CURL_HOME': directory, 'HOME': directory}
                subprocess.run(s3_command(config, endpoint, 'PUT', ''), env=env, check=True, capture_output=True, timeout=10)
                self.assertEqual(requests, ['/foundation/'])
                self.assertFalse(trace.exists(), 'ambient config must not leak signing credentials to a trace')
        finally:
            server.shutdown()
            thread.join()
            server.server_close()


if __name__ == '__main__':
    unittest.main()
