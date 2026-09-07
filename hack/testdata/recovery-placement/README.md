# Recovery placement replay

`observations.json` preserves the redacted container observations and main logs
from real CNPG run 34090997211, subject
`38ef34384b7df29f8293b48a8aa5a57d8800a303` (CNPG 1.30.0).
Original main exits are 2/2/2/1; init/sidecar exit 0 is not main success.
Source: the orchestrator's `pr-d-rspec-diagnostics/head-cnpg/cnpg-smoke`
`recover-{pgdata,wal,tablespace,fresh}-{pod.json,main.log}` artifacts.

The regression invokes the actual `recovery_placement_matrix`, replaying these
observations at its Kubernetes boundary. Minimal Pod specs and PV/file responses
are reconstructed for the documented scenarios; they are not additional saved
cluster observations. Negative controls change main exits or the fresh failure
cause while preserving preflight logs and simulated file/marker outcomes.
This tests the production harness verdict, not real recovery or qualification.
