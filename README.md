# service-telemetry-listener

`service-telemetry-listener` is the MQTT ingress worker for telemetry payloads.

Current role:
- subscribe to MQTT topics from the broker
- parse source identity hints from incoming payloads
- forward raw-first ingest requests to `service-telemetry-hub`
- apply retry classification and log ingest outcomes

Future role:
- dead-letter handling
- topic routing expansion
- richer source-identity parsing and validation before hub forwarding

Non-owned concerns:
- database writes
- timeseries normalization
- latest snapshot or diagnostic persistence
- vehicle or terminal master writes

Dependency:
- `service-telemetry-hub` owns the ingest API and all telemetry storage

Entry point:
- `entrypoint.sh` -> `python -m telemetry_listener.main`

Current truth:
- `../../docs/decisions/specs/2026-03-21-telemetry-listener-design.md`

Historical context:
- `../../docs/archive/historical/rollout/2026-03-21-telemetry-listener-implementation-plan.md`
