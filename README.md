# service-telemetry-listener

## Purpose / Boundary

`service-telemetry-listener` is the MQTT ingress worker for telemetry payloads.

Current role:
- subscribe to MQTT topics from the broker
- parse source identity hints from incoming payloads
- forward raw-first ingest requests to `service-telemetry-hub`
- apply retry classification and log ingest outcomes

Non-owned concerns:
- database writes
- timeseries normalization
- latest snapshot or diagnostic persistence
- vehicle or terminal master writes
- public HTTP route ownership
- 플랫폼 전체 compose와 gateway 설정

## Runtime Contract / Local Role

- compose service는 `telemetry-listener` 다.
- public gateway prefix는 없다.
- dependency:
  - `service-telemetry-hub` owns ingest API and telemetry storage
  - `service-telemetry-dead-letter` owns failed-payload admin/read surface
- entrypoint: `entrypoint.sh` -> `python -m telemetry_listener.main`

## Local Run / Verification

- local worker run: `. .venv/bin/activate && python -m telemetry_listener.main`
- fixture publish helpers는 out-of-band local support tooling에서 관리한다.

## Image Build / Deploy Contract

- GitHub Actions workflow 이름은 `Build service-telemetry-listener image` 다.
- workflow는 immutable `service-telemetry-listener:<sha>` 이미지를 ECR로 publish 한다.
- production rollout은 `../runtime-prod-release/` 가 수행하고, runtime shape와 inventory는 `../runtime-prod-platform/` 이 소유한다.

## Environment Files And Safety Notes

- 이 worker는 internal-only MQTT ingress worker이고 `desired=0` 가 기본값이다.
- public HTTP proof가 없으므로 honest verification은 runtime state, service logs, broker connectivity 로 본다.
- broker endpoint와 credentials 확정 전에는 활성화하지 않는다.

## Key Tests Or Verification Commands

- worker boot: `. .venv/bin/activate && python -m telemetry_listener.main`
- malformed/sample payload smoke는 out-of-band local support tooling을 사용한다.

## Root Docs / Runbooks

- `../../docs/boundaries/`
- `../../docs/mappings/`
- `../../docs/runbooks/ev-dashboard-ui-smoke-and-decommission.md`
- `../../docs/decisions/specs/2026-03-21-telemetry-listener-design.md`
- `../../docs/archive/historical/rollout/2026-03-21-telemetry-listener-implementation-plan.md`

## Root Development Whitelist

- 이 repo는 `clever-msa-platform` root `development/` whitelist에 포함된다.
- root visible set은 `front-web-console`, `edge-api-gateway`, `runtime-prod-release`, `runtime-prod-platform`, active `service-*` repo만 유지한다.
- local stack support repo, legacy infra repo, bridge lane repo는 root `development/` whitelist 바깥에서 관리한다.
- 이 README와 repo-local AGENTS는 운영 안내 문서이며 정본이 아니다. 경계, 계약, 런타임 truth는 root `docs/`를 따른다.
