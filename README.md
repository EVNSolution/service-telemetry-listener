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
- fixture publish helpers:
  - `../../development/integration-local-stack/scripts/publish_sample_telemetry.sh`
  - `../../development/integration-local-stack/scripts/publish_malformed_telemetry.sh`

## Image Build / Deploy Contract

- prod contract is build, test, and immutable image publish only
- production runtime rollout ownership belongs to `runtime-prod-release`
- build and publish auth uses `ECR_BUILD_AWS_ROLE_ARN` plus shared `AWS_REGION`


- GitHub Actions workflow 이름은 `Build service-telemetry-listener image` 다.
- workflow는 immutable `service-telemetry-listener:<sha>` 이미지를 ECR로 publish 한다.
- runtime rollout은 `../runtime-prod-release/` 가 소유한다.
- production runtime shape와 canonical inventory는 `../runtime-prod-platform/` 이 소유한다.

## Environment Files And Safety Notes

- 이 worker는 `Slice 7b` 이고 `desired=0` 가 기본값이다.
- public HTTP proof가 없으므로 honest verification은 ECS state, CloudWatch logs, broker connectivity 로 본다.
- broker endpoint와 credentials 확정 전에는 활성화하지 않는다.

## Key Tests Or Verification Commands

- worker boot: `. .venv/bin/activate && python -m telemetry_listener.main`
- local malformed payload smoke: `../../development/integration-local-stack/scripts/publish_malformed_telemetry.sh`
- local sample payload smoke: `../../development/integration-local-stack/scripts/publish_sample_telemetry.sh`

## Root Docs / Runbooks

- `../../docs/boundaries/`
- `../../docs/mappings/`
- `../../docs/runbooks/ev-dashboard-ui-smoke-and-decommission.md`
- `../../docs/decisions/specs/2026-03-21-telemetry-listener-design.md`
- `../../docs/archive/historical/rollout/2026-03-21-telemetry-listener-implementation-plan.md`
