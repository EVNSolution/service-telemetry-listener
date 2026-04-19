Source: https://lessons.md

# service-telemetry-listener Lessons.md

This repo is an internal-only MQTT ingress worker. Keep `desired=0` until a real MQTT broker endpoint, credentials, and connectivity checks are all confirmed.

Because this worker has no public HTTP proof, its honest verification is runtime state, service logs, and broker connectivity. Do not turn it on based on assumption or by reusing old hub values.
