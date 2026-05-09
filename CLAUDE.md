# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

NG-SA (Next Generation Service Assurance) is the **monitoring and closed-loop automation** backbone of OSM. The architecture was introduced in **v13.0** and coexisted with the old MON/POL components until **v18.0**, when MON was reduced to the Dashboarder only and POL was removed entirely.

The architecture is built around **Airflow DAGs**, **Prometheus** (as TSDB), **Prometheus Recording Rules**, and **AlertManager**. All of these components are deployed as part of the OSM Helm chart (see `devops/`).

## Module Structure

```
src/osm_ngsa/
├── dags/                               # Airflow DAG definitions
│   ├── multivim_vm_metrics.py          # Collect VM perf metrics → Push Gateway (every 5 min)
│   ├── multivim_vm_status.py           # Collect VM operational status (every 1 min)
│   ├── multivim_vim_status.py          # Monitor VIM health
│   ├── multisdnc_sdnc_status.py        # Monitor SDNC health
│   ├── ns_topology.py                  # Collect NS/VNF/VDU topology → Push Gateway (every 2 min)
│   ├── vdu_down.py                     # Triggered by AlertManager: auto-heal downed VDUs
│   ├── vdu_alarm.py                    # Triggered by AlertManager: generic VDU alarms
│   ├── scalein_vdu.py                  # Triggered by AlertManager: scale-in
│   └── scaleout_vdu.py                 # Triggered by AlertManager: scale-out
└── osm_mon/
    ├── core/
    │   ├── config.py                   # YAML + env-var configuration
    │   ├── common_db.py                # OSM MongoDB client
    │   └── message_bus_client.py       # Kafka message bus client
    ├── vim_connectors/
    │   ├── base_vim.py                 # Abstract VIM connector base class
    │   ├── openstack.py                # OpenStack (Gnocchi/Ceilometer)
    │   ├── azure.py                    # Azure Monitor
    │   ├── gcp.py                      # Google Cloud Monitoring
    │   └── vrops_helper.py             # VMware vRealize Operations
    └── sdnc_connectors/
        ├── base_sdnc.py                # Abstract SDNC connector
        └── onos.py                     # ONOS SDN controller

```

> **Note:** `osm_webhook_translator` has been migrated to a separate repository (`osm/webhook-translator`).

## Closed-Loop Monitoring Architecture

```
VIM / Infrastructure (OpenStack, Azure, GCP, K8s)
        │
        ▼ (queried by collection DAGs)
  Airflow DAGs (NG-SA)
        │  multivim_vm_metrics  → osm_cpu_utilization, osm_average_memory_utilization, …
        │  multivim_vm_status   → vm_status
        │  ns_topology          → ns_topology (labels: ns_id, vnf_id, vdu_id, vim_id, …)
        ▼
  Prometheus Push Gateway (:9091)
        │
        ▼
  Prometheus (:9090)
        │  [Recording Rules applied continuously]
        │  vm_status_extended = vm_status joined with ns_topology labels
        │  vnf_status          = min(vm_status_extended) by ns_id, vnf_id
        │  ns_status           = min(vm_status_extended) by ns_id
        │
        │  [Alert Rules evaluated every 15s]
        │  vdu_down:     vm_status_extended != 1  for 3m
        │  scaleout_*:   user-defined thresholds
        │  scalein_*:    user-defined thresholds
        │  vdu_alarm_*:  user-defined alarms
        ▼
  AlertManager
        │  Routes by alertname:
        │    vdu_down      → POST http://webhook-translator:9998/vdu_down
        │    scaleout_.*   → POST http://webhook-translator:9998/scaleout_vdu
        │    scalein_.*    → POST http://webhook-translator:9998/scalein_vdu
        │    vdu_alarm_.*  → POST http://webhook-translator:9998/vdu_alarm
        ▼
  osm_webhook_translator (HTTP server — see osm/webhook-translator repo)
        │  Translates webhook payload → triggers Airflow DAG run
        ▼
  Airflow DAGs (remediation)
        │  vdu_down.py     → reads alert config from MongoDB → publishes ns.heal to Kafka
        │  scaleout_vdu.py → reads scaling rule from MongoDB → publishes ns.scale to Kafka
        │  scalein_vdu.py  → same, for scale-in
        │  vdu_alarm.py    → generic alarm handling
        ▼
  LCM (consumes Kafka) → executes healing / scaling operation
```

## DAG Details

### Collection DAGs (run on schedule, push to Prometheus)

For VM status, VIM status, and VM metrics **one DAG instance is created per VIM** so that collection scales independently per VIM and failures are isolated. The NS topology uses a single shared DAG.

| DAG | Instances | Schedule | Metrics pushed |
|-----|-----------|----------|----------------|
| `multivim_vm_metrics.py` | one per VIM | every 5 min | `osm_cpu_utilization`, `osm_average_memory_utilization`, `osm_disk_read/write_ops/bytes`, `osm_packets_received/sent/dropped` |
| `multivim_vm_status.py` | one per VIM | every 1 min | `vm_status` (labels: `vm_id`, `vim_id`) |
| `multivim_vim_status.py` | one per VIM | periodic | VIM availability |
| `ns_topology.py` | single | every 2 min | `ns_topology` (labels: `ns_id`, `project_id`, `vnf_id`, `vdu_id`, `vm_id`, `vim_id`, `vdu_name`, `ns_name`, `vnf_member_index`) |
| `multisdnc_sdnc_status.py` | one per SDNC | periodic | SDNC availability |

All collection DAGs push to **Prometheus Push Gateway** (`pushgateway-prometheus-pushgateway:9091`) using `prometheus-client`. Job names follow the pattern `airflow_osm_<type>_<vim_id>`.

### Remediation DAGs (triggered by AlertManager via webhook-translator)

| DAG | Trigger | Action |
|-----|---------|--------|
| `vdu_down.py` | `vdu_down` alert | Reads heal config from MongoDB → publishes `ns.heal` to Kafka |
| `scaleout_vdu.py` | `scaleout_*` alert | Reads scaling rule from MongoDB, enforces cooldown → publishes `ns.scale` to Kafka |
| `scalein_vdu.py` | `scalein_*` alert | Same as scaleout, for scale-in direction |
| `vdu_alarm.py` | `vdu_alarm_*` alert | Generic alarm handler |

All remediation DAGs read alert configuration from MongoDB via `CommonDbClient` and publish operations to Kafka via `MessageBusClient`.

## Prometheus Configuration (managed via devops)

Prometheus Recording Rules and Alert Rules are defined in the OSM Helm chart values (`devops/installers/helm/osm/values.yaml`), not in this repo. The Prometheus deployment includes a custom **sidecar** (`devops/docker/Prometheus/`) that dynamically adds scrape jobs and alert rules by watching the MongoDB `prometheus_jobs` and `alerts` collections.

AlertManager is deployed as a sub-component of the Prometheus Helm subchart (`alertmanager.enabled: true`), not as a separate chart.

## Adding a New VIM Connector

1. Create a new file in `osm_mon/vim_connectors/`
2. Subclass `BaseVimConnector`
3. Implement the metric/status query methods
4. Register the connector in the relevant collection DAGs

## Development Commands

```bash
# Install in development mode
pip install -e . -r requirements.txt

# Run all test environments
tox

# Unit tests with coverage
tox -e cover

# Linting
tox -e pylint
tox -e flake8

# Code formatting check
tox -e black

# Build Python wheel + sdist
tox -e dist_ng_sa
```
