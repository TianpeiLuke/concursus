# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-07

Initial alpha — the declarative core.

### Added

- **`AgentDAG`** — a pure, backend-agnostic directed acyclic graph of agents/tasks:
  `add_node`/`add_edge`, `get_dependencies`/`get_dependents`, `sources`/`sinks`,
  `topological_sort` (Kahn's, raises on a cycle), `validate`, and `to_dict`/`from_dict`.
- **`AgentManifest`** — the `.agent.yaml` model (registry + contract + spec) with
  `from_yaml`/`from_dict` and `validate` (requires a hosting binding and a mandatory output
  JSON Schema — the dependency resolver's type gate).
- **`concursus` CLI** — `info`, `validate <manifest.yaml>...`, `--version`.
- Packaging: PyPI-ready (`pyproject.toml`, dynamic version from `VERSION`, `src/` layout,
  `py.typed`, `concursus[agentcore]` / `concursus[dev]` extras).
