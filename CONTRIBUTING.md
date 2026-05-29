# Contributing to the Kafka Connector Migration Utility

Thanks for your interest in contributing! This guide explains how to set up your
environment, how the code is organized, and what we expect in a pull request.

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Getting started](#getting-started)
- [Project layout](#project-layout)
- [Running the tool locally](#running-the-tool-locally)
- [Running the tests](#running-the-tests)
- [Adding support for a new connector](#adding-support-for-a-new-connector)
- [Coding guidelines](#coding-guidelines)
- [Commit and pull request workflow](#commit-and-pull-request-workflow)

## Code of conduct

Be respectful and constructive. Assume good intent, keep discussions focused on
the technical problem, and help reviewers and other contributors succeed.

## Getting started

### Prerequisites

- Python 3.8+
- `pip` and (recommended) a virtual environment

### Set up your environment

```bash
# Clone your fork
git clone <your-fork-url>
cd connect-migration-utility

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate

# Install runtime dependencies
pip install -r requirements.txt

# Install the test runner
pip install pytest
```

### (Optional) Download the semantic-matching model

Some matching logic uses the `all-MiniLM-L6-v2` sentence-transformer model. It is
**not** required for the test suite (tests stub it out), but it is used at runtime
for semantic config matching. See [MODEL_SETUP.md](MODEL_SETUP.md) for details:

```bash
python download_model.py
```

## Project layout

```
src/
  discovery_script.py            # CLI: discover/translate self-managed configs
  migrate_connector_script.py    # CLI: create fully-managed connectors on Confluent Cloud
  connector_comparator.py        # Core orchestrator (composed of mixins below)
  comparator/
    template_resolver.py         # TemplateResolverMixin: pick the FM template
    config_mapper.py             # ConfigMapperMixin: map SM config -> FM config
    config_deriver.py            # ConfigDeriverMixin: derive/default FM values
  config_discovery.py            # Fetch configs from worker URLs / files
  semantic_matcher.py            # Semantic property matching (uses the ML model)
  *_v1_to_v2_transformer.py      # Per-connector v1->v2 transformers (http, bigquery)
  debezium_v1_to_v2_translator.py
  offset_manager.py              # Offset handling during migration
  terraform_generator.py         # Emit Terraform for migrated connectors
  summary.py                     # Migration summary reporting

templates/fm/                    # Resolved fully-managed connector templates (JSON)
fm_transforms_list.json          # Known Confluent Cloud SMTs/transforms
tests/                           # Test suite (see below)
```

`ConnectorComparator` is intentionally composed from focused mixins
(`TemplateResolverMixin`, `ConfigMapperMixin`, `ConfigDeriverMixin`). When adding
behavior, prefer extending the mixin that owns that concern rather than growing the
base class.

## Running the tool locally

Discover and translate self-managed connector configurations:

```bash
python src/discovery_script.py \
  --worker-urls "http://worker1:8083,http://worker2:8083" \
  --output-dir output/
```

Migrate to Confluent Cloud:

```bash
python src/migrate_connector_script.py \
  --worker-urls "<WORKER_URL>" \
  --cluster-id "<CLUSTER_ID>" \
  --environment-id "<ENVIRONMENT_ID>" \
  --migration-mode "create" \
  --bearer-token "<BEARER_TOKEN>" \
  --fm-config-dir "<INPUT_FM_CONFIGS_DIR>" \
  --migration-output-dir "<OUTPUT_DIR>"
```

See [README.md](README.md) for the full set of flags and options.

## Running the tests

The suite uses `pytest`. `tests/conftest.py` puts `src/` on `sys.path` and stubs
the semantic matcher, so tests are deterministic and never touch the network or the
ML model.

```bash
# Run everything
pytest

# Run a single layer
pytest tests/unit
pytest tests/integration

# Run one file or test
pytest tests/unit/test_config_mapper.py
pytest tests/unit/test_config_mapper.py::test_some_case
```

Test layout:

- `tests/unit/` — fast, isolated tests for individual modules/mixins.
- `tests/integration/` — end-to-end flows and recorded SM→FM pairings.
- `tests/fixtures/` — sample discovered configs and SM/FM pairs used by tests.

Useful fixtures (defined in `tests/conftest.py`):

- `make_comparator` — builds a `ConnectorComparator` wired for offline testing.
- `write_template` / `template_factory` — construct FM templates on disk.
- `jdbc_source_template`, `sample_jdbc_config` — realistic sample inputs.

**Please add or update tests for any behavior change.** New connector support and
mapping logic should come with fixtures and assertions.

## Adding support for a new connector

1. Add the resolved fully-managed template to `templates/fm/<ConnectorName>_resolved_templates.json`.
2. If the connector needs v1→v2 translation or special handling, add/extend a
   transformer in `src/` and wire it into `ConnectorComparator`.
3. Add a fixture under `tests/fixtures/` and a test (unit and/or integration) that
   exercises the mapping.
4. Update [README.md](README.md) if the new connector affects documented behavior.

## Coding guidelines

- Target Python 3.8+ compatibility.
- Match the style of the surrounding code: type hints, module-level loggers
  (`self.logger = logging.getLogger(__name__)`), and clear docstrings.
- Keep changes scoped to the relevant mixin/module; avoid unrelated refactors in a
  feature PR.
- Do not commit secrets, credentials, or real worker URLs. Sensitive values are
  redacted intentionally — don't weaken that logic.
- Don't commit runtime artifacts; `migration_output/`, `output/`, and `tmp_input/`
  are git-ignored.

## Commit and pull request workflow

1. Create a topic branch off `master`.
2. Make your change with accompanying tests.
3. Run the suite locally and make sure it passes:
   ```bash
   pytest
   ```
4. Write clear, focused commits.
5. Open a pull request against `master`. In the description, explain **what**
   changed and **why**, and reference any related issue.
6. PRs are reviewed by the code owners (`@confluentinc/connect`). Address review
   feedback by pushing follow-up commits to the same branch.

Keep PRs as small and focused as practical — it makes review faster and safer.

Thanks for contributing!