# Tests

Unit tests for the pure-function modules extracted during the refactor.

## Running

```bash
# Install the test extra
pip install -e ".[test]"

# Run everything
pytest

# Run just the unit suite
pytest tests/unit/
```

## Coverage

These tests are deliberately focused on modules that don't need heavy
dependencies (no `sentence-transformers`, `torch`, or network access):

| File | Module under test |
|------|-------------------|
| `test_jdbc_url_parser.py` | `mapper.jdbc.url_parser` |
| `test_database_inferrer.py` | `mapper.jdbc.database_inferrer` |
| `test_sensitive_data_redactor.py` | `utils.sensitive_data_redactor` |
| `test_smt_classifier.py` | `mapper.smt.smt_classifier` |
| `test_direct_mappings.py` | `mapper.properties.direct_mappings` |
| `test_config_def_processor.py` | `mapper.properties.config_def_processor` |
| `test_field_derivers.py` | `mapper.properties.derivations` (facade + a representative group) |
| `test_template_loader.py` | `mapper.templates.{template_loader,connector_class_index}` |

## What's not covered yet

- End-to-end snapshot regression of the discovery flow (would require
  pinning the semantic-matcher model output, which is non-deterministic
  without a fixed local model). Left for follow-up.
- HTTP-touching modules (`worker_rest_client`, `translate_api_client`,
  `sm_template_fetcher`, `fm_smt_registry`'s HTTP path) — would need
  mocking the `requests` layer.
- The orchestrator methods on `ConnectorMapper` (`process_connectors`,
  `transformSMToFm`) — too many collaborators to unit-test in isolation;
  best covered via an integration test.
