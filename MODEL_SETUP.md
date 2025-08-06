# Sentence Transformer Model Setup

This repository includes a local sentence transformer model for semantic matching to avoid downloading the model each time.

## Quick Setup

1. **Download the model** (one-time setup):
   ```bash
   python download_model.py
   ```

2. **Run the migration tool** (uses local model automatically):
   ```bash
   python src/main.py --worker-urls "localhost:8083" --config-file connectors.json --output-dir output1/
   ```

## Model Details

- **Model**: `all-MiniLM-L6-v2`
- **Size**: ~90MB
- **Location**: `models/sentence_transformer/current/`
- **Purpose**: Semantic matching for connector property mapping

## Directory Structure

```
models/
└── sentence_transformer/
    ├── all-MiniLM-L6-v2/     # Downloaded model files
    └── current -> all-MiniLM-L6-v2  # Symlink to current model
```

## Usage

```bash
python src/main.py --config-file connectors.json --output-dir output1/
```
Uses the local model at `models/sentence_transformer/current`

## Troubleshooting

### Model Not Found
If you get an error about the model not being found:
```bash
# Download the model
python download_model.py

# Verify the model works
python -c "from sentence_transformers import SentenceTransformer; model = SentenceTransformer('models/sentence_transformer/current'); print('Model loaded successfully')"
```

### Model Verification
To verify the downloaded model works:
```bash
python -c "
from download_model import verify_model
verify_model()
"
```

## Requirements

- `sentence-transformers`
- `torch`
- `scikit-learn`

Install with:
```bash
pip install sentence-transformers torch scikit-learn
```

## Notes

- The model is downloaded once and stored locally
- No internet connection required after initial download
- The model is used for semantic similarity matching between SM and FM connector properties 