# Model Download Guide

This guide explains how to download and set up the `all-MiniLM-L6-v2` sentence transformer model for the connector migration utility.

## What is all-MiniLM-L6-v2?

The `all-MiniLM-L6-v2` is a lightweight sentence transformer model that:
- Generates 384-dimensional embeddings
- Is optimized for semantic similarity tasks
- Has a small model size (~90MB)
- Provides good performance for text matching applications

## Prerequisites

**No manual installation required!** The scripts will automatically install all necessary dependencies.

## Quick Start

### 1. Download the Model

Run the download script:

```bash
python download_model.py
```

This will automatically:
- **Install sentence-transformers** if missing or corrupted
- **Install all required dependencies** from requirements.txt or core packages
- Download the model from Hugging Face
- Save it to `models/sentence_transformer/all-MiniLM-L6-v2/`
- Create a symlink at `models/sentence_transformer/current`
- Verify the model works correctly


This will also automatically install dependencies if needed.

# Generate embeddings
sentences = ["Hello world", "How are you?"]
embeddings = model.encode(sentences)
```

## Directory Structure

After running the download script, you'll have:

```
models/
└── sentence_transformer/
    ├── all-MiniLM-L6-v2/     # Downloaded model files
    └── current -> all-MiniLM-L6-v2  # Symlink to current model
```

