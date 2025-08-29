"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

#!/usr/bin/env python3
"""
Script to download and set up the all-MiniLM-L6-v2 sentence transformer model.
This model is used for semantic matching in the connector migration utility.
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Model configuration
MODEL_NAME = "all-MiniLM-L6-v2"
MODELS_DIR = Path("models/sentence_transformer")
CURRENT_MODEL_LINK = MODELS_DIR / "current"
MODEL_DOWNLOAD_DIR = MODELS_DIR / MODEL_NAME

def install_sentence_transformers():
    """Install sentence-transformers package specifically."""
    try:
        logger.info("Installing sentence-transformers package...")
        
        # Try to install the package
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "sentence-transformers"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to install sentence-transformers: {result.stderr}")
            return False
        
        logger.info("sentence-transformers installed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to install sentence-transformers: {e}")
        return False

def install_dependencies():
    """Install required dependencies from requirements.txt."""
    try:
        logger.info("Installing required dependencies...")
        
        # Check if requirements.txt exists
        requirements_file = Path("requirements.txt")
        if not requirements_file.exists():
            logger.warning("requirements.txt not found. Installing core dependencies manually...")
            # Install core dependencies manually if requirements.txt doesn't exist
            core_deps = [
                "sentence-transformers>=2.2.2",
                "torch>=1.9.0",
                "scikit-learn>=1.0.2",
                "numpy>=1.21.0"
            ]
            for dep in core_deps:
                logger.info(f"Installing {dep}...")
                result = subprocess.run([sys.executable, "-m", "pip", "install", dep], 
                                     capture_output=True, text=True)
                if result.returncode != 0:
                    logger.error(f"Failed to install {dep}: {result.stderr}")
                    return False
        else:
            # Install from requirements.txt
            logger.info("Installing dependencies from requirements.txt...")
            result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                                 capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to install dependencies: {result.stderr}")
                return False
        
        logger.info("Dependencies installed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to install dependencies: {e}")
        return False

def check_dependencies():
    """Check if required dependencies are available."""
    try:
        # Try to import sentence-transformers first
        try:
            import sentence_transformers
            logger.info("sentence-transformers is available.")
        except ImportError:
            logger.warning("sentence-transformers not found, attempting to install...")
            if not install_sentence_transformers():
                return False
        
        # Check other dependencies
        try:
            import torch
            import sklearn
            import numpy
            logger.info("All required dependencies are available.")
            return True
        except ImportError as e:
            logger.warning(f"Missing dependency: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking dependencies: {e}")
        return False

def verify_sentence_transformers():
    """Verify that sentence-transformers is working correctly."""
    try:
        import sentence_transformers
        
        # Check if the package has the required components
        if not hasattr(sentence_transformers, 'SentenceTransformer'):
            logger.error("sentence-transformers package is incomplete. Reinstalling...")
            return False
        
        # Try to create a simple model instance
        try:
            # Test with a simple model name to verify the package works
            test_model = sentence_transformers.SentenceTransformer('all-MiniLM-L6-v2')
            logger.info("sentence-transformers verification successful!")
            return True
        except Exception as e:
            logger.error(f"sentence-transformers test failed: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to verify sentence-transformers: {e}")
        return False

def create_directories():
    """Create necessary directories for the model."""
    try:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {MODELS_DIR}")
        return True
    except Exception as e:
        logger.error(f"Failed to create directories: {e}")
        return False

def download_model():
    """Download the all-MiniLM-L6-v2 model."""
    try:
        # Import here after dependencies are installed
        from sentence_transformers import SentenceTransformer
        
        logger.info(f"Downloading model: {MODEL_NAME}")
        logger.info("This may take a few minutes depending on your internet connection...")
        
        # Download the model
        model = SentenceTransformer(MODEL_NAME)
        
        # Save the model to local directory
        model.save(str(MODEL_DOWNLOAD_DIR))
        logger.info(f"Model downloaded and saved to: {MODEL_DOWNLOAD_DIR}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to download model: {e}")
        return False

def create_symlink():
    """Create a symlink from 'current' to the downloaded model."""
    try:
        # Remove existing symlink if it exists
        if CURRENT_MODEL_LINK.exists():
            if CURRENT_MODEL_LINK.is_symlink():
                CURRENT_MODEL_LINK.unlink()
            else:
                shutil.rmtree(CURRENT_MODEL_LINK)
        
        # Create new symlink
        if os.name == 'nt':  # Windows
            # On Windows, create a junction or copy the directory
            if CURRENT_MODEL_LINK.exists():
                shutil.rmtree(CURRENT_MODEL_LINK)
            shutil.copytree(MODEL_DOWNLOAD_DIR, CURRENT_MODEL_LINK)
            logger.info(f"Copied model to: {CURRENT_MODEL_LINK}")
        else:  # Unix-like systems
            CURRENT_MODEL_LINK.symlink_to(MODEL_DOWNLOAD_DIR, target_is_directory=True)
            logger.info(f"Created symlink: {CURRENT_MODEL_LINK} -> {MODEL_DOWNLOAD_DIR}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to create symlink: {e}")
        return False

def verify_model():
    """Verify that the downloaded model works correctly."""
    try:
        logger.info("Verifying downloaded model...")
        
        # Import here after dependencies are installed
        from sentence_transformers import SentenceTransformer
        
        # Try to load the model from the local path
        # First try the symlink, then fall back to direct path
        model_paths = [
            str(CURRENT_MODEL_LINK),
            str(MODEL_DOWNLOAD_DIR)
        ]
        
        model = None
        working_path = None
        
        for model_path in model_paths:
            if os.path.exists(model_path):
                try:
                    logger.info(f"Trying to load model from: {model_path}")
                    model = SentenceTransformer(model_path)
                    working_path = model_path
                    break
                except Exception as e:
                    logger.warning(f"Failed to load from {model_path}: {e}")
                    continue
        
        if model is None:
            raise Exception("Could not load model from any available path")
        
        # Test with a simple sentence
        test_sentence = "This is a test sentence."
        embedding = model.encode(test_sentence)
        
        logger.info(f"Model verification successful!")
        logger.info(f"Embedding dimension: {len(embedding)}")
        logger.info(f"Model loaded from: {working_path}")
        
        return True
    except Exception as e:
        logger.error(f"Model verification failed: {e}")
        return False

def get_model_info():
    """Display information about the downloaded model."""
    try:
        # Check both possible paths
        model_paths = [
            str(CURRENT_MODEL_LINK),
            str(MODEL_DOWNLOAD_DIR)
        ]
        
        working_path = None
        for model_path in model_paths:
            if os.path.exists(model_path):
                working_path = model_path
                break
        
        if working_path is None:
            logger.warning("Model not found. Please run the download first.")
            return False
        
        # Get directory size
        total_size = 0
        file_count = 0
        for dirpath, dirnames, filenames in os.walk(working_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
                file_count += 1
        
        size_mb = total_size / (1024 * 1024)
        
        logger.info("Model Information:")
        logger.info(f"  Name: {MODEL_NAME}")
        logger.info(f"  Location: {working_path}")
        logger.info(f"  Size: {size_mb:.2f} MB")
        logger.info(f"  Files: {file_count}")
        
        # Show symlink status
        if CURRENT_MODEL_LINK.exists():
            if CURRENT_MODEL_LINK.is_symlink():
                logger.info(f"  Symlink: {CURRENT_MODEL_LINK} -> {os.readlink(CURRENT_MODEL_LINK)}")
            else:
                logger.info(f"  Current directory: {CURRENT_MODEL_LINK}")
        else:
            logger.warning("  Note: 'current' symlink not available, using direct model path")
        
        return True
    except Exception as e:
        logger.error(f"Failed to get model info: {e}")
        return False

def main():
    """Main function to download and set up the model."""
    logger.info("Starting model download and setup...")
    
    # Step 1: Check and install sentence-transformers specifically
    logger.info("Step 1: Checking sentence-transformers package...")
    if not verify_sentence_transformers():
        logger.info("Installing sentence-transformers...")
        if not install_sentence_transformers():
            logger.error("Failed to install sentence-transformers. Exiting.")
            return
        
        # Verify again after installation
        if not verify_sentence_transformers():
            logger.error("sentence-transformers still not working after installation. Exiting.")
            return
    
    # Step 2: Check other dependencies
    logger.info("Step 2: Checking other dependencies...")
    if not check_dependencies():
        logger.info("Installing required dependencies...")
        if not install_dependencies():
            logger.error("Failed to install dependencies. Exiting.")
            return
        
        # Check again after installation
        if not check_dependencies():
            logger.error("Dependencies still not available after installation. Exiting.")
            return
    
    # Step 3: Check if model already exists
    if MODEL_DOWNLOAD_DIR.exists():
        logger.info(f"Model already exists at: {MODEL_DOWNLOAD_DIR}")
        
        # Try to create symlink if it doesn't exist
        if not CURRENT_MODEL_LINK.exists():
            logger.info("Creating symlink for existing model...")
            if not create_symlink():
                logger.warning("Failed to create symlink, but model exists. Will use direct path.")
        
        if verify_model():
            logger.info("Model setup completed successfully!")
            get_model_info()
            return
        else:
            logger.error("Model verification failed. Please check the installation.")
            return
    
    # Step 4: Create directories
    logger.info("Step 3: Creating directories...")
    if not create_directories():
        logger.error("Failed to create directories. Exiting.")
        return
    
    # Step 5: Download model
    logger.info("Step 4: Downloading model...")
    if not download_model():
        logger.error("Failed to download model. Exiting.")
        return
    
    # Step 6: Try to create symlink (but don't fail if it doesn't work)
    logger.info("Step 5: Creating symlink...")
    if create_symlink():
        logger.info("Symlink created successfully.")
    else:
        logger.warning("Symlink creation failed, but model was downloaded. Will use direct path.")
    
    # Step 7: Verify model
    logger.info("Step 6: Verifying model...")
    if not verify_model():
        logger.error("Model verification failed. Please check the installation.")
        return
    
    # Step 8: Display model info
    get_model_info()
    
    logger.info("Model setup completed successfully!")
    if CURRENT_MODEL_LINK.exists():
        logger.info(f"You can now use the model from: {CURRENT_MODEL_LINK}")
    else:
        logger.info(f"You can now use the model from: {MODEL_DOWNLOAD_DIR}")
        logger.info("Note: 'current' symlink not available, use the direct model path")

if __name__ == "__main__":
    main() 