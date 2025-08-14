from typing import Dict, Any, List, Optional, NamedTuple
import numpy as np
from dataclasses import dataclass
from rapidfuzz import fuzz
import logging

logger = logging.getLogger(__name__)
# Import third-party libraries, making them optional
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    sentence_transformers_available = True
    try:
        # Using a smaller, faster model suitable for description similarity
        semantic_model = SentenceTransformer('all-MiniLM-L6-v2')
    except Exception as e:
        logger.warning(f"Failed to load SentenceTransformer model 'all-MiniLM-L6-v2'. Error: {e}")
        sentence_transformers_available = False
except ImportError:
    logger.warning("sentence-transformers or scikit-learn not found. Install with: python download_model.py")
    sentence_transformers_available = False


def initialize_semantic_model(model_path: str = None):
    """Initialize the semantic model from a local path only"""
    global _semantic_model, sentence_transformers_available
    
    if sentence_transformers_available:
        return True
        
    try:
        if model_path:
            # Load from specified path
            logger.info(f"Loading sentence transformer model from: {model_path}")
            _semantic_model = SentenceTransformer(model_path)
        else:
            # Load from local models directory only
            from pathlib import Path
            local_model_path = Path('models/sentence_transformer/all-MiniLM-L6-v2')
            if local_model_path.exists():
                logger.info(f"Loading local sentence transformer model from: {local_model_path}")
                _semantic_model = SentenceTransformer(str(local_model_path))
            else:
                logger.error("Local model not found at models/sentence_transformer/all-MiniLM-L6-v2")
                logger.error("Please run download_model.py first to download the model")
                sentence_transformers_available = False
                return False
        
        logger.info("Sentence transformer model loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to load sentence transformer model. Error: {e}")
        sentence_transformers_available = False
        return False

# Cache for embeddings to avoid re-computation
_fm_embeddings_cache: Dict[str, np.ndarray] = {}
_sm_embeddings_cache: Dict[str, np.ndarray] = {}

@dataclass
class Property:
    name: str
    description: str
    type: str = ""
    section: str = ""
    value: Optional[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class MatchResult:
    matched_fm_property: Any
    similarity_score: float
    match_type: str  # 'exact', 'semantic', or 'string'

def _get_embedding(text: str, cache: Dict[str, np.ndarray]) -> Optional[np.ndarray]:
    """Gets or computes sentence embedding for a given text."""
    global _semantic_model
    
    if not sentence_transformers_available or not text or _semantic_model is None:
        return None
    if text in cache:
        return cache[text]
    try:
        # Encode expects a list
        embedding = _semantic_model.encode([text], convert_to_numpy=True)[0]
        cache[text] = embedding
        return embedding
    except Exception as e:
        logger.error(f"Error generating embedding for text: '{text[:50]}...'. Error: {e}", exc_info=False)
        return None

def calculate_similarity(sm_property: Dict[str, Any], fm_property: Property) -> float:
    """Calculate similarity between SM and FM properties using both semantic and string matching."""
    # Create text for semantic matching
    sm_text = f"{sm_property['name']} {sm_property.get('description', '')} {sm_property.get('section', '')}"
    fm_text = f"{fm_property.name} {fm_property.description} {fm_property.section}"
    
    # Get embeddings
    sm_embedding = _get_embedding(sm_text, _sm_embeddings_cache)
    fm_embedding = _get_embedding(fm_text, _fm_embeddings_cache)
    
    if sm_embedding is not None and fm_embedding is not None:
        # Calculate semantic similarity
        semantic_score = np.dot(sm_embedding, fm_embedding) / (
            np.linalg.norm(sm_embedding) * np.linalg.norm(fm_embedding)
        )
    else:
        semantic_score = 0.0
    
    # Calculate string similarity
    string_score = fuzz.ratio(sm_property['name'].lower(), fm_property.name.lower()) / 100.0
    
    # Combined score (weighted average)
    return 0.7 * semantic_score + 0.3 * string_score

class SemanticMatcher:
    def __init__(self):
        self.fm_embeddings = {}
        self.fm_properties = {}
        
        # # Initialize the semantic model
        if not initialize_semantic_model():
            logger.warning("Failed to initialize semantic model. Semantic matching will be disabled.")
        else:
            logger.info("Semantic model initialized successfully.")

    def preload_fm_embeddings(self, fm_properties: Dict[str, Any]):
        """Preload embeddings for FM properties"""
        self.fm_properties = fm_properties
        for prop_name, prop_info in fm_properties.items():
            # Create text for embedding
            text = f"{prop_name} {prop_info.get('description', '')} {prop_info.get('section', '')}"
            # Generate embedding
            embedding = _get_embedding(text, _fm_embeddings_cache)
            if embedding is not None:
                self.fm_embeddings[prop_name] = embedding

    def find_best_match(self, sm_property: Dict[str, Any], fm_properties: Dict[str, Any], semantic_threshold: float = 0.7) -> Optional[MatchResult]:
        """
        Find the best matching FM property for a given SM property using the following strategy:
        1. First check for exact name matches
        2. If no exact match found, perform semantic matching with stricter validation
        """
        # Step 1: Check for exact name matches
        if sm_property['name'] in fm_properties:
            return MatchResult(
                matched_fm_property=fm_properties[sm_property['name']],
                similarity_score=1.0,
                match_type='exact'
            )
        
        # Step 2: If no exact match found, perform semantic matching
        best_match = None
        best_score = 0.0
        
        for fm_prop_name, fm_prop_info in fm_properties.items():
            # Skip properties that should only be matched exactly
            if fm_prop_info.get('metadata', {}).get('direct_match', False):
                continue
            
                
            # Calculate similarity
            similarity = calculate_similarity(sm_property, Property(
                name=fm_prop_name,
                description=fm_prop_info.get('description', ''),
                section=fm_prop_info.get('section', ''),
                metadata=fm_prop_info.get('metadata', {})
            ))
            
            if similarity > best_score:
                best_score = similarity
                best_match = (fm_prop_name, fm_prop_info)
        
        if best_match and best_score >= semantic_threshold:
            fm_prop_name, fm_prop_info = best_match
            logger.debug(f"Semantic match found: '{sm_property['name']}' -> '{fm_prop_name}' (score: {best_score:.3f})")
            return MatchResult(
                matched_fm_property=fm_prop_info,
                similarity_score=best_score,
                match_type='semantic'
            )
        
        if best_match:
            logger.debug(f"Semantic match rejected: '{sm_property['name']}' -> '{best_match[0]}' (score: {best_score:.3f}, threshold: {semantic_threshold})")
        
        return None 