"""Encoding helpers shared by HTTP clients."""

import base64


def encode_to_base64(input_string: str) -> str:
    """Encode ``input_string`` as UTF-8 base64 (UTF-8 string in, UTF-8 string out)."""
    return base64.b64encode(input_string.encode("utf-8")).decode("utf-8")