import unicodedata
from typing import List, Dict
import re

def sinitize_text(text: str) -> str:
    """
    Remove special characters from the text.
    """
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    # Remove special characters using regex
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '_', text)  # Replace spaces with underscores
    text = re.sub(r'_+', '_', text)  # Replace multiple underscores with a single one
    text = text.strip("_").strip()  # Remove leading/trailing underscores and spaces
    
    if re.match(r'^\d', text):
        text = f"col_{text}"

    return text