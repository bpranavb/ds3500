import re

def normalize_title(title):
    if not isinstance(title, str):
        return ""
    # Convert to lowercase
    title = title.lower()
    # Remove special characters and punctuation
    title = re.sub(r'[^a-z0-9\s]', '', title)
    # Strip extra whitespace
    return title.strip()