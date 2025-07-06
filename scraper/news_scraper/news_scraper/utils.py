import hashlib
import uuid


def generate_deterministic_uuid(content: str, namespace: str) -> str:
    """
    Generates a deterministic UUID based on the content and a namespace.

    Args:
        content (str): The content to generate the UUID from.
        namespace (str): The namespace to use for generating the UUID.
    Returns:
        str: A deterministic UUID.
    """
    hash_input = f"{namespace}:{content}".encode("utf-8")
    hash_digest = hashlib.sha256(hash_input).digest()
    return str(uuid.UUID(bytes=hash_digest[:16], version=4))


def generate_website_uuid(website_url: str) -> str:
    """
    Generate deterministic UUID for a website based on its URL.
    """
    return generate_deterministic_uuid(website_url, "website")


def generate_article_uuid(article_url: str, website_url: str) -> str:
    """
    Generate deterministic UUID for an article based on its URL and website.
    """
    content = f"{website_url}|{article_url}"
    return generate_deterministic_uuid(content, "article")


def generate_word_uuid(word_text: str) -> str:
    """
    Generate deterministic UUID for a word based on its text.
    """
    return generate_deterministic_uuid(word_text.lower().strip(), "word")


def generate_occurrence_uuid(word_text: str, article_url: str, website_url: str) -> str:
    """
    Generate deterministic UUID for an occurrence based on word, article, and website.
    """
    content = f"{word_text.lower().strip()}|{website_url}|{article_url}"
    return generate_deterministic_uuid(content, "occurrence")
