import os
from sentence_transformers import SentenceTransformer

EMBEDDER= os.getenv("EMBEDDER")

class Embedder:

    def __init__(self):
        self.model= SentenceTransformer(EMBEDDER)

    def embed_text(self, text):
        return self.model.encode([text])[0]