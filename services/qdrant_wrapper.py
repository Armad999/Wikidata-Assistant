from typing import List
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct

from objects.entity import Entity
from services.embedder import Embedder
from services.sqlite_wrapper import SqliteWrapper

DEFAULT_COLLECTION= "wikidata_vectors"
DEFAULT_EMBED_SIZE= 384

class QdrantWrapper:

    def __init__(self, embedder: Embedder, sqlite: SqliteWrapper, host: str= "localhost", port: int= 6333):
        self.embedder= embedder
        self.sqlite= sqlite
        self.client= QdrantClient(host, port= port)
        self.ensure_collection()

    def ensure_collection(self, collection: str= DEFAULT_COLLECTION, size: int= DEFAULT_EMBED_SIZE) -> None:
        if not self.client.collection_exists(collection):
            self.client.create_collection(
                collection,
                vectors_config= VectorParams(size= size, distance= Distance.COSINE)
            )

    def point_id_from_qid(self, qid: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"wikidata:{qid}"))

    def upsert_entity(self, entity: Entity, collection: str= DEFAULT_COLLECTION) -> None:
        point_id = self.point_id_from_qid(entity.qid)
        text= entity.vector_ready_str()
        vector= self.embedder.embed_text(text)
        point= PointStruct(
            id=point_id,
            vector=vector,
            payload={
                "qid": entity.qid,
                "label": entity.label,
                "description": entity.description,
            },
        )
        self.client.upsert(collection_name=collection, points=[point])

    def delete_entity(self, qid: str, collection: str= DEFAULT_COLLECTION) -> None:
        point_id = self.point_id_from_qid(qid)
        self.client.delete(collection_name= collection, points_selector= [point_id])

    def search_entities(self, prompt: str, min_score: float= 0.80, limit: int= 3, collection: str= DEFAULT_COLLECTION) -> List[Entity]:
        vector= self.embedder.embed_text(prompt)
        hits= self.client.search(collection_name= collection, query_vector= vector, limit= limit)
        entities: List[Entity]= []
        for h in hits:
            if h.score >= min_score:
                qid = (h.payload or {}).get("qid")
                if qid:
                    ent = self.sqlite.get_entity(qid)
                    if ent:
                        entities.append(ent)
        return entities