from typing import List

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams


class QdrantVectorDB:
    def __init__(
        self,
        url: str = "http://localhost:6333",
        collection_name: str = "docs",
        dim=3072,
    ) -> None:
        self.client = QdrantClient(url=url, timeout=30)
        self.collection_name = collection_name
        if not self.client.collection_exists(collection_name=self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )

    def upsert(self, ids: List, vecrots: List, payloads: List) -> None:
        points = [
            PointStruct(id=id, vector=vector, payload=payload)
            for id, vector, payload in zip(ids, vecrots, payloads)
        ]
        self.client.upsert(collection_name=self.collection_name, points=points)

    def search(self, query_vector: List[float], top_k: int = 5) -> dict:
        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            with_payload=True,
            limit=top_k,
        )
        contexts = []
        sources = set()

        for result in results:
            payload = getattr(result, "payload", {})
            text = payload.get("text", "")
            source = payload.get("source", "")

            if text:
                contexts.append(text)
                sources.add(source)

        return {"contexts": contexts, "sources": list(sources)}
