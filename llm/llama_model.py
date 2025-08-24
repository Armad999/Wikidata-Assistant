import os
from llama_cpp import Llama

from data.data_fetcher import DataFetcher
from services.qdrant_wrapper import QdrantWrapper
from services.sqlite_wrapper import SqliteWrapper

RAG_MODEL= os.getenv("RAG_MODEL")

class LlamaModel:
    def __init__(self, fetcher : DataFetcher, sqlite : SqliteWrapper, qdrant : QdrantWrapper, path= RAG_MODEL):
        self.fetcher = fetcher
        self.sqlite = sqlite
        self.qdrant = qdrant
        self.llm = Llama(model_path = path, n_ctx= 4096, n_gpu_layers= -1, verbose= False)


    def rag_ask(self, query : str, limit = 3, min_score = 0.80):
        contexts = self.qdrant.search_entities(prompt= query, min_score= min_score, limit= limit)
        relevant = [c for c in contexts if c["score"] >= min_score]
        entities = []

        if not relevant:
            ids = [i["QID"] for i in self.fetcher.search_for_qid(query, limit) if i["similarity_score"] >= min_score]
            entities = self.fetcher.get_or_fetch_wikidata_entities_by_qids(ids)
        else:
            entities = [self.sqlite.get_entity(r["qid"]) for r in relevant]

        if not entities:
            return "I'm sorry, but there is not enough context for me to answer that."


        context_str = ""
        for e in entities:
            context_str += e.query_context_str() + "\n"
        prompt = f'''<|system|>
                    You answer ONLY using the FACTS provided. If a detail is not in FACTS, say you don't have it. Keep it concise: 2–4 sentences, ≤80 words. End with a period.</s>
                    <|user|>
                    {context_str}
                    {query}
                    </s>
                    <|assistant|>'''

        result = self.llm(
            prompt=prompt,
            max_tokens=300,
            temperature=1.0,
            top_p=0.95,
            top_k=40,
            repeat_penalty=1.1,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            stop=["</s>", "<|user|>", "<|system|>"]
        )
        return result["choices"][0]["text"]