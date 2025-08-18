from data.data_fetcher import DataFetcher
from llm.llama_model import LlamaModel
from services.embedder import Embedder
from services.qdrant_wrapper import QdrantWrapper
from services.sqlite_wrapper import SqliteWrapper


def main():
    embedder= Embedder()
    sqlite= SqliteWrapper()
    qdrant= QdrantWrapper(embedder= embedder, sqlite= sqlite)
    fetcher= DataFetcher(sqlite= sqlite, qdrant= qdrant, embedder= embedder)
    llm= LlamaModel(fetcher= fetcher, sqlite= sqlite, qdrant= qdrant)

    print("Ready! Type only the entity you would like to know about.")
    print("Type 'quit' to stop.")

    while True:
        try:
            query= input("\n> ").strip()
            if query.lower() in "quit":
                print("Goodbye!")
                break
            if not query:
                continue
            
            answer= llm.rag_ask(query)
            print(answer)
        except KeyboardInterrupt:
            print("Goodbye!")
            break
        except Exception as e:
            print(f"ERROR: {e}")   


if __name__ == "__main__":
    main()