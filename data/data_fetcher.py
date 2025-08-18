import json
import time
from typing import Any, Dict, List
import requests

from objects.entity import Entity
from objects.fact import Fact
from services.qdrant_wrapper import QdrantWrapper
from services.sqlite_wrapper import SqliteWrapper
from services.embedder import Embedder

WIKIDATA_API= "https://www.wikidata.org/w/api.php"

class DataFetcher:

    def __init__(self, sqlite : SqliteWrapper, qdrant : QdrantWrapper, embedder : Embedder):
        self.sqlite= sqlite
        self.qdrant= qdrant
        self.embedder= embedder

    def fetch_property_labels(self, ids: List[str]) -> Dict[str, str]:
        labels: Dict[str, str] = {}
        if not ids:
            return labels

        seen = set()
        uniq_ids = [i for i in ids if i and not (i in seen or seen.add(i))]

        CHUNK = 50
        for i in range(0, len(uniq_ids), CHUNK):
            batch = uniq_ids[i:i + CHUNK]
            params = {
                "action": "wbgetentities",
                "ids": "|".join(batch),
                "languages": "en",
                "props": "labels",
                "format": "json",
            }
            resp= requests.get(WIKIDATA_API, params=params)
            entities = resp.json().get("entities", {})

            for wid in batch:
                ent = entities.get(wid, {}) or {}
                labels[wid] = ent.get("labels", {}).get("en", {}).get("value", wid)

        return labels

    def fetch_wikidata_entities_by_qids(self, qids: List[str]) -> List[Entity]:
        if not qids:
            return []

        params = {
            "action": "wbgetentities",
            "ids": "|".join(qids),
            "languages": "en",
            "props": "labels|descriptions|aliases|sitelinks|claims",
            "format": "json",
        }
        resp = requests.get(WIKIDATA_API, params=params)
        data = resp.json()
        entities_json: Dict[str, Any] = data.get("entities", {})

        pid_set = set()
        value_qids = set()
        for e in entities_json.values():
            for pid, stmts in e.get("claims", {}).items():
                pid_set.add(pid)
                for st in stmts:
                    dv = (st.get("mainsnak", {}).get("datavalue") or {})
                    if dv.get("type") == "wikibase-entityid":
                        v = dv.get("value", {})
                        q = v.get("id")
                        if q:
                            value_qids.add(q)

        pid_labels = self.fetch_property_labels(list(pid_set)) if pid_set else {}
        value_labels = self.fetch_property_labels(list(value_qids)) if value_qids else {}
        results: List[Entity] = []
        now_ts = time.time()

        for qid, e in entities_json.items():
            label = e.get("labels", {}).get("en", {}).get("value", "")
            desc = e.get("descriptions", {}).get("en", {}).get("value", "")
            aliases = [a.get("value", "") for a in e.get("aliases", {}).get("en", [])]
            sitelinks = {k: v.get("title", "") for k, v in e.get("sitelinks", {}).items()}

            facts: List[Fact] = []
            for pid, statements in e.get("claims", {}).items():
                prop_label = pid_labels.get(pid, pid)
                for st in statements:
                    guid = st.get("id") or f"{qid}${pid}${hash((pid, json.dumps(st, ensure_ascii=False, sort_keys=True), st.get('rank')))}"
                    mainsnak = st.get("mainsnak", {})
                    datavalue = mainsnak.get("datavalue", {})
                    value_type = datavalue.get("type", "")

                    value_qid = None
                    value_literal = None
                    val = datavalue.get("value")
                    if value_type == "wikibase-entityid" and isinstance(val, dict):
                        value_qid = val.get("id")
                    elif val is not None:
                        value_literal = json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)

                    facts.append(
                        Fact(
                            guid=guid,
                            pid=pid,
                            subject_qid=qid,
                            property_label=prop_label,
                            value_type=value_type,
                            value_qid=value_qid,
                            value_label=value_labels.get(value_qid) if value_qid else None,
                            value_literal=value_literal,
                            rank=st.get("rank"),
                            qualifiers=st.get("qualifiers"),
                            references=st.get("references"),
                            fetched_at=now_ts,
                        )
                    )

            results.append(
                Entity(
                    qid=qid,
                    label=label,
                    description=desc,
                    aliases=aliases,
                    sitelinks=sitelinks,
                    facts=facts,
                    fetched_at=now_ts,
                )
            )

        return results


    def get_or_fetch_wikidata_entities_by_qids(self, qids: list):
        entities: List[Entity]= []
        missing_qids: List[str]= []

        for qid in qids:
            entity= self.sqlite.get_entity(qid)
            if entity and (not entity.is_stale()):
                entities.append(entity)
            else:
                missing_qids.append(qid)

        if missing_qids:
            fetched_entities= self.fetch_wikidata_entities_by_qids(missing_qids)
            for fe in fetched_entities:
                self.sqlite.upsert_entity(fe)
                self.qdrant.upsert_entity(fe)
                entities.append(fe)

        return entities
        
    def search_for_qid(self, entity: str, limit= 3):
        url= WIKIDATA_API
        params= {
            "action": "wbsearchentities",
            "search": entity,
            "language": "en",
            "format": "json",
            "limit": limit
        }

        response= requests.get(url, params= params)
        data= response.json()
        return [result["id"] for result in data.get("search", [])]