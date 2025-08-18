import json
from typing import List, Optional
import sqlite_utils
from sqlite_utils.db import NotFoundError

from objects.entity import Entity
from objects.fact import Fact

class SqliteWrapper:

    def __init__(self, path: str= "wikidata_cache.sqlite"):
        self.db= sqlite_utils.Database(path)

        if "entities" not in self.db.table_names():
            self.db["entities"].create(
                {
                    "qid": str,
                    "label": str,
                    "description": str,
                    "aliases_json": str,
                    "sitelinks_json": str,
                    "fetched_at": float,
                },
                pk="qid",
                if_not_exists=True,
            )

        if "facts" not in self.db.table_names():
            self.db["facts"].create(
                {
                    "guid": str,
                    "pid": str,
                    "subject_qid": str,
                    "property_label": str,
                    "value_type": str,
                    "value_qid": str,
                    "value_label": str,
                    "value_literal": str,
                    "rank": str,
                    "qualifiers_json": str,
                    "references_json": str,
                    "fetched_at": float,
                    "display_value": str,
                    "display_line": str,
                },
                pk="guid",
                foreign_keys=[("subject_qid", "entities", "qid")],
                if_not_exists=True,
            )
        
        self.db["facts"].create_index(["subject_qid"], if_not_exists=True)
        self.db["entities"].create_index(["qid"], if_not_exists=True)
        self.db["facts"].create_index(["guid"], if_not_exists=True)

    def row_to_fact(self, fr: dict) -> Fact:
        return Fact(
            guid= fr["guid"],
            subject_qid= fr["subject_qid"],
            pid= fr["pid"],
            property_label= fr["property_label"],
            value_type= fr["value_type"],
            value_qid= fr.get("value_qid"),
            value_label= fr.get("value_label"),
            value_literal= fr.get("value_literal"),
            rank= fr.get("rank"),
            qualifiers= json.loads(fr.get("qualifiers_json") or "null"),
            references= json.loads(fr.get("references_json") or "null"),
            fetched_at= fr["fetched_at"]
            )
    
    def row_to_entity(self, er: dict) -> Entity:
        return Entity(
            qid= er["qid"],
            label= er["label"],
            description= er["description"],
            aliases= json.loads(er["aliases_json"] or "null"),
            sitelinks= json.loads(er["sitelinks_json"] or "null"),
            facts= self.get_facts_by_subject(er["qid"]),
            fetched_at= er["fetched_at"]
        )
    
    def get_fact(self, guid: str, table: str= "facts") -> Optional[Fact]:
        try:
            fr= self.db[table].get(guid)
        except NotFoundError:
            return None
        return self.row_to_fact(fr)

    def get_facts_by_subject(self, subject_qid: str, limit: Optional[int]= None, table: str= "facts") -> List[Fact]:
        where= "subject_qid= :qid"
        params= {"qid": subject_qid}
        frs= self.db[table].rows_where(where,params)
        result= []
        for i, fr in enumerate(frs):
            if limit is not None and i >= limit:
                break
            result.append(self.row_to_fact(fr))
        return result

    def get_entity(self, qid: str, table: str= "entities") -> Optional[Entity]:
        try:
            er= self.db[table].get(qid)
        except NotFoundError:
            return None
        return self.row_to_entity(er)
    
    def upsert_fact(self, fact: Fact, table: str= "facts") -> None:
        self.db[table].upsert(fact.to_row(), pk="guid")
        
    def upsert_entity(self, entity : Entity, table: str= "entities") -> None:
        for fact in entity.facts:
            self.upsert_fact(fact)
        self.db[table].upsert(entity.to_row(), pk= "qid")

    def delete_fact(self, guid : str, table : str= "facts") -> None:
        self.db[table].delete(guid)

    def delete_entity(self, qid: str, table: str= "entities") -> None:
        facts= self.get_facts_by_subject(qid)
        for fact in facts:
            self.delete_fact(fact.guid)
        self.db[table].delete(qid)