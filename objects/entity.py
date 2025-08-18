from dataclasses import dataclass
import json
import time
from typing import Any, Dict, List

from objects.fact import Fact


@dataclass
class Entity:
    qid: str
    label: str
    description: str
    aliases: List[str]
    sitelinks: Dict[str, str]
    facts: List[Fact]
    fetched_at: float= 0.0

    def text_summary(self) -> str:
        return f"{self.label}: {self.description}"
    
    def fact_lines(self, limit: int= None) -> List[str]:
        return [f.display_line for f in self.facts[:limit]]
    
    def important_fact_lines(self, limit: int= None) -> List[str]:
        pref= [f for f in self.facts if ((f.rank or "").lower() == "preferred" and f.valid_context_fact())]
        if pref:
            chosen= pref
        else:
            chosen= [f for f in self.facts if ((f.rank or "").lower() != "deprecated" and f.valid_context_fact())]

        return [f.display_line for f in chosen[:limit]]
    
    def relevant_fact_lines(self, limit: int= None) -> List[str]:
        chosen= [f for f in self.facts if ((f.rank or "").lower() != "deprecated" and f.valid_context_fact())]
        return [f.display_line for f in chosen[:limit]]

    def vector_ready_str(self, facts_limit: int= None) -> str:
        return self.text_summary() + ". " + "; ".join(self.important_fact_lines(facts_limit))
    
    def query_context_str(self, facts_limit: int= None) -> str:
        return self.text_summary() + ". " + "; ".join(self.relevant_fact_lines(facts_limit))

    def is_stale(self, max_age_days: int= 365) -> bool:
        return (time.time() - self.fetched_at) > (max_age_days * 86400)
    
    def to_row(self) -> Dict[str, str]:
        return {
            "qid": self.qid,
            "label": self.label,
            "description": self.description,
            "aliases_json": json.dumps(self.aliases, ensure_ascii=False),
            "sitelinks_json": json.dumps(self.sitelinks, ensure_ascii=False),
            "fetched_at": self.fetched_at
        }