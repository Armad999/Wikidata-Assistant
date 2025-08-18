import calendar
from dataclasses import dataclass, field
import json
import time
from typing import Any, Dict, List, Optional

@dataclass
class Fact:
    guid: str 
    pid: str
    subject_qid: str
    property_label: str
    value_type: str
    value_qid: Optional[str]= None
    value_label: Optional[str]= None
    value_literal: Optional[str]= None
    rank: Optional[str]= None
    qualifiers: Optional[List[Dict[str, Any]]]= None
    references: Optional[List[Dict[str, Any]]]= None
    fetched_at: float= 0.0
    display_value: str= field(init= False)
    display_line: str= field(init= False)

    def __post_init__(self):
        self.finalize_display()

    def finalize_display(self) -> None:
        if self.value_type == "wikibase-entityid":
            val = self.value_label or self.value_qid or ""
        elif self.value_type == "time":
            val = self.format_wikidata_time(self.value_literal) or (self.value_literal or "")
        else:
            val = self.value_literal or ""
        self.display_value = val
        self.display_line = f"{self.property_label}: {val}".strip(": ")

    def is_stale(self, max_age_days: int= 365) -> bool:
        return (time.time() - self.fetched_at) > (max_age_days * 86400)
    
    def valid_context_fact(self) -> bool:
        vt = (self.value_type or "")
        if any(v in vt for v in ("monolingualtext", "quantity")):
            return False
        pl = (self.property_label or "")
        if any(p in pl for p in ("image", "logo", "flag", "signature", " ID", "URL", "article", "ISNI", self.pid)):
            return False
        return True

    def to_row(self) -> Dict[str, Any]:
        return {
            "guid": self.guid,
            "pid": self.pid,
            "subject_qid": self.subject_qid,
            "property_label": self.property_label,
            "value_type": self.value_type,
            "value_qid": self.value_qid,
            "value_label": self.value_label,
            "value_literal": self.value_literal,
            "rank": self.rank,
            "qualifiers_json": json.dumps(self.qualifiers, ensure_ascii=False),
            "references_json": json.dumps(self.references, ensure_ascii=False),
            "fetched_at": self.fetched_at,
            "display_value": self.display_value,
            "display_line": self.display_line,
        }


    def format_wikidata_time(self, value_literal: Optional[str]) -> Optional[str]:
        if not value_literal:
            return None
        try:
            v = json.loads(value_literal) if isinstance(value_literal, str) else value_literal
            if not isinstance(v, dict):
                return None
            t = v.get("time")
            prec = int(v.get("precision", 9))
            if not t:
                return None

            sign = -1 if t.startswith("-") else 1
            ts = t[1:] if t[0] in "+-" else t
            y_str, m_str, d_str = ts.split("T")[0].split("-")
            year = int(y_str)
            year = -year if sign < 0 else year

            def era(y: int) -> str:
                return " BCE" if y <= 0 else ""

            if prec >= 11:
                month_name = calendar.month_name[int(m_str)] if m_str not in (None, "", "00") else ""
                day = int(d_str) if d_str not in (None, "", "00") else None
                if month_name and day:
                    return f"{day} {month_name} {abs(year)}{era(year)}"
            if prec == 10:
                month_name = calendar.month_name[int(m_str)] if m_str not in (None, "", "00") else ""
                if month_name:
                    return f"{month_name} {abs(year)}{era(year)}"
            if prec == 9:
                return f"{abs(year)}{era(year)}"
            if prec == 8:
                decade = (abs(year) // 10) * 10
                return f"{decade}s{era(year)}"
            if prec == 7:
                abs_year = abs(year)
                cent = (abs_year - 1) // 100 + 1
                suf = "th" if 10 <= cent % 100 <= 20 else {1:"st",2:"nd",3:"rd"}.get(cent % 10, "th")
                return f"{cent}{suf} century{era(year)}"

            return f"{abs(year)}{era(year)}"
        except Exception:
            return None