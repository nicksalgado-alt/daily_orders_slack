"""
Map Extensiv IM order items to Nok brand names (Ozlo Sleep, Bissell CA, Oliso, BBI,
Hatch, Ember, NZXT, FOREO, Dupray).

Three layers of rules run in this order for each SKU:

    1. sku_overrides   — exact SKU -> brand (for one-offs that rules can't catch)
    2. description_rules — regex match against the Description field on each line item
    3. sku_rules       — regex match against the SKU string itself (fallback for
                         channels like b2b that don't populate Description)

The rules live in `references/brand_map.json` as plain data so the mapping can be
edited by hand without touching code.

The `BrandMapper.audit(orders)` helper walks a full pulled-orders list and returns
both the per-SKU mapping and an `unmapped` list — this is what the pull_orders.py
guardrail uses to halt an export if any SKU is not covered by the current rules.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_BRAND_MAP_PATH = (
    Path(__file__).resolve().parent.parent / "references" / "brand_map.json"
)


@dataclass
class BrandMatch:
    """The result of mapping a single SKU to a brand."""
    sku: str
    brand: Optional[str]           # None if no rule matched
    source: str                    # "override" | "description" | "sku" | "unmapped"
    reason: str                    # human-readable note on which rule fired
    confidence: str                # "high" | "inferred" | "unmapped"


class BrandMapperError(Exception):
    """Raised when the brand_map.json file is malformed."""


class BrandMapper:
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = Path(config_path) if config_path else DEFAULT_BRAND_MAP_PATH
        if not self.config_path.exists():
            raise BrandMapperError(f"brand_map.json not found at {self.config_path}")
        try:
            cfg = json.loads(self.config_path.read_text())
        except json.JSONDecodeError as e:
            raise BrandMapperError(f"brand_map.json is not valid JSON: {e}") from e

        self.known_brands: set[str] = set(cfg.get("brands") or [])
        self.sku_overrides: Dict[str, str] = dict(cfg.get("sku_overrides") or {})
        self.description_rules = [self._compile(r) for r in cfg.get("description_rules") or []]
        self.sku_rules         = [self._compile(r) for r in cfg.get("sku_rules") or []]

        # Validate — every rule should point to a declared brand
        for rules_name, rules in (("description_rules", self.description_rules),
                                  ("sku_rules", self.sku_rules)):
            for r in rules:
                if self.known_brands and r["brand"] not in self.known_brands:
                    raise BrandMapperError(
                        f"{rules_name} references undeclared brand {r['brand']!r}. "
                        f"Add it to the 'brands' list in {self.config_path.name} "
                        f"or fix the rule."
                    )
        for sku, brand in self.sku_overrides.items():
            if self.known_brands and brand not in self.known_brands:
                raise BrandMapperError(
                    f"sku_overrides maps {sku!r} to undeclared brand {brand!r}."
                )

        # Cache of sku -> BrandMatch so the same SKU isn't re-evaluated each line
        self._cache: Dict[Tuple[str, Tuple[str, ...]], BrandMatch] = {}

    @staticmethod
    def _compile(rule: Dict[str, Any]) -> Dict[str, Any]:
        flags = 0
        for ch in (rule.get("flags") or ""):
            flags |= {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL}.get(ch, 0)
        return {
            "brand":   rule["brand"],
            "pattern": re.compile(rule["pattern"], flags),
            "note":    rule.get("note") or "",
        }

    # ------------------------------------------------------------------
    # Core mapping
    # ------------------------------------------------------------------
    def match(self, sku: str, descriptions: Optional[Iterable[str]] = None) -> BrandMatch:
        """Return a BrandMatch for a single SKU given zero or more item descriptions.

        Descriptions strengthen the match. If nothing matches, returns a BrandMatch
        with brand=None so the caller can decide whether to halt or warn.
        """
        sku = (sku or "").strip()
        desc_tuple = tuple(sorted({d.strip() for d in (descriptions or []) if d and d.strip()}))
        cache_key = (sku, desc_tuple)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Explicit override
        if sku in self.sku_overrides:
            m = BrandMatch(sku, self.sku_overrides[sku], "override",
                           "explicit sku_overrides entry", "high")
            self._cache[cache_key] = m
            return m

        # 2. Description rules — most reliable because the marketplace gave us
        # the actual product name
        for d in desc_tuple:
            for r in self.description_rules:
                if r["pattern"].search(d):
                    m = BrandMatch(sku, r["brand"], "description",
                                   r["note"] or f"description matched /{r['pattern'].pattern}/",
                                   "high")
                    self._cache[cache_key] = m
                    return m

        # 3. SKU string rules — fallback for channels without descriptions
        for r in self.sku_rules:
            if r["pattern"].search(sku):
                m = BrandMatch(sku, r["brand"], "sku",
                               r["note"] or f"sku matched /{r['pattern'].pattern}/",
                               "inferred")
                self._cache[cache_key] = m
                return m

        m = BrandMatch(sku, None, "unmapped",
                       "no brand_map.json rule matched", "unmapped")
        self._cache[cache_key] = m
        return m

    # ------------------------------------------------------------------
    # Bulk helpers
    # ------------------------------------------------------------------
    def enrich_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Mutate an order dict in place:
          - Each item gets a `_brand` field
          - The order gets `_brand` (the dominant brand by units) and
            `_brands_in_order` (comma-separated for mixed-brand orders).
        Returns the same order dict for convenience.
        """
        items = order.get("items") or []
        brand_units: Dict[str, int] = {}
        for it in items:
            sku = (it.get("item") or "").strip()
            desc = it.get("Description") or ""
            m = self.match(sku, [desc] if desc else [])
            it["_brand"] = m.brand  # may be None
            try:
                q = int(it.get("quantity") or 0)
            except (TypeError, ValueError):
                q = 0
            if m.brand:
                brand_units[m.brand] = brand_units.get(m.brand, 0) + q
        if brand_units:
            # Dominant brand = most units; ties broken alphabetically for determinism
            order["_brand"] = sorted(brand_units.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
            order["_brands_in_order"] = ", ".join(sorted(brand_units))
        else:
            order["_brand"] = None
            order["_brands_in_order"] = ""
        return order

    def audit(self, orders: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        """Walk a list of orders and summarize brand coverage.

        Returns:
            {
              "total_skus":    int,
              "mapped_skus":   int,
              "unmapped_skus": int,
              "by_brand":      {brand: {"skus": n, "units": n, "revenue": float}},
              "unmapped":      [
                {"sku": str, "units": int, "storefronts": [..],
                 "sample_description": str | ""},
                ...
              ],
            }
        """
        per_sku: Dict[str, Dict[str, Any]] = {}
        for o in orders:
            src = o.get("order_source") or "unknown"
            for it in (o.get("items") or []):
                sku = (it.get("item") or "").strip()
                if not sku:
                    continue
                desc = (it.get("Description") or "").strip()
                try:
                    qty = int(it.get("quantity") or 0)
                except (TypeError, ValueError):
                    qty = 0
                try:
                    price = float(it.get("price") or 0)
                except (TypeError, ValueError):
                    price = 0
                agg = per_sku.setdefault(sku, {
                    "sku": sku,
                    "units": 0,
                    "revenue": 0.0,
                    "storefronts": set(),
                    "descriptions": set(),
                })
                agg["units"] += qty
                agg["revenue"] += qty * price
                agg["storefronts"].add(src)
                if desc:
                    agg["descriptions"].add(desc)

        by_brand: Dict[str, Dict[str, Any]] = {}
        unmapped: List[Dict[str, Any]] = []
        for sku, agg in per_sku.items():
            m = self.match(sku, list(agg["descriptions"]))
            if m.brand:
                b = by_brand.setdefault(m.brand, {"skus": 0, "units": 0, "revenue": 0.0})
                b["skus"] += 1
                b["units"] += agg["units"]
                b["revenue"] += agg["revenue"]
            else:
                sample = next(iter(agg["descriptions"]), "")
                unmapped.append({
                    "sku": sku,
                    "units": agg["units"],
                    "revenue": round(agg["revenue"], 2),
                    "storefronts": sorted(agg["storefronts"]),
                    "sample_description": sample,
                })
        unmapped.sort(key=lambda r: (-r["units"], r["sku"]))

        return {
            "total_skus":    len(per_sku),
            "mapped_skus":   len(per_sku) - len(unmapped),
            "unmapped_skus": len(unmapped),
            "by_brand":      by_brand,
            "unmapped":      unmapped,
        }


# ----------------------------------------------------------------------
# CLI — lets Nick sanity-check a JSONL dump without touching Python:
#     python -m brand_mapper /path/to/orders.jsonl
# prints the brand-coverage audit summary.
# ----------------------------------------------------------------------
def _cli() -> int:
    import argparse
    import sys

    p = argparse.ArgumentParser(description="Audit brand coverage on a JSONL of IM orders.")
    p.add_argument("orders_jsonl", help="Path to a .jsonl dump produced by pull_orders.py")
    p.add_argument("--brand-map", default=None, help="Path to brand_map.json (optional)")
    args = p.parse_args()

    mapper = BrandMapper(args.brand_map)
    orders = []
    with open(args.orders_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                orders.append(json.loads(line))

    audit = mapper.audit(orders)
    print(f"Total unique SKUs:  {audit['total_skus']}")
    print(f"Mapped:             {audit['mapped_skus']}")
    print(f"Unmapped:           {audit['unmapped_skus']}")
    print()
    print("By brand:")
    for b, s in sorted(audit["by_brand"].items(), key=lambda kv: -kv[1]["revenue"]):
        print(f"  {b:<15} {s['skus']:>3} SKUs   {s['units']:>5} units   ${s['revenue']:>10,.2f}")
    if audit["unmapped"]:
        print()
        print(f"UNMAPPED ({len(audit['unmapped'])}):")
        for u in audit["unmapped"]:
            print(f"  {u['sku']:<25} units={u['units']:<4} storefronts={u['storefronts']}")
        return 1
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
