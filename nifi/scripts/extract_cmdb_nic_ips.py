#!/usr/bin/env python3
"""Extract IPv4 addresses from cmdb nicdetail.csv into a line-delimited list for NiFi."""

from __future__ import annotations

import csv
import ipaddress
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
NICDETAIL_CSV = ROOT / "data" / "cmdb_models_dev" / "cmdb" / "nicdetail.csv"
OUT = Path(__file__).resolve().parent.parent / "data" / "cmdb_nic_ips.txt"


def is_valid_ipv4(value: str) -> bool:
    if not value or "x" in value.lower():
        return False
    try:
        addr = ipaddress.ip_address(value)
    except ValueError:
        return False
    return isinstance(addr, ipaddress.IPv4Address) and value != "0.0.0.0"


def main() -> None:
    if not NICDETAIL_CSV.is_file():
        raise SystemExit(f"Missing {NICDETAIL_CSV}")

    ips: list[str] = []
    seen: set[str] = set()
    with NICDETAIL_CSV.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            raw = (row.get("ip_address") or "").strip()
            if not is_valid_ipv4(raw) or raw in seen:
                continue
            seen.add(raw)
            ips.append(raw)

    ips.sort()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(ips) + ("\n" if ips else ""), encoding="utf-8")
    print(f"Wrote {len(ips)} IPs to {OUT}")


if __name__ == "__main__":
    main()
