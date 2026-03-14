from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from capastudy.merge_enrichment import (
    add_alliance_trade_columns,
    add_ana_etd_weeknum,
    attach_ids_to_port_calls,
    enrich_port_calls,
    enrich_voyages_with_ids,
    enrich_voyages_with_teu,
    ensure_vessel_db_coverage,
)
from capastudy.merge_loading import load_latest_all, load_service_meta_map, load_vessel_maps
from capastudy.merge_state import save_merged, save_update_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge carriers and maintain incremental update state.")
    parser.add_argument(
        "--no-update-state",
        action="store_true",
        help="Only produce timestamped merged file; do not maintain current/history/changes state.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    voyages, port_calls, selected = load_latest_all()
    ensure_vessel_db_coverage(voyages, port_calls)
    teu_map, imo_map = load_vessel_maps()
    service_meta = load_service_meta_map()
    voyages = enrich_voyages_with_ids(voyages, imo_map)
    voyages = enrich_voyages_with_teu(voyages, teu_map)
    port_calls = attach_ids_to_port_calls(port_calls, voyages, imo_map)
    port_calls = enrich_port_calls(port_calls, teu_map)
    voyages = add_ana_etd_weeknum(voyages, port_calls)
    voyages, port_calls = add_alliance_trade_columns(voyages, port_calls, service_meta)
    output = save_merged(voyages, port_calls, selected)
    update_outputs: Dict[str, Path] = {}
    if not args.no_update_state:
        update_outputs = save_update_outputs(voyages, port_calls, selected)

    print("Selected source files:")
    for carrier, path in selected.items():
        print(f"- {carrier}: {path}")
    print(f"Merged voyages: {len(voyages)}")
    print(f"Merged port calls: {len(port_calls)}")
    print(f"Merged output: {output}")
    if update_outputs:
        print(f"Update current: {update_outputs['current']}")
        print(f"Update history: {update_outputs['history']}")
        print(f"Snapshot output: {update_outputs['snapshot']}")
        print(f"Changes output: {update_outputs['changes']}")


if __name__ == "__main__":
    main()
