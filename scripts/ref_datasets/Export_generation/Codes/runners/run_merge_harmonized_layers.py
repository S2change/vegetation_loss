from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.merge_harmonized_layers import (  # noqa: E402
    merge_harmonized_layers_to_gpkg,
)


def main() -> None:
    data_dir = PROJECT_ROOT / "Data"
    output_dir = PROJECT_ROOT / "Results" / "Merged_harmonized_layers"
    output_dir.mkdir(parents=True, exist_ok=True)

    input_specs = {
        "NVG": {
            "path": str(data_dir / "NVG_v1.gpkg"),
            "layer": "NVG_v1",
        },
        "BDR": {
            "path": str(data_dir / "BDR_CCDC_TNE_v1.gpkg"),
            "layer": "BDR_CCDC_TNE_v1",
        },
        "ICNF": {
            "path": str(data_dir / "ICNF_2020_2024_harmonized_v1.gpkg"),
            "layer": None,
        },
        "BDRexpanded": {
            "path": str(data_dir / "BDR_expanded_v1.gpkg"),
            "layer": "BDR_expanded_v1",
        },
    }

    missing = [
        Path(spec["path"])
        for spec in input_specs.values()
        if not Path(spec["path"]).exists()
    ]
    if missing:
        missing_text = "\n".join(f"  - {path}" for path in missing)
        raise FileNotFoundError(
            "The following harmonized input files are missing:\n"
            f"{missing_text}"
        )

    output_file = output_dir / "all_harmonized_merged.gpkg"
    output_layer = "all_harmonized_merged"

    summary = merge_harmonized_layers_to_gpkg(
        input_specs=input_specs,
        output_path=output_file,
        output_layer=output_layer,
        keep_extra_fields=True,
        source_field_name="source_layer",
    )

    print("\n--- Merge summary ---")
    print("Output path:   ", summary["output_path"])
    print("Output layer:  ", summary["output_layer"])
    print("N features:    ", summary["n_features"])
    print("Shared columns:", summary["shared_columns"])
    print("Final columns: ", summary["final_columns"])
    print("--- end summary ---\n")


if __name__ == "__main__":
    main()
