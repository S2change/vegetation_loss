from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.buffer_ccd_nvg_propios import run_nvg_point_cleaning


def main() -> None:
    input_propios = PROJECT_ROOT / "Data" / "NVG_proprios_2015_2023_clean.gpkg"
    input_points = (
        PROJECT_ROOT
        / "Data"
        / "ccd_results_all_tiles_visual_analysis_data0_data1.shp"
    )
    output_dir = PROJECT_ROOT / "Results" / "NVG_clean_points_by_internal_buffer"
    output_dir.mkdir(parents=True, exist_ok=True)

    propios_layer = "NVG_2015-2023_Proprios_clean"
    target_crs = "EPSG:32629"
    buffer_m = -5.0

    print("Project root:       ", PROJECT_ROOT)
    print("NVG propios:        ", input_propios)
    print("NVG propios layer:  ", propios_layer)
    print("CCDC points:        ", input_points)
    print("Output directory:   ", output_dir)
    print("Target CRS:         ", target_crs)
    print("Internal buffer (m):", buffer_m)

    if not input_propios.exists():
        raise FileNotFoundError(f"Input file not found: {input_propios}")
    if not input_points.exists():
        raise FileNotFoundError(f"Input file not found: {input_points}")

    run_nvg_point_cleaning(
        nvg_path=str(input_propios),
        nvg_layer=propios_layer,
        points_path=str(input_points),
        points_layer=None,
        out_dir=str(output_dir),
        buffer_m=buffer_m,
        target_crs=target_crs,
        export_masks=True,
    )


if __name__ == "__main__":
    main()
