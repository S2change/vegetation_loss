from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.legacy.pipelines.process_layer_NVG import nvg_harmonize_and_dissolve_by_data1


def main() -> None:
    input_pixels = (
        PROJECT_ROOT
        / "Results"
        / "Pixel_polygons"
        / "NVG_S2_pixels_from_points_all_pixels.gpkg"
    )
    admin_file = PROJECT_ROOT / "Data" / "NUTS" / "areas_administrativas.shp"
    output_dir = PROJECT_ROOT / "Results" / "Legacy_harmonization"
    output_dir.mkdir(parents=True, exist_ok=True)

    pixels_harmonized = output_dir / "NVG_S2_pixels_all_harmonized.shp"
    dissolved_by_data1 = output_dir / "NVG_dissolved_Id_Data1_with_stats_harmonized.shp"
    stats_csv = output_dir / "NVG_subtalhao_stats_from_pixels.csv"

    print("LEGACY workflow: this runner is retained for traceability and is not part of run_all_NVG.py.")
    print("Input pixels:       ", input_pixels)
    print("Administrative:     ", admin_file)
    print("Pixels output:      ", pixels_harmonized)
    print("Dissolved output:   ", dissolved_by_data1)
    print("Statistics CSV:     ", stats_csv)

    if not input_pixels.exists():
        raise FileNotFoundError(f"Input file not found: {input_pixels}")
    if not admin_file.exists():
        raise FileNotFoundError(f"Administrative layer not found: {admin_file}")

    nvg_harmonize_and_dissolve_by_data1(
        input_pixels_shp=str(input_pixels),
        admin_areas_shp=str(admin_file),
        out_pixels_harmonized_shp=str(pixels_harmonized),
        out_dissolved_with_stats_shp=str(dissolved_by_data1),
        out_stats_csv=str(stats_csv),
        target_crs="EPSG:32629",
        keep_only_harmonized_pixels=True,
        keep_only_harmonized_dissolved=True,
    )


if __name__ == "__main__":
    main()
