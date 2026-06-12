from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.process_q3_q1_q9_10 import nvg_pipeline_pixels_normal_one_gpkg


def main() -> None:
    input_pixels = (
        PROJECT_ROOT
        / "Results"
        / "Pixel_polygons"
        / "NVG_S2_pixels_from_points_all_pixels.gpkg"
    )
    admin_file = PROJECT_ROOT / "Data" / "NUTS" / "areas_administrativas.shp"

    output_dir = PROJECT_ROOT / "Results" / "Harmonizacion_datos"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_gpkg = output_dir / "NVG_pixels_clean_with_id_stats_windows_q50_p80_ccdc_dropNC1_2.gpkg"
    stats_csv = output_dir / "NVG_stats_by_id_q1_q3_p10_p90_spans_ccdc_dropNC1_2.csv"
    validation_csv = output_dir / "NVG_validation_report_stats_vs_dissolve.csv"

    print("Project root:       ", PROJECT_ROOT)
    print("Input pixels:       ", input_pixels)
    print("Administrative:     ", admin_file)
    print("Output GPKG:        ", output_gpkg)
    print("Statistics CSV:     ", stats_csv)
    print("Validation CSV:     ", validation_csv)

    if not input_pixels.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_pixels}\n"
            "Run run_nvg_points_to_polygons.py first."
        )
    if not admin_file.exists():
        raise FileNotFoundError(f"Administrative layer not found: {admin_file}")

    nvg_pipeline_pixels_normal_one_gpkg(
        input_pixels_shp=str(input_pixels),
        admin_areas_shp=str(admin_file),
        out_gpkg=str(output_gpkg),
        out_stats_csv=str(stats_csv),
        target_crs="EPSG:32629",
        run_validation=True,
        validation_raise=False,
        validation_report_csv=str(validation_csv),
        date_tol_days=0,
    )


if __name__ == "__main__":
    main()
