from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.nvg_points_to_polygons import points_to_s2_pixels


def main() -> None:
    input_points = (
        PROJECT_ROOT
        / "Results"
        / "Normalized_text_columns"
        / "ccd_results_all_tiles_visual_analysis_data0_data1_ccdc_flag_textnorm.gpkg"
    )
    output_dir = PROJECT_ROOT / "Results" / "Pixel_polygons"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_pixels = output_dir / "NVG_S2_pixels_from_points_all_pixels.gpkg"

    print("Project root:       ", PROJECT_ROOT)
    print("Input points:       ", input_points)
    print("Output pixels:      ", output_pixels)

    if not input_points.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_points}\n"
            "Run run_normalize_string.py first."
        )

    points_to_s2_pixels(
        points_shp=str(input_points),
        output_pixels_shp=str(output_pixels),
        pixel_size=10.0,
        target_crs="EPSG:32629",
    )


if __name__ == "__main__":
    main()
