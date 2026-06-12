from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.NVG_ccdc_confirmation import flag_ccdc_results


def main() -> None:
    input_file = (
        PROJECT_ROOT
        / "Results"
        / "NVG_clean_points_by_internal_buffer"
        / "points_clean.gpkg"
    )
    output_dir = PROJECT_ROOT / "Results" / "CCDC_confirmation"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "ccd_results_all_tiles_visual_analysis_data0_data1_ccdc_flag.gpkg"

    print("Project root:       ", PROJECT_ROOT)
    print("Input clean points: ", input_file)
    print("Output CCDC flags:  ", output_file)

    if not input_file.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}\n"
            "Run run_buffer_ccd_nvg_propios.py first."
        )

    flag_ccdc_results(str(input_file), str(output_file))


if __name__ == "__main__":
    main()
