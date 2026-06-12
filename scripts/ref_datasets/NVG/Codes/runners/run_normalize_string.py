from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.utils.normalize_string import normalize_text_fields


def main() -> None:
    input_file = (
        PROJECT_ROOT
        / "Results"
        / "CCDC_confirmation"
        / "ccd_results_all_tiles_visual_analysis_data0_data1_ccdc_flag.gpkg"
    )
    output_dir = PROJECT_ROOT / "Results" / "Normalized_text_columns"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "ccd_results_all_tiles_visual_analysis_data0_data1_ccdc_flag_textnorm.gpkg"

    print("Project root:       ", PROJECT_ROOT)
    print("Input CCDC flags:   ", input_file)
    print("Output normalized:  ", output_file)

    if not input_file.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}\n"
            "Run run_NVG_ccdc_confirmation.py first."
        )

    normalize_text_fields(str(input_file), str(output_file))


if __name__ == "__main__":
    main()
