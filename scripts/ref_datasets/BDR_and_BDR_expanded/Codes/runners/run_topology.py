import argparse
import sys
from pathlib import Path

# Root of the BDR_DGT_300 folder, independent of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.core.topology import add_overlap_flag_to_layer


def run(min_overlap_area: float = 1.0) -> None:
    input_shp = (
        PROJECT_ROOT
        / "Results"
        / "Normalized_text_columns"
        / "BDR_CCDC_TNE_v3_textnorm.shp"
    )
    output_dir = PROJECT_ROOT / "Results" / "Topologia_revisado"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_shp = output_dir / "BDR_CCDC_TNE_v3_textnorm_tplgy.shp"

    print("Project root:       ", PROJECT_ROOT)
    print("Input shapefile:    ", input_shp)
    print("Output shapefile:   ", output_shp)

    if not input_shp.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_shp}\n"
            "Run run_normalize_string.py first."
        )

    add_overlap_flag_to_layer(
        input_shp=str(input_shp),
        output_shp=str(output_shp),
        min_overlap_area=min_overlap_area,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run BDR topology analysis.")
    parser.add_argument("--min-overlap-area", type=float, default=1.0)
    args = parser.parse_args()
    run(min_overlap_area=args.min_overlap_area)
