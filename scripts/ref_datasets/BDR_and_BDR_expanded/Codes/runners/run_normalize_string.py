import sys
from pathlib import Path

# Root of the BDR_DGT_300 folder, independent of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.utils.normalize_string import normalize_text_fields


def main() -> None:
    data_dir = PROJECT_ROOT / "Data"
    results_dir = PROJECT_ROOT / "Results" / "Normalized_text_columns"
    results_dir.mkdir(parents=True, exist_ok=True)

    input_shp = data_dir / "BDR_CCDC_TNE_v3.shp"
    output_shp = results_dir / "BDR_CCDC_TNE_v3_textnorm.shp"

    print("Project root:       ", PROJECT_ROOT)
    print("Input shapefile:    ", input_shp)
    print("Output shapefile:   ", output_shp)

    if not input_shp.exists():
        raise FileNotFoundError(f"Input file not found: {input_shp}")

    normalize_text_fields(str(input_shp), str(output_shp))


if __name__ == "__main__":
    main()
