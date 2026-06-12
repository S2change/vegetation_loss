import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.utils.normalize_string import normalize_text_fields


def main() -> None:
    input_dir = PROJECT_ROOT / "Results" / "Topologia_revisado"
    output_dir = PROJECT_ROOT / "Results" / "Normalized_text_columns"
    output_dir.mkdir(parents=True, exist_ok=True)

    input_files = sorted(input_dir.glob("ardida_*_overlap.shp"))
    if not input_files:
        raise FileNotFoundError(
            f"No ICNF topology outputs were found in: {input_dir}\n"
            "Run run_topology_icnf.py first."
        )

    print("Project root:       ", PROJECT_ROOT)
    print("Input folder:       ", input_dir)
    print("Output folder:      ", output_dir)

    for input_shp in input_files:
        output_shp = output_dir / f"{input_shp.stem}_textnorm.shp"
        print("Input shapefile:    ", input_shp)
        print("Output shapefile:   ", output_shp)
        normalize_text_fields(str(input_shp), str(output_shp))


if __name__ == "__main__":
    main()
