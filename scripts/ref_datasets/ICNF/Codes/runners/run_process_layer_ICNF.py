import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.process_layer_ICNF import harmonize_icnf_years_one_gpkg


def main() -> None:
    shapes_dir = PROJECT_ROOT / "Results" / "Normalized_text_columns"
    out_dir = PROJECT_ROOT / "Results" / "Harmonizacion_datos"
    out_dir.mkdir(parents=True, exist_ok=True)

    year_to_shp = {
        2020: shapes_dir / "ardida_2020_overlap_textnorm.shp",
        2021: shapes_dir / "ardida_2021_overlap_textnorm.shp",
        2022: shapes_dir / "ardida_2022_overlap_textnorm.shp",
        2023: shapes_dir / "ardida_2023_overlap_textnorm.shp",
        2024: shapes_dir / "ardida_2024_overlap_textnorm.shp",
    }

    for year, input_shp in year_to_shp.items():
        if not input_shp.exists():
            raise FileNotFoundError(
                f"Missing input for {year}: {input_shp}\n"
                "Run run_topology_icnf.py and run_normalize_string.py first."
            )

    output_gpkg = out_dir / "ICNF_2020_2024_harmonized.gpkg"

    print("Project root:       ", PROJECT_ROOT)
    print("Input folder:       ", shapes_dir)
    print("Output GPKG:        ", output_gpkg)
    print("Reports folder:     ", out_dir)

    harmonize_icnf_years_one_gpkg(
        year_to_shp={year: str(path) for year, path in year_to_shp.items()},
        out_gpkg=str(output_gpkg),
        reports_dir=str(out_dir),
        keep_only_harmonized=True,
    )


if __name__ == "__main__":
    main()
