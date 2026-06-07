import sys
from pathlib import Path

# Root of the BDR_DGT_300 folder, independent of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.process_layer_BDR import harmonize_bdr_layer


def main() -> None:
    input_file = (
        PROJECT_ROOT
        / "Results"
        / "Topologia_revisado"
        / "BDR_CCDC_TNE_v3_textnorm_tplgy.shp"
    )

    output_dir = PROJECT_ROOT / "Results" / "Harmonizacion_datos"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "BDR_CCDC_TNE_v1.gpkg"
    report_file = output_dir / "BDR_CCDC_TNE_v1.xlsx"

    print("Project root:       ", PROJECT_ROOT)
    print("Input:              ", input_file)
    print("Output:             ", output_file)
    print("Report:             ", report_file)

    if not input_file.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}\n"
            "Run run_normalize_string.py and run_topology.py first."
        )

    harmonize_bdr_layer(
        input_shp=str(input_file),
        output_shp=str(output_file),
        report_xlsx=str(report_file),
        keep_only_harmonized=True,
        layer_name="BDR_CCDC_TNE_v1",
    )


if __name__ == "__main__":
    main()
