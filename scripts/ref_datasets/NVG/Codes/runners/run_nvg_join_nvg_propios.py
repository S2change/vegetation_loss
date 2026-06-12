from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Codes.pipelines.nvg_join_nvg_propios import (
    nvg_join_harmon_to_propios_maxarea_1to1_one_gpkg,
)


def _print_layers(gpkg_path: Path, label: str) -> list[str]:
    import fiona

    layers = list(fiona.listlayers(str(gpkg_path)))
    print(f"\n--- Available layers: {label} ---")
    print("GPKG:", gpkg_path)
    for index, layer in enumerate(layers, start=1):
        print(f"  {index:02d}. {layer}")
    print("--- end layers ---\n")
    return layers


def main() -> None:
    input_propios = PROJECT_ROOT / "Data" / "NVG_proprios_2015_2023_clean.gpkg"
    input_harmonized = (
        PROJECT_ROOT
        / "Results"
        / "Harmonizacion_datos"
        / "NVG_pixels_clean_with_id_stats_windows_q50_p80_ccdc_dropNC1_2.gpkg"
    )
    sentinel2_tiles = (
        PROJECT_ROOT
        / "Data"
        / "S2_tiles"
        / "sentinel2_tiles_PT_terra_tm06.shp"
    )

    output_dir = PROJECT_ROOT / "Results" / "Harmonizacion_datos"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_gpkg = output_dir / "NVG_propios_split_by_harmonized_keep_propios.gpkg"
    validation_csv = output_dir / "NVG_split_keep_propios_validation.csv"

    propios_layer = "NVG_2015-2023_Proprios_clean"
    harmonized_layer = "PorId_dissolve_sin_Data0_Data1"

    print("Project root:       ", PROJECT_ROOT)
    print("Input propios:      ", input_propios)
    print("Input harmonized:   ", input_harmonized)
    print("Sentinel-2 tiles:   ", sentinel2_tiles)
    print("Output GPKG:        ", output_gpkg)
    print("Validation CSV:     ", validation_csv)

    for path in [input_propios, input_harmonized, sentinel2_tiles]:
        if not path.exists():
            raise FileNotFoundError(f"Required input not found: {path}")

    propios_layers = _print_layers(input_propios, "NVG propios")
    harmonized_layers = _print_layers(input_harmonized, "NVG harmonized")

    if propios_layer not in propios_layers:
        raise ValueError(
            f"Layer {propios_layer!r} was not found. Available: {propios_layers}"
        )
    if harmonized_layer not in harmonized_layers:
        raise ValueError(
            f"Layer {harmonized_layer!r} was not found. Available: {harmonized_layers}"
        )

    nvg_join_harmon_to_propios_maxarea_1to1_one_gpkg(
        input_propios=str(input_propios),
        propios_layer=propios_layer,
        input_harmonized=str(input_harmonized),
        harmonized_layer=harmonized_layer,
        out_gpkg=str(output_gpkg),
        sentinel2_tiles_path=str(sentinel2_tiles),
        out_layer_before_dissolve="NVG_propios_join_harmon_before_dissolve",
        out_layer_after_dissolve_id="NVG_propios_after_dissolve_by_Id",
        out_layer_after_dissolve_gleba="NVG_propios_after_dissolve_by_Id_gleba",
        out_layer_after_dissolve_final="NVG_harmonized",
        out_qa_layer="QA_split_stats",
        target_crs="EPSG:3763",
        normalize_output_columns=True,
        rebuild_area_ha=True,
        propios_gleba_col="id_gleba",
        dissolve_unmatched_by_gleba=True,
        fill_propios_src_uid=True,
        propios_src_value="nvg_propios",
        propios_uid_prefix="nvg_propios_",
        propios_uid_width=6,
        strict_join_validation=True,
        join_validation_tol=1e-9,
        run_validation=True,
        validation_report_csv=str(validation_csv),
    )

    _print_layers(output_gpkg, "final output")


if __name__ == "__main__":
    main()
