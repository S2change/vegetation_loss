from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import geopandas as gpd
import pandas as pd


def _normalize_non_geometry_column_names(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Strip simple whitespace from non-geometry column names.
    Does not alter the geometry column name.
    """
    geom_col = gdf.geometry.name
    rename_map = {}

    for col in gdf.columns:
        if col == geom_col:
            continue
        rename_map[col] = str(col).strip()

    return gdf.rename(columns=rename_map)


def _read_vector(path: Path, layer: Optional[str] = None) -> gpd.GeoDataFrame:
    """
    Read a vector dataset. If layer is None, geopandas reads the default/first layer.
    """
    if layer is None:
        gdf = gpd.read_file(path)
    else:
        gdf = gpd.read_file(path, layer=layer)

    if gdf.empty:
        gdf = gdf.copy()

    gdf = _normalize_non_geometry_column_names(gdf)
    return gdf


def _non_geometry_columns(gdf: gpd.GeoDataFrame) -> List[str]:
    geom_col = gdf.geometry.name
    return [c for c in gdf.columns if c != geom_col]


def _lower_to_actual(columns: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for col in columns:
        out[str(col).lower()] = col
    return out


def _get_output_field_name(source_name: str, field_name: str) -> str:
    """
    Harmonize input field names to the final output schema.

    For NVG:
    - Data0_p10 -> Data0
    - Data1_p90 -> Data1

    All other fields remain unchanged.
    """
    field_lc = str(field_name).lower()

    if source_name == "NVG":
        if field_lc == "data0_p10":
            return "Data0"
        if field_lc == "data1_p90":
            return "Data1"

    return str(field_name)


def _reorder_columns_with_date_pair(
    columns: List[str],
    source_field_name: str = "source_layer",
) -> List[str]:
    """
    Reorder columns so Data0 and Data1 stay together in the final schema.

    Rules
    -----
    - Keep all other columns in their current order.
    - Remove Data0/Data1 from their original positions.
    - Reinsert them together.
    - Prefer placing them where the first of the pair originally appeared.
    - Keep source_field_name at the end if present.
    """
    if not columns:
        return columns

    source_key = source_field_name.lower()
    data0_name = None
    data1_name = None
    data0_idx = None
    data1_idx = None

    for i, col in enumerate(columns):
        col_lc = str(col).lower()
        if col_lc == "data0" and data0_name is None:
            data0_name = col
            data0_idx = i
        elif col_lc == "data1" and data1_name is None:
            data1_name = col
            data1_idx = i

    base_cols = [
        c for c in columns
        if str(c).lower() not in ("data0", "data1", source_key)
    ]

    pair = []
    if data0_name is not None:
        pair.append(data0_name)
    if data1_name is not None:
        pair.append(data1_name)

    if pair:
        original_positions = [x for x in (data0_idx, data1_idx) if x is not None]
        insert_at = min(original_positions)

        n_before = 0
        for i, col in enumerate(columns):
            if i >= insert_at:
                break
            if str(col).lower() not in ("data0", "data1", source_key):
                n_before += 1

        final_cols = base_cols[:n_before] + pair + base_cols[n_before:]
    else:
        final_cols = base_cols

    if any(str(c).lower() == source_key for c in columns):
        final_cols.append(source_field_name)

    return final_cols


def _shared_columns(gdfs: Dict[str, gpd.GeoDataFrame]) -> List[str]:
    """
    Return the shared non-geometry columns across all input GeoDataFrames.
    Matching is case-insensitive after harmonizing output names.
    Output names are taken from the first dataset after harmonization.
    """
    common_lower: Optional[set[str]] = None

    for source_name, gdf in gdfs.items():
        cols_lower = {
            _get_output_field_name(source_name, c).lower()
            for c in _non_geometry_columns(gdf)
        }
        if common_lower is None:
            common_lower = cols_lower
        else:
            common_lower = common_lower.intersection(cols_lower)

    if not common_lower:
        return []

    first_key = next(iter(gdfs.keys()))
    first_names = {
        _get_output_field_name(first_key, c).lower(): _get_output_field_name(first_key, c)
        for c in _non_geometry_columns(gdfs[first_key])
    }

    shared = [first_names[c] for c in sorted(common_lower)]
    return shared


def _union_columns(gdfs: Dict[str, gpd.GeoDataFrame]) -> List[str]:
    """
    Return the union of non-geometry columns without duplicates.
    Matching is case-insensitive after harmonizing output names.
    Keeps first-seen harmonized output name.
    """
    ordered_lower: List[str] = []
    preferred_name: Dict[str, str] = {}
    seen: set[str] = set()

    for source_name, gdf in gdfs.items():
        for col in _non_geometry_columns(gdf):
            out_col = _get_output_field_name(source_name, col)
            key = out_col.lower()

            if key not in seen:
                seen.add(key)
                ordered_lower.append(key)
                preferred_name[key] = out_col

    return [preferred_name[k] for k in ordered_lower]


def _validate_crs(gdfs: Dict[str, gpd.GeoDataFrame]) -> None:
    crs_values = {str(gdf.crs) for gdf in gdfs.values()}
    if len(crs_values) > 1:
        raise ValueError(
            "All input layers must have the same CRS. "
            f"Found: {sorted(crs_values)}"
        )


def _build_output_to_source_map(
    gdf: gpd.GeoDataFrame,
    source_name: str,
) -> Dict[str, str]:
    """
    Build mapping:
    output_field_name_lower -> actual_source_field_name

    Example for NVG:
    - data0 -> Data0_p10
    - data1 -> Data1_p90
    """
    out: Dict[str, str] = {}

    for col in _non_geometry_columns(gdf):
        out_name = _get_output_field_name(source_name, col)
        out[out_name.lower()] = col

    return out


def _align_columns_for_concat(
    gdf: gpd.GeoDataFrame,
    final_columns: List[str],
    source_name: str,
    source_field_name: str,
) -> gpd.GeoDataFrame:
    """
    Create a new GeoDataFrame with the exact target schema.
    Missing columns are filled with pd.NA.

    Uses harmonized output field names. For NVG:
    - values from Data0_p10 are stored in Data0
    - values from Data1_p90 are stored in Data1
    """
    output_to_source = _build_output_to_source_map(gdf, source_name)
    data: Dict[str, pd.Series | str | object] = {}

    for final_col in final_columns:
        if final_col == source_field_name:
            data[final_col] = source_name
            continue

        key = final_col.lower()
        if key in output_to_source:
            source_col = output_to_source[key]
            data[final_col] = gdf[source_col]
        else:
            data[final_col] = pd.NA

    aligned = gpd.GeoDataFrame(data, geometry=gdf.geometry, crs=gdf.crs)
    return aligned


def read_harmonized_inputs(
    input_specs: Dict[str, Dict[str, Optional[str]]]
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Read all harmonized input layers.

    Parameters
    ----------
    input_specs : dict
        Example:
        {
            "NVG": {"path": ".../NVG_harmonized.gpkg", "layer": None},
            "ICNF": {"path": ".../ICNF_harmonized.gpkg", "layer": None},
            "BDR": {"path": ".../BDR_harmonized.gpkg", "layer": None},
            "BDRexpanded": {"path": ".../BDRexpanded_harmonized.gpkg", "layer": None},
        }

    Returns
    -------
    dict[str, GeoDataFrame]
    """
    gdfs: Dict[str, gpd.GeoDataFrame] = {}

    for source_name, spec in input_specs.items():
        if "path" not in spec:
            raise ValueError(f"Input spec for {source_name} must include 'path'.")

        path = Path(spec["path"])
        layer = spec.get("layer", None)

        if not path.exists():
            raise FileNotFoundError(f"Input not found for {source_name}: {path}")

        gdf = _read_vector(path=path, layer=layer)
        gdfs[source_name] = gdf

    if not gdfs:
        raise ValueError("No input datasets were provided.")

    _validate_crs(gdfs)
    return gdfs


def build_merged_harmonized_layer(
    gdfs: Dict[str, gpd.GeoDataFrame],
    keep_extra_fields: bool = True,
    source_field_name: str = "source_layer",
) -> Tuple[gpd.GeoDataFrame, Dict[str, List[str]]]:
    """
    Merge all harmonized layers into a single GeoDataFrame.

    Parameters
    ----------
    gdfs : dict[str, GeoDataFrame]
        Input harmonized layers already read.
    keep_extra_fields : bool, default True
        If True, keep shared + extra fields.
        If False, keep only shared fields.
    source_field_name : str, default "source_layer"
        Name of the column storing the source dataset name.

    Returns
    -------
    merged_gdf : GeoDataFrame
    info : dict
        Useful metadata:
        {
            "shared_columns": [...],
            "final_columns": [...],
        }
    """
    if not gdfs:
        raise ValueError("gdfs is empty.")

    shared_cols = _shared_columns(gdfs)
    if not shared_cols:
        raise ValueError("No shared non-geometry columns were found across inputs.")

    if keep_extra_fields:
        final_columns = _union_columns(gdfs)
    else:
        final_columns = shared_cols.copy()

    final_columns = _reorder_columns_with_date_pair(
        final_columns,
        source_field_name=source_field_name,
    )

    if source_field_name not in final_columns:
        final_columns.append(source_field_name)

    aligned_gdfs: List[gpd.GeoDataFrame] = []
    first_key = next(iter(gdfs.keys()))
    target_crs = gdfs[first_key].crs

    for source_name, gdf in gdfs.items():
        aligned = _align_columns_for_concat(
            gdf=gdf,
            final_columns=final_columns,
            source_name=source_name,
            source_field_name=source_field_name,
        )
        aligned_gdfs.append(aligned)

    merged = gpd.GeoDataFrame(
        pd.concat(aligned_gdfs, ignore_index=True),
        geometry="geometry",
        crs=target_crs,
    )

    info = {
        "shared_columns": shared_cols,
        "final_columns": final_columns,
    }
    return merged, info


def export_merged_harmonized_layer(
    merged_gdf: gpd.GeoDataFrame,
    output_path: Path | str,
    output_layer: str,
) -> Path:
    """
    Export merged GeoDataFrame to a GPKG layer.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    merged_gdf.to_file(output_path, layer=output_layer, driver="GPKG")
    return output_path


def merge_harmonized_layers_to_gpkg(
    input_specs: Dict[str, Dict[str, Optional[str]]],
    output_path: Path | str,
    output_layer: str,
    keep_extra_fields: bool = True,
    source_field_name: str = "source_layer",
) -> Dict[str, object]:
    """
    High-level pipeline:
    read inputs -> merge -> export

    Returns a small summary dict.
    """
    gdfs = read_harmonized_inputs(input_specs=input_specs)

    merged_gdf, info = build_merged_harmonized_layer(
        gdfs=gdfs,
        keep_extra_fields=keep_extra_fields,
        source_field_name=source_field_name,
    )

    export_merged_harmonized_layer(
        merged_gdf=merged_gdf,
        output_path=output_path,
        output_layer=output_layer,
    )

    summary = {
        "output_path": str(output_path),
        "output_layer": output_layer,
        "n_features": int(len(merged_gdf)),
        "shared_columns": info["shared_columns"],
        "final_columns": info["final_columns"],
    }
    return summary