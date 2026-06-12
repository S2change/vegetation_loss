# -*- coding: utf-8 -*-

import os
import re
import uuid

from qgis.PyQt.QtCore import QVariant
from qgis.PyQt.QtGui import QColor, QFont
from qgis.PyQt.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QLineEdit,
    QPushButton,
    QCheckBox,
    QMessageBox,
    QFileDialog,
    QDialogButtonBox,
    QWidget,
    QScrollArea,
    QGroupBox,
)

from qgis.core import (
    Qgis,
    QgsProject,
    QgsMapLayerType,
    QgsVectorLayer,
    QgsFeature,
    QgsField,
    QgsFields,
    QgsWkbTypes,
    QgsVectorFileWriter,
    QgsSymbol,
    QgsCategorizedSymbolRenderer,
    QgsRendererCategory,
    QgsPalLayerSettings,
    QgsTextFormat,
    QgsTextBufferSettings,
    QgsVectorLayerSimpleLabeling,
)

from qgis.utils import iface

  
# ============================================================
# CONFIG
# ============================================================

SOURCE_OPTIONS = ["NVG", "ICNF", "BDR", "BDRexpanded"]

SOURCE_BASE_DATE_FIELD = {
    "NVG": "Data0_p10",
    "ICNF": "Data0",
    "BDR": "Data0",
    "BDRexpanded": "Data0",
}

MONTH_COLORS = {
    1:  "#fff5f0",
    2:  "#fee0d2",
    3:  "#fcbba1",
    4:  "#fc9272",
    5:  "#fb6a4a",
    6:  "#ef3b2c",
    7:  "#cb181d",
    8:  "#a50f15",
    9:  "#99000d",
    10: "#7f0000",
    11: "#67000d",
    12: "#49000a",
}
DEFAULT_COLOR = "#bdbdbd"


# ============================================================
# DETECCIÓN DE CAPAS
# ============================================================

def classify_layer(layer_name):
    """Classify the harmonized layers loaded in the current QGIS project."""
    name = (layer_name or "").strip().lower()

    # Normalize spaces, hyphens, em dashes and other separators so that
    # names such as "BDR expanded v1" and "BDR_expanded_v1" are equivalent.
    normalized_name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")

    # Check BDR Expanded before BDR to avoid ambiguous classifications.
    if "bdr_expanded" in normalized_name:
        return {"source_value": "BDRexpanded"}

    if "bdr_ccdc_tne" in normalized_name:
        return {"source_value": "BDR"}

    if "icnf_2020_2024_harmonized" in normalized_name:
        return {"source_value": "ICNF"}

    # The current QGIS project uses the layer name NVG_v1.
    if normalized_name == "nvg" or normalized_name.startswith("nvg_"):
        return {"source_value": "NVG"}

    return None


def get_layers_by_source(source_value):
    result = []
    for layer in QgsProject.instance().mapLayers().values():
        if layer.type() != QgsMapLayerType.VectorLayer:
            continue

        meta = classify_layer(layer.name())
        if meta is None:
            continue

        if meta["source_value"] == source_value:
            result.append((layer, meta))

    return result


# ============================================================
# HELPERS
# ============================================================

def qvariant_is_null(value):
    if value is None:
        return True

    try:
        if value.isNull():
            return True
    except Exception:
        pass

    try:
        txt = str(value).strip()
    except Exception:
        return True

    if txt == "" or txt.lower() in ("null", "none"):
        return True

    return False


def find_field_case_insensitive(layer, field_name):
    wanted = field_name.lower()
    for f in layer.fields():
        if f.name().lower() == wanted:
            return f.name()
    return None


def clone_qgs_field(field_obj):
    return QgsField(
        field_obj.name(),
        field_obj.type(),
        field_obj.typeName(),
        field_obj.length(),
        field_obj.precision()
    )


def parse_year_month_from_value(value):
    if qvariant_is_null(value):
        return (None, None)

    text = str(value).strip()

    match = re.search(r"(20\d{2})[-/](\d{1,2})", text)
    if match:
        try:
            return (int(match.group(1)), int(match.group(2)))
        except Exception:
            return (None, None)

    match = re.search(r"(20\d{2})", text)
    if match:
        try:
            return (int(match.group(1)), None)
        except Exception:
            return (None, None)

    return (None, None)


def clean_label_date_text(value):
    if qvariant_is_null(value):
        return None

    text = str(value).strip()
    if text == "" or text.lower() in ("null", "none"):
        return None

    return re.sub(r"^20(\d{2})([-/])", r"\1\2", text)


def build_time_code(year_ref, month_ref):
    if year_ref is None:
        return None
    if month_ref is None:
        return "%04d" % int(year_ref)
    return "%04d-%02d" % (int(year_ref), int(month_ref))


def build_label_text(source_value, base_date_value, year_ref, month_ref):
    cleaned = clean_label_date_text(base_date_value)
    if cleaned is not None:
        return "%s | %s" % (source_value, cleaned)

    time_code = build_time_code(year_ref, month_ref)
    if time_code is None:
        return "%s | no date" % source_value

    cleaned_time = re.sub(r"^20(\d{2})-", r"\1-", time_code)
    return "%s | %s" % (source_value, cleaned_time)


def ensure_output_gpkg(path):
    path = path.strip()
    if not path.lower().endswith(".gpkg"):
        path += ".gpkg"
    return path


def detect_geometry_type(source_layers):
    for layer, meta in source_layers:
        if layer is not None and layer.isValid():
            return layer.wkbType()
    return None


def detect_crs(source_layers):
    for layer, meta in source_layers:
        if layer is not None and layer.isValid():
            return layer.crs()
    return None


def get_base_date_field_for_source(source_value):
    return SOURCE_BASE_DATE_FIELD[source_value]


def get_output_field_name(source_value, field_name):
    field_lc = field_name.lower()

    if source_value == "NVG":
        if field_lc == "data0_p10":
            return "Data0"
        if field_lc == "data1_p90":
            return "Data1"

    return field_name


def get_source_field_name_for_export(source_value, output_field_name):
    field_lc = output_field_name.lower()

    if source_value == "NVG":
        if field_lc == "data0":
            return "Data0_p10"
        if field_lc == "data1":
            return "Data1_p90"

    return output_field_name


def get_visual_date_field_for_saved_layer(source_value, original_visual_base_field):
    if source_value == "NVG" and original_visual_base_field.lower() == "data0_p10":
        return "Data0"
    return original_visual_base_field


def feature_matches(layer, feature, selected_date_field, selected_year, selected_month):
    local_date_field = find_field_case_insensitive(layer, selected_date_field)
    if local_date_field is None:
        return False

    value = feature[local_date_field]
    year_found, month_found = parse_year_month_from_value(value)

    if year_found is None:
        return False

    if selected_year is not None and int(year_found) != int(selected_year):
        return False

    if selected_month is None:
        return True

    if month_found is None:
        return False

    return int(month_found) == int(selected_month)


# ============================================================
# AÑOS / MESES DISPONIBLES
# ============================================================

def collect_available_years(source_layers, selected_date_field):
    years = set()

    for layer, meta in source_layers:
        local_date_field = find_field_case_insensitive(layer, selected_date_field)
        if local_date_field is None:
            continue

        for feature in layer.getFeatures():
            value = feature[local_date_field]
            year_ref, month_ref = parse_year_month_from_value(value)
            if year_ref is not None:
                years.add(int(year_ref))

    return sorted(years)


def collect_available_months(source_layers, selected_date_field, selected_year):
    months = set()

    for layer, meta in source_layers:
        local_date_field = find_field_case_insensitive(layer, selected_date_field)
        if local_date_field is None:
            continue

        for feature in layer.getFeatures():
            value = feature[local_date_field]
            year_ref, month_ref = parse_year_month_from_value(value)

            if year_ref is None:
                continue

            if int(year_ref) != int(selected_year):
                continue

            if month_ref is not None:
                months.add(int(month_ref))

    return sorted(months)


def collect_available_years_multi(selected_sources):
    years = set()

    for source_value in selected_sources:
        source_layers = get_layers_by_source(source_value)
        date_field = get_base_date_field_for_source(source_value)

        for y in collect_available_years(source_layers, date_field):
            years.add(int(y))

    return sorted(years)


def collect_available_months_multi(selected_sources, selected_year):
    months = set()

    for source_value in selected_sources:
        source_layers = get_layers_by_source(source_value)
        date_field = get_base_date_field_for_source(source_value)

        for m in collect_available_months(source_layers, date_field, selected_year):
            months.add(int(m))

    return sorted(months)


# ============================================================
# CAMPOS
# ============================================================

def get_available_fields(source_layers):
    field_defs = {}
    actual_name_map = {}

    for layer, meta in source_layers:
        for fld in layer.fields():
            key = fld.name().lower()
            if key not in field_defs:
                field_defs[key] = clone_qgs_field(fld)
                actual_name_map[key] = fld.name()

    field_names = [actual_name_map[k] for k in sorted(actual_name_map.keys())]
    return field_names, field_defs, actual_name_map


def get_available_fields_by_sources(selected_sources):
    fields_by_source = {}
    field_defs_union = {}
    presence_map = {}
    actual_name_map_union = {}

    for source_value in selected_sources:
        source_layers = get_layers_by_source(source_value)
        available_fields, field_defs, actual_name_map = get_available_fields(source_layers)

        fields_by_source[source_value] = available_fields

        for k, fld in field_defs.items():
            if k not in field_defs_union:
                field_defs_union[k] = fld

        for k, actual_name in actual_name_map.items():
            if k not in actual_name_map_union:
                actual_name_map_union[k] = actual_name

            if k not in presence_map:
                presence_map[k] = set()
            presence_map[k].add(source_value)

    return fields_by_source, field_defs_union, presence_map, actual_name_map_union


def get_shared_fields(selected_sources, presence_map, actual_name_map_union):
    shared = []
    n = len(selected_sources)

    for field_lc, present_sources in presence_map.items():
        if len(present_sources) == n:
            shared.append(actual_name_map_union[field_lc])

    return sorted(shared, key=lambda x: x.lower())


def get_extra_fields_by_source(selected_sources, presence_map, actual_name_map_union):
    extra_by_source = {src: [] for src in selected_sources}

    for field_lc, present_sources in presence_map.items():
        if len(present_sources) == len(selected_sources):
            continue

        actual_name = actual_name_map_union[field_lc]
        for src in sorted(present_sources):
            extra_by_source[src].append(actual_name)

    for src in extra_by_source:
        extra_by_source[src] = sorted(set(extra_by_source[src]), key=lambda x: x.lower())

    return extra_by_source


def get_default_checked_fields(source_value, available_field_names):
    available_lc = {name.lower(): name for name in available_field_names}

    common_wanted = [
        "id",
        "src",
        "uid",
        "chg_type",
        "pi_dicofre",
        "temp_eval_start",
        "temp_eval_end",
        "validation_flag",
        "area_ha",
    ]

    if source_value == "NVG":
        source_specific = ["data0_p10", "data1_p90"]
    else:
        source_specific = ["data0", "data1"]

    wanted = common_wanted + source_specific

    defaults = []
    seen = set()

    for w in wanted:
        if w in available_lc and w not in seen:
            defaults.append(available_lc[w])
            seen.add(w)

    return defaults


def get_default_checked_fields_for_multi(selected_sources, shared_fields, fields_by_source):
    shared_lc = {x.lower(): x for x in shared_fields}

    defaults_shared = []
    seen = set()

    common_wanted = [
        "id",
        "src",
        "uid",
        "chg_type",
        "pi_dicofre",
        "temp_eval_start",
        "temp_eval_end",
        "validation_flag",
        "area_ha",
    ]

    for w in common_wanted:
        if w in shared_lc and w not in seen:
            defaults_shared.append(shared_lc[w])
            seen.add(w)

    for src in selected_sources:
        base_f = get_base_date_field_for_source(src).lower()
        if base_f in shared_lc and base_f not in seen:
            defaults_shared.append(shared_lc[base_f])
            seen.add(base_f)

    defaults_extra_by_source = {}

    for src in selected_sources:
        defaults_extra_by_source[src] = get_default_checked_fields(src, fields_by_source[src])

    return defaults_shared, defaults_extra_by_source


# ============================================================
# DIÁLOGO DE CAMPOS
# ============================================================

class MultiSourceFieldSelectionDialog(QDialog):
    def __init__(
        self,
        selected_sources,
        shared_fields,
        extra_fields_by_source,
        default_checked_shared,
        default_checked_extra_by_source,
        required_shared_fields,
        parent=None
    ):
        QDialog.__init__(self, parent)
        self.setWindowTitle("Select fields to export")
        self.setMinimumWidth(560)
        self.setMinimumHeight(680)

        self.required_shared_fields = [x.lower() for x in required_shared_fields]
        self.shared_checkboxes = []
        self.extra_checkboxes_by_source = {}

        main_layout = QVBoxLayout()

        info = QLabel(
            "Shared fields are exported by default. "
            "Extra fields are grouped by source so they do not get mixed. "
            "Defaults from each source are preselected. "
            "Missing extra fields are written as NULL where they do not exist. "
            "Note: for NVG, Data0_p10 and Data1_p90 keep these names in the selection dialog, "
            "but in the output they are stored as Data0 and Data1."
        )
        info.setWordWrap(True)
        main_layout.addWidget(info)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)

        container = QWidget()
        container_layout = QVBoxLayout()

        shared_group = QGroupBox("Shared fields")
        shared_layout = QVBoxLayout()
        default_shared_lc = {x.lower() for x in default_checked_shared}

        for field_name in sorted(shared_fields, key=lambda x: x.lower()):
            cb = QCheckBox(field_name)

            if field_name.lower() in default_shared_lc:
                cb.setChecked(True)

            if field_name.lower() in self.required_shared_fields:
                cb.setChecked(True)
                cb.setEnabled(False)

            self.shared_checkboxes.append(cb)
            shared_layout.addWidget(cb)

        shared_group.setLayout(shared_layout)
        container_layout.addWidget(shared_group)

        for src in selected_sources:
            src_fields = extra_fields_by_source.get(src, [])
            if not src_fields:
                continue

            grp = QGroupBox("Extra fields - %s" % src)
            lay = QVBoxLayout()

            default_extra_lc = {
                x.lower() for x in default_checked_extra_by_source.get(src, [])
            }

            self.extra_checkboxes_by_source[src] = []

            for field_name in sorted(src_fields, key=lambda x: x.lower()):
                cb = QCheckBox(field_name)

                if field_name.lower() in default_extra_lc:
                    cb.setChecked(True)

                self.extra_checkboxes_by_source[src].append(cb)
                lay.addWidget(cb)

            grp.setLayout(lay)
            container_layout.addWidget(grp)

        container_layout.addStretch()
        container.setLayout(container_layout)
        scroll.setWidget(container)
        main_layout.addWidget(scroll)

        row = QHBoxLayout()

        btn_select_shared = QPushButton("Select shared")
        btn_select_shared.clicked.connect(self.select_shared_defaults)

        btn_clear_extras = QPushButton("Clear extras")
        btn_clear_extras.clicked.connect(self.clear_extras)

        row.addWidget(btn_select_shared)
        row.addWidget(btn_clear_extras)
        main_layout.addLayout(row)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.validate_and_accept)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)

        self.setLayout(main_layout)

    def select_shared_defaults(self):
        for cb in self.shared_checkboxes:
            cb.setChecked(True)

        for cb in self.shared_checkboxes:
            if cb.text().lower() in self.required_shared_fields:
                cb.setChecked(True)
                cb.setEnabled(False)

    def clear_extras(self):
        for src in self.extra_checkboxes_by_source:
            for cb in self.extra_checkboxes_by_source[src]:
                cb.setChecked(False)

    def get_selected_fields(self):
        out = []

        for cb in self.shared_checkboxes:
            if cb.isChecked():
                out.append(cb.text())

        for src in self.extra_checkboxes_by_source:
            for cb in self.extra_checkboxes_by_source[src]:
                if cb.isChecked():
                    out.append(cb.text())

        seen = set()
        clean = []
        for x in out:
            key = x.lower()
            if key not in seen:
                clean.append(x)
                seen.add(key)

        return clean

    def validate_and_accept(self):
        selected = [x.lower() for x in self.get_selected_fields()]

        if not selected:
            QMessageBox.warning(self, "Warning", "You must select at least one field.")
            return

        for req in self.required_shared_fields:
            if req not in selected:
                QMessageBox.warning(
                    self,
                    "Warning",
                    "Required shared fields must remain selected."
                )
                return

        self.accept()


# ============================================================
# CAPA DE SALIDA
# ============================================================

def build_output_fields(selected_field_names, field_defs, include_time_code=False):
    fields = QgsFields()
    used_names = set()

    for field_name in selected_field_names:
        key = field_name.lower()

        if key in used_names:
            continue

        if key in field_defs:
            src_field = field_defs[key]
            fields.append(
                QgsField(
                    field_name,
                    src_field.type(),
                    src_field.typeName(),
                    src_field.length(),
                    src_field.precision()
                )
            )
        else:
            fields.append(QgsField(field_name, QVariant.String))

        used_names.add(key)

    fields.append(QgsField("label_txt", QVariant.String))

    if include_time_code:
        fields.append(QgsField("time_code", QVariant.String))

    return fields


def create_memory_output_layer_single(
    source_layers,
    source_value,
    selected_date_field,
    selected_year,
    selected_month,
    selected_field_names,
    field_defs
):
    if not source_layers:
        raise Exception("No loaded layers were found for the selected source.")

    wkb_type = detect_geometry_type(source_layers)
    crs = detect_crs(source_layers)

    if wkb_type is None or crs is None:
        raise Exception("Could not detect geometry type or CRS from source layers.")

    output_field_names = []
    output_field_defs = {}

    for selected_name in selected_field_names:
        out_name = get_output_field_name(source_value, selected_name)
        out_key = out_name.lower()
        src_key = selected_name.lower()

        if out_key not in [x.lower() for x in output_field_names]:
            output_field_names.append(out_name)

        if src_key in field_defs and out_key not in output_field_defs:
            src_field = field_defs[src_key]
            output_field_defs[out_key] = QgsField(
                out_name,
                src_field.type(),
                src_field.typeName(),
                src_field.length(),
                src_field.precision()
            )

    geometry_string = QgsWkbTypes.displayString(wkb_type)
    memory_name = "tmp_%s_%s" % (source_value, uuid.uuid4().hex[:8])
    memory_uri = "%s?crs=%s" % (geometry_string, crs.authid())

    output_layer = QgsVectorLayer(memory_uri, memory_name, "memory")
    if not output_layer.isValid():
        raise Exception("Could not create the temporary memory layer.")

    provider = output_layer.dataProvider()
    provider.addAttributes(build_output_fields(output_field_names, output_field_defs, include_time_code=False))
    output_layer.updateFields()

    idx_label = output_layer.fields().indexFromName("label_txt")
    output_field_idx = {f: output_layer.fields().indexFromName(f) for f in output_field_names}

    new_features = []

    for layer, meta in source_layers:
        local_date_field = find_field_case_insensitive(layer, selected_date_field)
        if local_date_field is None:
            continue

        for feature in layer.getFeatures():
            if not feature_matches(layer, feature, selected_date_field, selected_year, selected_month):
                continue

            base_date_value = feature[local_date_field]
            year_ref, month_ref = parse_year_month_from_value(base_date_value)

            new_feature = QgsFeature(output_layer.fields())
            new_feature.setGeometry(feature.geometry())

            for target_field_name in output_field_names:
                source_field_name = get_source_field_name_for_export(source_value, target_field_name)
                source_field_name = find_field_case_insensitive(layer, source_field_name)

                if source_field_name is not None:
                    new_feature.setAttribute(output_field_idx[target_field_name], feature[source_field_name])
                else:
                    new_feature.setAttribute(output_field_idx[target_field_name], None)

            new_feature.setAttribute(
                idx_label,
                build_label_text(source_value, base_date_value, year_ref, month_ref)
            )

            new_features.append(new_feature)

    if not new_features:
        raise Exception("No features matched the selected filter.")

    provider.addFeatures(new_features)
    output_layer.updateExtents()
    return output_layer


def create_memory_output_layer_multi(
    selected_sources,
    selected_year,
    selected_month,
    selected_field_names,
    field_defs
):
    source_layers_all = []
    for source_value in selected_sources:
        source_layers_all.extend(get_layers_by_source(source_value))

    if not source_layers_all:
        raise Exception("No loaded layers were found for the selected sources.")

    wkb_type = detect_geometry_type(source_layers_all)
    crs = detect_crs(source_layers_all)

    if wkb_type is None or crs is None:
        raise Exception("Could not detect geometry type or CRS from source layers.")

    output_field_names = []
    output_field_defs = {}

    for selected_name in selected_field_names:
        if selected_name.lower() == "data0_p10":
            out_name = "Data0"
        elif selected_name.lower() == "data1_p90":
            out_name = "Data1"
        else:
            out_name = selected_name

        out_key = out_name.lower()
        src_key = selected_name.lower()

        if out_key not in [x.lower() for x in output_field_names]:
            output_field_names.append(out_name)

        if src_key in field_defs and out_key not in output_field_defs:
            src_field = field_defs[src_key]
            output_field_defs[out_key] = QgsField(
                out_name,
                src_field.type(),
                src_field.typeName(),
                src_field.length(),
                src_field.precision()
            )

    geometry_string = QgsWkbTypes.displayString(wkb_type)
    memory_name = "tmp_multi_%s" % uuid.uuid4().hex[:8]
    memory_uri = "%s?crs=%s" % (geometry_string, crs.authid())

    output_layer = QgsVectorLayer(memory_uri, memory_name, "memory")
    if not output_layer.isValid():
        raise Exception("Could not create the temporary memory layer.")

    provider = output_layer.dataProvider()
    provider.addAttributes(build_output_fields(output_field_names, output_field_defs, include_time_code=True))
    output_layer.updateFields()

    idx_label = output_layer.fields().indexFromName("label_txt")
    idx_time_code = output_layer.fields().indexFromName("time_code")
    output_field_idx = {f: output_layer.fields().indexFromName(f) for f in output_field_names}

    new_features = []

    for source_value in selected_sources:
        source_layers = get_layers_by_source(source_value)
        base_date_field = get_base_date_field_for_source(source_value)

        for layer, meta in source_layers:
            local_date_field = find_field_case_insensitive(layer, base_date_field)
            if local_date_field is None:
                continue

            for feature in layer.getFeatures():
                if not feature_matches(layer, feature, base_date_field, selected_year, selected_month):
                    continue

                base_date_value = feature[local_date_field]
                year_ref, month_ref = parse_year_month_from_value(base_date_value)
                time_code = build_time_code(year_ref, month_ref)

                new_feature = QgsFeature(output_layer.fields())
                new_feature.setGeometry(feature.geometry())

                for target_field_name in output_field_names:
                    source_field_name = get_source_field_name_for_export(source_value, target_field_name)
                    source_field_name = find_field_case_insensitive(layer, source_field_name)

                    if source_field_name is not None:
                        new_feature.setAttribute(output_field_idx[target_field_name], feature[source_field_name])
                    else:
                        new_feature.setAttribute(output_field_idx[target_field_name], None)

                new_feature.setAttribute(
                    idx_label,
                    build_label_text(source_value, base_date_value, year_ref, month_ref)
                )
                new_feature.setAttribute(idx_time_code, time_code)

                new_features.append(new_feature)

    if not new_features:
        raise Exception("No features matched the selected filter.")

    provider.addFeatures(new_features)
    output_layer.updateExtents()
    return output_layer


# ============================================================
# GUARDAR / CARGAR
# ============================================================

def save_layer_to_gpkg(layer, gpkg_path, layer_name):
    gpkg_path = ensure_output_gpkg(gpkg_path)

    options = QgsVectorFileWriter.SaveVectorOptions()
    options.driverName = "GPKG"
    options.layerName = layer_name
    options.fileEncoding = "UTF-8"

    if os.path.exists(gpkg_path):
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    else:
        options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteFile

    transform_context = QgsProject.instance().transformContext()

    result = QgsVectorFileWriter.writeAsVectorFormatV3(
        layer,
        gpkg_path,
        transform_context,
        options
    )

    error_code = result[0]
    error_message = result[1]

    if error_code != QgsVectorFileWriter.NoError:
        raise Exception("Error while saving GPKG: %s" % error_message)

    uri = "%s|layername=%s" % (gpkg_path, layer_name)
    loaded_layer = QgsVectorLayer(uri, layer_name, "ogr")

    if not loaded_layer.isValid():
        raise Exception("The layer was saved but could not be reloaded from the GPKG.")

    QgsProject.instance().addMapLayer(loaded_layer)
    return loaded_layer


# ============================================================
# SIMBOLOGÍA / LABELS
# ============================================================

def detect_symbol(layer):
    geometry_type = layer.geometryType()

    if geometry_type == QgsWkbTypes.PointGeometry:
        symbol = QgsSymbol.defaultSymbol(QgsWkbTypes.PointGeometry)
        symbol.setSize(2.5)
        return symbol

    if geometry_type == QgsWkbTypes.LineGeometry:
        symbol = QgsSymbol.defaultSymbol(QgsWkbTypes.LineGeometry)
        symbol.setWidth(0.9)
        return symbol

    symbol = QgsSymbol.defaultSymbol(QgsWkbTypes.PolygonGeometry)
    symbol.setOpacity(0.95)
    return symbol


def get_month_color(month_ref):
    if month_ref is None:
        return QColor(DEFAULT_COLOR)

    month_ref = int(month_ref)
    if month_ref in MONTH_COLORS:
        return QColor(MONTH_COLORS[month_ref])

    return QColor(DEFAULT_COLOR)


def apply_time_symbology(layer, visual_base_field):
    local_date_field = find_field_case_insensitive(layer, visual_base_field)
    if local_date_field is None:
        return

    unique_values = {}

    for feature in layer.getFeatures():
        value = feature[local_date_field]
        year_ref, month_ref = parse_year_month_from_value(value)
        time_code = build_time_code(year_ref, month_ref)

        if time_code is None:
            continue

        if time_code not in unique_values:
            unique_values[time_code] = {
                "month_ref": month_ref,
                "label": time_code,
            }

    categories = []

    for key in sorted(unique_values.keys()):
        info = unique_values[key]
        symbol = detect_symbol(layer)
        symbol.setColor(get_month_color(info["month_ref"]))
        categories.append(QgsRendererCategory(key, symbol, info["label"]))

    if not categories:
        symbol = detect_symbol(layer)
        symbol.setColor(QColor(DEFAULT_COLOR))
        categories.append(QgsRendererCategory("", symbol, "No date"))

    expr = "replace(substr(to_string(\"%s\"), 1, 7), '/', '-')" % local_date_field

    renderer = QgsCategorizedSymbolRenderer(expr, categories)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def apply_time_symbology_multi(layer):
    local_time_field = find_field_case_insensitive(layer, "time_code")
    if local_time_field is None:
        return

    unique_values = {}

    for feature in layer.getFeatures():
        value = feature[local_time_field]
        if qvariant_is_null(value):
            continue

        text = str(value).strip()
        year_ref, month_ref = parse_year_month_from_value(text)

        if text not in unique_values:
            unique_values[text] = {
                "month_ref": month_ref,
                "label": text,
            }

    categories = []

    for key in sorted(unique_values.keys()):
        info = unique_values[key]
        symbol = detect_symbol(layer)
        symbol.setColor(get_month_color(info["month_ref"]))
        categories.append(QgsRendererCategory(key, symbol, info["label"]))

    if not categories:
        symbol = detect_symbol(layer)
        symbol.setColor(QColor(DEFAULT_COLOR))
        categories.append(QgsRendererCategory("", symbol, "No date"))

    expr = "\"%s\"" % local_time_field

    renderer = QgsCategorizedSymbolRenderer(expr, categories)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def apply_simple_labels(layer):
    if layer.fields().indexFromName("label_txt") == -1:
        return

    settings = QgsPalLayerSettings()
    settings.fieldName = "label_txt"
    settings.isExpression = False

    text_format = QgsTextFormat()
    text_format.setFont(QFont("Arial", 9))
    text_format.setSize(9)

    buffer_settings = QgsTextBufferSettings()
    buffer_settings.setEnabled(True)
    buffer_settings.setSize(0.8)
    buffer_settings.setColor(QColor("white"))
    text_format.setBuffer(buffer_settings)

    settings.setFormat(text_format)

    geometry_type = layer.geometryType()
    if geometry_type == QgsWkbTypes.PointGeometry:
        settings.placement = Qgis.LabelPlacement.OverPoint
    elif geometry_type == QgsWkbTypes.LineGeometry:
        settings.placement = Qgis.LabelPlacement.Line
    else:
        settings.placement = Qgis.LabelPlacement.OverPoint

    layer.setLabelsEnabled(True)
    layer.setLabeling(QgsVectorLayerSimpleLabeling(settings))
    layer.triggerRepaint()


# ============================================================
# MAIN DIALOG
# ============================================================

class ExportTemporalLayerDialog(QDialog):
    def __init__(self, parent=None):
        QDialog.__init__(self, parent)
        self.setWindowTitle("Export filtered temporal layer")
        self.setMinimumWidth(640)

        main_layout = QVBoxLayout()
        form_layout = QFormLayout()

        self.all_sources_check = QCheckBox("All sources")
        self.all_sources_check.toggled.connect(self.toggle_all_sources)

        source_group = QGroupBox("Sources")
        source_group_layout = QVBoxLayout()

        self.source_checks = {}
        for source_name in SOURCE_OPTIONS:
            cb = QCheckBox(source_name)
            cb.toggled.connect(self.update_year_options)
            self.source_checks[source_name] = cb
            source_group_layout.addWidget(cb)

        source_group.setLayout(source_group_layout)

        self.year_combo = QComboBox()
        self.year_combo.currentTextChanged.connect(self.update_month_options)

        self.month_combo = QComboBox()

        self.output_path_edit = QLineEdit()
        self.output_name_edit = QLineEdit()

        browse_button = QPushButton("Browse…")
        browse_button.clicked.connect(self.pick_output_path)

        path_layout = QHBoxLayout()
        path_layout.addWidget(self.output_path_edit)
        path_layout.addWidget(browse_button)

        self.labels_check = QCheckBox("Apply labels")
        self.labels_check.setChecked(True)

        self.style_check = QCheckBox("Apply symbology")
        self.style_check.setChecked(True)

        form_layout.addRow("", self.all_sources_check)
        form_layout.addRow("", source_group)
        form_layout.addRow("Year:", self.year_combo)
        form_layout.addRow("Month:", self.month_combo)
        form_layout.addRow("Output GPKG:", path_layout)
        form_layout.addRow("Output layer name:", self.output_name_edit)
        form_layout.addRow("", self.labels_check)
        form_layout.addRow("", self.style_check)

        main_layout.addLayout(form_layout)

        info_label = QLabel(
            "If one source is selected, it is exported normally. "
            "If two or more sources are selected, they are merged into one single output layer. "
            "Shared fields are selected by default, and extra fields are grouped by source."
        )
        info_label.setWordWrap(True)
        main_layout.addWidget(info_label)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.run_export)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)

        self.setLayout(main_layout)
        self.update_year_options()

    def get_selected_sources(self):
        return [src for src in SOURCE_OPTIONS if self.source_checks[src].isChecked()]

    def toggle_all_sources(self, checked):
        for source_name in SOURCE_OPTIONS:
            self.source_checks[source_name].blockSignals(True)
            self.source_checks[source_name].setChecked(checked)
            self.source_checks[source_name].blockSignals(False)
        self.update_year_options()

    def update_year_options(self):
        selected_sources = self.get_selected_sources()

        self.year_combo.blockSignals(True)
        self.year_combo.clear()

        if not selected_sources:
            self.year_combo.blockSignals(False)
            self.update_month_options()
            return

        available_years = collect_available_years_multi(selected_sources)

        if available_years:
            self.year_combo.addItem("All")
            self.year_combo.addItems([str(y) for y in available_years])

        self.year_combo.blockSignals(False)

        if self.year_combo.count() > 0:
            self.year_combo.setCurrentIndex(0)

        self.update_month_options()

    def update_month_options(self):
        selected_sources = self.get_selected_sources()
        year_text = self.year_combo.currentText().strip()

        self.month_combo.blockSignals(True)
        self.month_combo.clear()

        if not selected_sources:
            self.month_combo.blockSignals(False)
            return

        if year_text == "" or year_text == "All":
            self.month_combo.addItem("All")
            self.month_combo.setCurrentIndex(0)
            self.month_combo.blockSignals(False)
            return

        selected_year = int(year_text)
        available_months = collect_available_months_multi(selected_sources, selected_year)

        self.month_combo.addItem("All")
        for m in available_months:
            self.month_combo.addItem("%02d" % int(m))

        self.month_combo.setCurrentIndex(0)
        self.month_combo.blockSignals(False)

    def pick_output_path(self):
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save output GeoPackage",
            "",
            "GeoPackage (*.gpkg)"
        )
        if path:
            self.output_path_edit.setText(path)

    def run_export(self):
        selected_sources = self.get_selected_sources()

        if not selected_sources:
            QMessageBox.warning(self, "Warning", "You must select at least one source.")
            return

        year_text = self.year_combo.currentText().strip()
        if year_text == "":
            QMessageBox.warning(self, "Warning", "No available years were found.")
            return

        selected_year = None if year_text == "All" else int(year_text)
        month_text = self.month_combo.currentText().strip()
        selected_month = None if month_text == "All" else int(month_text)

        output_path = self.output_path_edit.text().strip()
        output_layer_name = self.output_name_edit.text().strip()

        if not output_path:
            QMessageBox.warning(self, "Warning", "You must choose an output GPKG path.")
            return

        if not output_layer_name:
            source_tag = "_".join(selected_sources)
            if selected_year is None:
                output_layer_name = "%s_all_years" % source_tag
            elif selected_month is None:
                output_layer_name = "%s_%s" % (source_tag, selected_year)
            else:
                output_layer_name = "%s_%s_%02d" % (source_tag, selected_year, selected_month)

            self.output_name_edit.setText(output_layer_name)

        try:
            # ----------------------------------------------------
            # UNA SOLA FUENTE
            # ----------------------------------------------------
            if len(selected_sources) == 1:
                source_value = selected_sources[0]
                source_layers = get_layers_by_source(source_value)

                if not source_layers:
                    raise Exception("No loaded layers were found for source %s." % source_value)

                available_fields, field_defs, actual_name_map = get_available_fields(source_layers)
                default_checked = get_default_checked_fields(source_value, available_fields)
                visual_base_field = get_base_date_field_for_source(source_value)

                field_dialog = MultiSourceFieldSelectionDialog(
                    selected_sources=[source_value],
                    shared_fields=available_fields,
                    extra_fields_by_source={},
                    default_checked_shared=default_checked,
                    default_checked_extra_by_source={},
                    required_shared_fields=[visual_base_field],
                    parent=self
                )

                if field_dialog.exec_() != QDialog.Accepted:
                    return

                selected_field_names = field_dialog.get_selected_fields()

                memory_layer = create_memory_output_layer_single(
                    source_layers=source_layers,
                    source_value=source_value,
                    selected_date_field=visual_base_field,
                    selected_year=selected_year,
                    selected_month=selected_month,
                    selected_field_names=selected_field_names,
                    field_defs=field_defs
                )

                saved_layer = save_layer_to_gpkg(
                    layer=memory_layer,
                    gpkg_path=output_path,
                    layer_name=output_layer_name,
                )

                saved_visual_base_field = get_visual_date_field_for_saved_layer(
                    source_value,
                    visual_base_field
                )

                if self.style_check.isChecked():
                    apply_time_symbology(saved_layer, saved_visual_base_field)

                if self.labels_check.isChecked():
                    apply_simple_labels(saved_layer)

            # ----------------------------------------------------
            # MULTI-FUENTE -> UNA SOLA CAPA
            # ----------------------------------------------------
            else:
                fields_by_source, field_defs_union, presence_map, actual_name_map_union = \
                    get_available_fields_by_sources(selected_sources)

                shared_fields = get_shared_fields(
                    selected_sources,
                    presence_map,
                    actual_name_map_union
                )

                if not shared_fields:
                    raise Exception("There are no shared fields across the selected sources.")

                extra_fields_by_source = get_extra_fields_by_source(
                    selected_sources,
                    presence_map,
                    actual_name_map_union
                )

                default_checked_shared, default_checked_extra_by_source = \
                    get_default_checked_fields_for_multi(
                        selected_sources,
                        shared_fields,
                        fields_by_source
                    )

                required_shared_fields = []
                shared_lc = {x.lower(): x for x in shared_fields}
                for src in selected_sources:
                    req = get_base_date_field_for_source(src).lower()
                    if req in shared_lc:
                        required_shared_fields.append(shared_lc[req])

                field_dialog = MultiSourceFieldSelectionDialog(
                    selected_sources=selected_sources,
                    shared_fields=shared_fields,
                    extra_fields_by_source=extra_fields_by_source,
                    default_checked_shared=default_checked_shared,
                    default_checked_extra_by_source=default_checked_extra_by_source,
                    required_shared_fields=required_shared_fields,
                    parent=self
                )

                if field_dialog.exec_() != QDialog.Accepted:
                    return

                selected_field_names = field_dialog.get_selected_fields()

                memory_layer = create_memory_output_layer_multi(
                    selected_sources=selected_sources,
                    selected_year=selected_year,
                    selected_month=selected_month,
                    selected_field_names=selected_field_names,
                    field_defs=field_defs_union
                )

                saved_layer = save_layer_to_gpkg(
                    layer=memory_layer,
                    gpkg_path=output_path,
                    layer_name=output_layer_name,
                )

                if self.style_check.isChecked():
                    apply_time_symbology_multi(saved_layer)

                if self.labels_check.isChecked():
                    apply_simple_labels(saved_layer)

            if iface is not None:
                iface.setActiveLayer(saved_layer)
                iface.mapCanvas().setExtent(saved_layer.extent())
                iface.mapCanvas().refresh()

            QMessageBox.information(
                self,
                "Success",
                "The filtered layer was saved and loaded successfully:\n%s" % output_layer_name
            )
            self.accept()

        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))


# ============================================================
# RUN
# ============================================================

dialog = ExportTemporalLayerDialog(iface.mainWindow() if iface is not None else None)
dialog.show()

#