from __future__ import annotations

import os
import re

from qgis.PyQt.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QListWidget,
    QPushButton,
    QMessageBox,
    QLabel,
    QHBoxLayout,
)
from qgis.core import (
    QgsProject,
    QgsVectorLayer,
    QgsRasterLayer,
    QgsMultiBandColorRenderer,
    QgsContrastEnhancement,
    QgsRasterBandStats,
    QgsFillSymbol,
    QgsSingleSymbolRenderer,
)
from qgis.utils import iface


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = r"C:\Users\jesus\OneDrive\Ejercicio 7\Documents\Work\PT\Harmonizacion_datos\chips_data\vchips\v_chips_v1_10bands"

MASK_POLYGONS_DIR = os.path.join(BASE_DIR, "mask_polygons")
SOURCE_RASTERS_10B_DIR = os.path.join(BASE_DIR, "source_rasters")

REVIEW_DIR = os.path.join(BASE_DIR, "Revisado_10_bandas_pngs")
REVIEW_MASK_POLYGONS_DIR = os.path.join(REVIEW_DIR, "mask_polygons")

MASK_GPKG_REGEX = re.compile(
    r"^(vchip_\d+_\d+_\d{8}_mask)\.gpkg$",
    re.IGNORECASE
)

# Raster 10 bandas
TEN_BAND_RED = 4
TEN_BAND_GREEN = 8
TEN_BAND_BLUE = 9

# Estilo de máscara
MASK_COLOR = "#1f78b4"
MASK_OUTLINE_WIDTH = 0.66

REMOVE_PREVIOUS_LOADED_REVIEW_LAYERS = True

# Si True: solo muestra chips que aún NO tienen versión revisada guardada
# Si False: muestra todos con estado [PENDIENTE] / [REVISADO]
SHOW_ONLY_PENDING = False


# ============================================================
# HELPERS
# ============================================================

def safe_exists(path: str) -> bool:
    return os.path.exists(path)


def list_mask_gpkgs(mask_dir: str, review_mask_dir: str) -> list[dict]:
    items = []

    if not os.path.isdir(mask_dir):
        raise Exception(f"No existe la carpeta de máscaras: {mask_dir}")

    review_exists = os.path.isdir(review_mask_dir)

    for fname in sorted(os.listdir(mask_dir)):
        m = MASK_GPKG_REGEX.match(fname)
        if not m:
            continue

        mask_base = m.group(1)      # vchip_X_Y_DATE_mask
        chip_base = mask_base[:-5]  # vchip_X_Y_DATE

        original_gpkg = os.path.join(mask_dir, fname)
        reviewed_gpkg = os.path.join(review_mask_dir, fname)

        is_reviewed = review_exists and safe_exists(reviewed_gpkg)

        if SHOW_ONLY_PENDING and is_reviewed:
            continue

        load_gpkg = reviewed_gpkg if is_reviewed else original_gpkg
        status = "--REVISADO--" if is_reviewed else "PENDIENTE"
        label = f"[{status}] {chip_base}"

        items.append({
            "label": label,
            "chip_base": chip_base,
            "mask_base": mask_base,
            "mask_gpkg": load_gpkg,
            "mask_layer_name": mask_base,
            "is_reviewed": is_reviewed,
            "original_gpkg": original_gpkg,
            "reviewed_gpkg": reviewed_gpkg,
        })

    return items


def first_existing_path(paths: list[str]) -> str | None:
    for path in paths:
        if safe_exists(path):
            return path

    return None


def build_expected_paths(chip_base: str) -> dict:
    """
    Busca los rasters 10B aceptando ambas convenciones de nombre.

    Forma limpia:
        vchip_X_Y_DATE_before.tif
        vchip_X_Y_DATE_after.tif

    Forma anterior:
        vchip_X_Y_DATE_mask_before.tif
        vchip_X_Y_DATE_mask_after.tif
    """

    before_candidates = [
        os.path.join(SOURCE_RASTERS_10B_DIR, f"{chip_base}_before.tif"),
        os.path.join(SOURCE_RASTERS_10B_DIR, f"{chip_base}_mask_before.tif"),
    ]

    after_candidates = [
        os.path.join(SOURCE_RASTERS_10B_DIR, f"{chip_base}_after.tif"),
        os.path.join(SOURCE_RASTERS_10B_DIR, f"{chip_base}_mask_after.tif"),
    ]

    ten_before = first_existing_path(before_candidates)
    ten_after = first_existing_path(after_candidates)

    return {
        "ten_before": ten_before,
        "ten_after": ten_after,
        "ten_before_candidates": before_candidates,
        "ten_after_candidates": after_candidates,
    }


def remove_previous_loaded_review_layers() -> None:
    if not REMOVE_PREVIOUS_LOADED_REVIEW_LAYERS:
        return

    project = QgsProject.instance()
    to_remove = []

    for lyr in project.mapLayers().values():
        if lyr.name().startswith("REV_"):
            to_remove.append(lyr.id())

    if to_remove:
        project.removeMapLayers(to_remove)


def load_vector_layer(
    gpkg_path: str,
    layer_name: str,
    qgis_name: str
) -> QgsVectorLayer:
    uri = f"{gpkg_path}|layername={layer_name}"
    lyr = QgsVectorLayer(uri, qgis_name, "ogr")

    if not lyr.isValid():
        raise Exception(f"No se pudo cargar el vector: {uri}")

    QgsProject.instance().addMapLayer(lyr)
    return lyr


def load_raster_layer(
    raster_path: str,
    qgis_name: str,
    add_to_project: bool = True
) -> QgsRasterLayer:
    lyr = QgsRasterLayer(raster_path, qgis_name)

    if not lyr.isValid():
        raise Exception(f"No se pudo cargar el raster: {raster_path}")

    if add_to_project:
        QgsProject.instance().addMapLayer(lyr)

    return lyr


def zoom_to_layer(layer) -> None:
    try:
        iface.setActiveLayer(layer)
        iface.zoomToActiveLayer()
    except Exception:
        pass


def get_band_min_max(layer: QgsRasterLayer, band: int) -> tuple[float, float]:
    provider = layer.dataProvider()

    stats = provider.bandStatistics(
        band,
        QgsRasterBandStats.Min | QgsRasterBandStats.Max,
        layer.extent(),
        0
    )

    min_value = float(stats.minimumValue)
    max_value = float(stats.maximumValue)

    if min_value == max_value:
        max_value = min_value + 1.0

    return min_value, max_value


def make_contrast_enhancement(
    layer: QgsRasterLayer,
    band: int
) -> QgsContrastEnhancement:
    provider = layer.dataProvider()

    min_value, max_value = get_band_min_max(layer, band)

    ce = QgsContrastEnhancement(provider.dataType(band))
    ce.setMinimumValue(min_value)
    ce.setMaximumValue(max_value)
    ce.setContrastEnhancementAlgorithm(
        QgsContrastEnhancement.StretchToMinimumMaximum,
        True
    )

    return ce


def apply_10band_rgb_style(layer: QgsRasterLayer) -> None:
    if not layer.isValid():
        raise Exception(f"Raster 10 bandas inválido: {layer.name()}")

    provider = layer.dataProvider()
    band_count = provider.bandCount()

    required_bands = [
        TEN_BAND_RED,
        TEN_BAND_GREEN,
        TEN_BAND_BLUE,
    ]

    for band in required_bands:
        if band > band_count:
            raise Exception(
                f"El raster {layer.name()} tiene {band_count} bandas, "
                f"pero se solicitó la banda {band}."
            )

    renderer = QgsMultiBandColorRenderer(
        provider,
        TEN_BAND_RED,
        TEN_BAND_GREEN,
        TEN_BAND_BLUE,
    )

    renderer.setRedContrastEnhancement(
        make_contrast_enhancement(layer, TEN_BAND_RED)
    )
    renderer.setGreenContrastEnhancement(
        make_contrast_enhancement(layer, TEN_BAND_GREEN)
    )
    renderer.setBlueContrastEnhancement(
        make_contrast_enhancement(layer, TEN_BAND_BLUE)
    )

    layer.setRenderer(renderer)
    layer.triggerRepaint()


def apply_mask_outline_style(layer: QgsVectorLayer) -> None:
    if not layer.isValid():
        raise Exception(f"Capa de máscara inválida: {layer.name()}")

    symbol = QgsFillSymbol.createSimple({
        "style": "no",
        "outline_style": "solid",
        "outline_color": MASK_COLOR,
        "outline_width": str(MASK_OUTLINE_WIDTH),
        "outline_width_unit": "MM",
    })

    renderer = QgsSingleSymbolRenderer(symbol)
    layer.setRenderer(renderer)
    layer.triggerRepaint()


def format_candidate_list(paths: list[str]) -> str:
    return "\n".join(f"- {path}" for path in paths)


def load_chip_for_review(chip_item: dict) -> None:
    chip_base = chip_item["chip_base"]
    mask_gpkg = chip_item["mask_gpkg"]
    mask_layer_name = chip_item["mask_layer_name"]

    expected = build_expected_paths(chip_base)

    missing_required = []

    if expected["ten_before"] is None:
        missing_required.append(
            "No se encontró raster BEFORE. Se buscaron estas opciones:\n"
            + format_candidate_list(expected["ten_before_candidates"])
        )

    if expected["ten_after"] is None:
        missing_required.append(
            "No se encontró raster AFTER. Se buscaron estas opciones:\n"
            + format_candidate_list(expected["ten_after_candidates"])
        )

    if not safe_exists(mask_gpkg):
        missing_required.append(f"No se encontró mask_gpkg: {mask_gpkg}")

    if missing_required:
        raise Exception(
            "Faltan archivos requeridos:\n\n" + "\n\n".join(missing_required)
        )

    remove_previous_loaded_review_layers()

    # ========================================================
    # Orden de carga
    # ========================================================
    # QGIS coloca arriba la última capa cargada.
    #
    # Cargamos:
    # 1) after
    # 2) before
    # 3) mask
    #
    # Resultado visual:
    # - mask polygon
    # - before 10 bandas
    # - after 10 bandas
    # ========================================================

    ten_after_layer = load_raster_layer(
        expected["ten_after"],
        f"REV_{chip_base}_after_10b",
        add_to_project=True
    )

    ten_before_layer = load_raster_layer(
        expected["ten_before"],
        f"REV_{chip_base}_before_10b",
        add_to_project=True
    )

    mask_layer = load_vector_layer(
        gpkg_path=mask_gpkg,
        layer_name=mask_layer_name,
        qgis_name=f"REV_{chip_base}_mask_polygon"
    )

    apply_10band_rgb_style(ten_after_layer)
    apply_10band_rgb_style(ten_before_layer)
    apply_mask_outline_style(mask_layer)

    iface.setActiveLayer(mask_layer)
    zoom_to_layer(mask_layer)

    origen = "revisada" if chip_item["is_reviewed"] else "original"

    msg = (
        f"Chip cargado: {chip_base}\n"
        f"Máscara cargada desde versión: {origen}\n"
        f"Capas cargadas:\n"
        f"- mask polygon\n"
        f"- before 10 bandas\n"
        f"- after 10 bandas\n\n"
        f"Raster before usado:\n{expected['ten_before']}\n\n"
        f"Raster after usado:\n{expected['ten_after']}"
    )

    iface.messageBar().pushSuccess("Revisión 10 bandas", msg)


# ============================================================
# DIALOG
# ============================================================

class ChipReviewDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("Seleccionar mask para revisión (10 bandas)")
        self.resize(620, 560)

        self.chip_items: list[dict] = []
        self.item_map: dict[str, dict] = {}

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.info_label = QLabel("")
        self.layout.addWidget(self.info_label)

        self.list_widget = QListWidget()
        self.layout.addWidget(self.list_widget)

        btn_row = QHBoxLayout()
        self.layout.addLayout(btn_row)

        self.load_btn = QPushButton("Cargar seleccionado")
        self.refresh_btn = QPushButton("Refrescar lista")
        self.close_btn = QPushButton("Cerrar")

        btn_row.addWidget(self.load_btn)
        btn_row.addWidget(self.refresh_btn)
        btn_row.addWidget(self.close_btn)

        self.load_btn.clicked.connect(self.load_selected)
        self.refresh_btn.clicked.connect(self.populate)
        self.close_btn.clicked.connect(self.close)

        self.populate()

    def populate(self) -> None:
        self.chip_items = list_mask_gpkgs(
            MASK_POLYGONS_DIR,
            REVIEW_MASK_POLYGONS_DIR
        )

        self.item_map = {
            item["label"]: item
            for item in self.chip_items
        }

        self.list_widget.clear()

        for item in self.chip_items:
            self.list_widget.addItem(item["label"])

        total = len(self.chip_items)
        reviewed = sum(1 for x in self.chip_items if x["is_reviewed"])
        pending = total - reviewed

        if SHOW_ONLY_PENDING:
            self.info_label.setText(
                f"Pendientes visibles: {total}"
            )
        else:
            self.info_label.setText(
                f"Total visibles: {total} | Pendientes: {pending} | Revisados: {reviewed}"
            )

    def load_selected(self) -> None:
        current = self.list_widget.currentItem()

        if current is None:
            QMessageBox.warning(
                self,
                "Aviso",
                "Selecciona un chip primero."
            )
            return

        label = current.text()
        chip_item = self.item_map.get(label)

        if chip_item is None:
            QMessageBox.critical(
                self,
                "Error",
                f"No se encontró información para: {label}"
            )
            return

        try:
            load_chip_for_review(chip_item)

            row = self.list_widget.row(current)
            self.list_widget.takeItem(row)

            total = self.list_widget.count()
            self.info_label.setText(
                f"Pendientes visibles en esta sesión: {total}"
            )

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error al cargar",
                str(e)
            )


# ============================================================
# MAIN
# ============================================================

dlg = ChipReviewDialog(iface.mainWindow())
dlg.show()