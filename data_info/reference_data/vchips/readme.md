# Vchips (visual chips)

**vchips** are annotated visual chips of approximately **4 × 4 km**, designed as paired samples for change analysis. Each vchip is associated with a location and a reference date **D**, and contains three aligned files:

1. **before**: multiband GeoTIFF file (`int16`) representing a temporal composite built from imagery before date **D**.
2. **after**: same as **before**, but built from imagery after date **D**.
3. **mask**: single-band GeoTIFF (`int8`) representing the visually interpreted change classes between the **before** and **after** composites.

All three files share the same extent, pixel size, and grid.

## Data values

### before / after
- Data type: `int16`
- Content: reflectance values


### mask
- Data type: `int8`
- Classes:
  - **0** = nao_alteracao
  - **1** = corte
  - **2** = outro
  - **3** = agricultura
  - **4** = fogo

## Naming convention

The file naming scheme is:

- `vchip_X_Y_YYYYMMDD_before`
- `vchip_X_Y_YYYYMMDD_after`
- `vchip_X_Y_YYYYMMDD_mask`

where:

- **X** and **Y** are the integer centroid coordinates of the chip raster in the working CRS (**EPSG:32629**).
- **YYYYMMDD** is the chip reference date **D**.
- **before** and **after** identify the temporal composite.
- **mask** identifies the annotation mask for that chip.

The mask is produced only once per chip, since it is identical for the corresponding **before** and **after** pair.

## Additional remarks

- The reference date **D** is an organizing date for the chip pair and does not necessarily correspond to the acquisition date of a single source image.
- A given vchip is defined by the combination of location and reference date.
- The three files belonging to the same vchip should always be handled as a set.

Examples are available at `\partilhado\vchips`.

The dataset is organized into three folders: one containing the masks in **GPKG** format, one containing the masks in raster format (**GeoTIFF**), and one containing the **before** and **after** composites.


Example: for 2020 reference data (94 vchips)

    ========================================
          MASK VALUE DISTRIBUTION
    ========================================
    Value   0:   13,330,366 pixels ( 91.55%)
    Value   1:       33,228 pixels (  0.23%)
    Value   2:          496 pixels (  0.00%)
    Value   3:        3,115 pixels (  0.02%)
    Value   4:    1,192,792 pixels (  8.19%)
    Value 255:            3 pixels (  0.00%) <-- ALERT: INVALID DATA
    ========================================
    
