import numpy as np

_NODATA_16BIT = 65535
_NODATA_8BIT  = 255


def _to_uint8(arr):
    """Convert (H, W, C) uint16 array to uint8 via per-band q0.02–q0.98 stretch.

    NoData (65535) maps to 255.  Returns arr unchanged if already uint8.
    """
    if arr.dtype == np.uint8:
        return arr
    arr = arr.astype(np.float32)
    nodata = (arr == _NODATA_16BIT)
    arr[nodata] = np.nan
    out = np.empty(arr.shape, dtype=np.uint8)
    for b in range(arr.shape[2]):
        band, nd = arr[:, :, b], nodata[:, :, b]
        q02, q98 = np.nanpercentile(band, [2, 98])
        denom = float(q98 - q02) if q98 > q02 else 1.0
        scaled = np.clip((band - q02) / denom * (_NODATA_8BIT - 1), 0, _NODATA_8BIT - 1)
        scaled[nd] = _NODATA_8BIT
        out[:, :, b] = scaled.astype(np.uint8)
    return out
