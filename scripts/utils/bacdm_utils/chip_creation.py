import numpy as np


def select_temporal_indices(all_ordinals, break_ordinal, window_days, max_images):
    """
    Select temporal indices for pre and post break periods.

    Returns:
    --------
    tuple of (pre_indices, post_indices, pre_ordinals, post_ordinals)
    """
    # Pre-break selection
    pre_mask = (all_ordinals <= break_ordinal) & (all_ordinals >= break_ordinal - window_days)
    pre_indices = np.where(pre_mask)[0]
    pre_ordinals = all_ordinals[pre_indices]

    # Sort by date descending and take max_images
    sorted_idx = np.argsort(pre_ordinals)[::-1][:max_images]
    pre_indices = pre_indices[sorted_idx]
    pre_ordinals = pre_ordinals[sorted_idx]

    # Post-break selection
    post_mask = (all_ordinals > break_ordinal) & (all_ordinals <= break_ordinal + window_days)
    post_indices = np.where(post_mask)[0]
    post_ordinals = all_ordinals[post_indices]

    # Sort by date ascending and take max_images
    sorted_idx = np.argsort(post_ordinals)[:max_images]
    post_indices = post_indices[sorted_idx]
    post_ordinals = post_ordinals[sorted_idx]

    if len(pre_indices) == 0 or len(post_indices) == 0:
        return None, None, None, None

    return pre_indices, post_indices, pre_ordinals, post_ordinals


def cascading_selection_optimized(pre_data, post_data, pre_ordinals, post_ordinals,
                                  selection_band_idx, s2_nodata, output_nodata):
    """
    Optimized cascading selection working directly with numpy arrays.

    Parameters:
    -----------
    pre_data : ndarray
        Shape (n_pre_timesteps, n_bands, height, width)
    post_data : ndarray
        Shape (n_post_timesteps, n_bands, height, width)
    pre_ordinals : ndarray
        Ordinal dates for pre-break timesteps
    post_ordinals : ndarray
        Ordinal dates for post-break timesteps
    selection_band_idx : int
        Band index to use for selection

    Returns:
    --------
    tuple of (pre_selected, post_selected, pre_timestamps, post_timestamps)
        Each 'selected' is shape (n_bands, height, width)
        Each 'timestamps' is shape (height, width) with ordinal dates
    """
    n_bands, height, width = pre_data.shape[1], pre_data.shape[2], pre_data.shape[3]

    # Extract selection band
    pre_selection_band = pre_data[:, selection_band_idx, :, :]  # (n_pre, h, w)
    post_selection_band = post_data[:, selection_band_idx, :, :]  # (n_post, h, w)

    # Find first valid timestep for each pixel (cascading)
    pre_valid_mask = pre_selection_band < s2_nodata  # (n_pre, h, w)
    pre_first_valid_idx = pre_valid_mask.argmax(axis=0)  # (h, w)
    pre_any_valid = pre_valid_mask.any(axis=0)  # (h, w)

    post_valid_mask = post_selection_band < s2_nodata
    post_first_valid_idx = post_valid_mask.argmax(axis=0)
    post_any_valid = post_valid_mask.any(axis=0)

    # Create output arrays
    pre_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    post_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    pre_timestamps = np.full((height, width), output_nodata, dtype=np.int64)
    post_timestamps = np.full((height, width), output_nodata, dtype=np.int64)

    # Gather data using advanced indexing
    # Create meshgrid for row and column indices
    row_indices, col_indices = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')

    for band_idx in range(n_bands):
        # For each pixel, select the value from its first valid timestep
        pre_selected[band_idx] = pre_data[pre_first_valid_idx, band_idx, row_indices, col_indices]
        post_selected[band_idx] = post_data[post_first_valid_idx, band_idx, row_indices, col_indices]

    # Get timestamps
    pre_timestamps[:] = pre_ordinals[pre_first_valid_idx]
    post_timestamps[:] = post_ordinals[post_first_valid_idx]

    # Apply validity mask
    pre_selected[:, ~pre_any_valid] = output_nodata
    post_selected[:, ~post_any_valid] = output_nodata
    pre_timestamps[~pre_any_valid] = output_nodata
    post_timestamps[~post_any_valid] = output_nodata

    return pre_selected, post_selected, pre_timestamps, post_timestamps


def ordinal_to_unix_timestamp(ordinal_array, ordinal_to_unix_map, output_nodata=0):
    """Convert array of ordinal dates to Unix timestamps in milliseconds."""
    result = np.full_like(ordinal_array, output_nodata, dtype=np.int64)
    unique_ordinals = np.unique(ordinal_array)
    unique_ordinals = unique_ordinals[unique_ordinals != output_nodata]

    for ordinal in unique_ordinals:
        if ordinal in ordinal_to_unix_map:
            mask = ordinal_array == ordinal
            result[mask] = ordinal_to_unix_map[ordinal]

    return result