Example submission to past into terminal while in the directory scripts/pybacdm/cpca070342024_shared/predict_pipeline/distribute

./submit_tile.sh \
	TILE_ID=T29TPE \
	TILE_HDF5_PATH=/users1/cpca070342024/shared/vegetation_loss/scripts/pybacdm/cpca070342024_shared/predict_pipeline/small_test_area/T29TPE_testblock.h5 \
	START_DATE=2023-02-01 \
	END_DATE=2023-10-01 \
	OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/12_Testing_previous_commit_head \
	MAX_COMPOSITE_DAYS=45 \
	BLOCK_ROWS=1-2 \
	BLOCK_COLS=1-2 \
	WRITE_COMPOSITE_TIFS=1 \
	MODEL=efficientnet_b2_16bit_pipeline \
	DATA_DTYPE=u16
