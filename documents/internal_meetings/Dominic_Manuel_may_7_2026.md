# Addressed issues:

(+) are issues that need to be discussed with DGT/LIP

## hdf5 files with duplicate dates (+)

Actions: 
  - revise create_hdf5.py script to prevent duplicates and force ordered dates in hdf5 creation
  - Identify how many duplicates there are in each tile (+)

    Source: `/users1/dgt/hdf5` (17 files, 27,080 total timesteps, 4,609 duplicate entries; all files have duplicates)

    | tile_id | total_timesteps | unique_dates | duplicate_groups | duplicate_entries | most_dup_date | most_dup_count |
    | --- | ---: | ---: | ---: | ---: | --- | ---: |
    | T29TPG | 2232 | 1858 | 368 | 374 | 2022-10-21 | 4 |
    | T29SMD | 1590 | 1288 | 293 | 302 | 2022-09-09 | 4 |
    | T29SND | 1590 | 1288 | 293 | 302 | 2022-02-08 | 4 |
    | T29SMC | 1557 | 1288 | 261 | 269 | 2022-09-09 | 4 |
    | T29SPD | 1555 | 1289 | 257 | 266 | 2022-02-08 | 4 |
    | T29TNF | 1552 | 1287 | 252 | 265 | 2022-03-18 | 4 |
    | T29TQF | 1549 | 1284 | 253 | 265 | 2022-02-05 | 4 |
    | T29SPC | 1553 | 1289 | 253 | 264 | 2022-01-26 | 4 |
    | T29TQG | 1547 | 1284 | 252 | 263 | 2022-09-18 | 4 |
    | T29TPE | 1549 | 1288 | 254 | 261 | 2022-02-08 | 4 |
    | T29TPF | 1544 | 1284 | 249 | 260 | 2022-02-05 | 4 |
    | T29TME | 1546 | 1288 | 250 | 258 | 2022-02-08 | 4 |
    | T29SPB | 1546 | 1289 | 244 | 257 | 2022-11-22 | 4 |
    | T29TNE | 1559 | 1304 | 248 | 255 | 2022-02-08 | 4 |
    | T29SNC | 1538 | 1288 | 244 | 250 | 2023-02-03 | 4 |
    | T29TNG | 1537 | 1287 | 245 | 250 | 2022-10-21 | 4 |
    | T29SNB | 1536 | 1288 | 235 | 248 | 2022-11-15 | 4 |

## Determine current RAM memory use per CPU

Solution: ask LIP (Gonçalo for help)

## Script at INCD to create and process chips (chip_creation)

  - "flatten" prediction model files at INCD so simplify file structure

  - incorporate model prediction in ... and assess computation cost

  - Run prediction on GPU (+)

  - Input gpkg: possibly replace by chip size and chip overlap, to create/process all chips for tile: no coordinates necessary

  - Can efficiency be improved by chunk aware indexing of pixels?
