# Addressed issues:

(+) are issues that need to be discussed with DGT/LIP

## hdf5 files with duplicate dates (+)

Actions: 
  - revise create_hdf5.py script to prevent duplicates and force ordered dates in hdf5 creation
  - Identify how many duplicates there are in each tile (+)

## Determine current RAM memory use per CPU

Solution: ask LIP (Gonçalo for help)

## Script at INCD to create and process chips (chip_creation)

  - "flatten" prediction model files at INCD so simplify file structure

  - incorporate model prediction in ... and assess computation cost

  - Run prediction on GPU (+)

  - Input gpkg: possibly replace by chip size and chip overlap, to create/process all chips for tile: no coordinates necessary

  - Can efficiency be improved by chunk aware indexing of pixels?
