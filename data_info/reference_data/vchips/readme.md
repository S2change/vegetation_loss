# Vchips (visual chips)

These are $4 \times 4 ~km^2$ annotated chips. Each *vchip* corresponds to a date *D* and a location. There are three components:
1. **before**: multiband geotiff file (`int16`) which is a time composite *before* date *D*, with *N* bands. Band values are reflectances between 0 and 10000. There are no NoData values due to compositing (if no value is available, pixel value is 0). The band order is ...
2. **after** : same, but *after* date *D*
3. **mask**: singleband geotiff (`int8`) with the same extent that represents the visualy identifies classes of *vegetation lost* or *no change* from *before* to *after*. The classes are: 0 - *no change* ;  1 - ...

Naming rules for *vchips*:
1. date: this is the intermediate date *D* and in general it is not equal to an acquisition date.
2. etc

Examples are available at `\partilhado\vchips`.
