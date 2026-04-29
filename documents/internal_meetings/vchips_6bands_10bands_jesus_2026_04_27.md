Mon, 27 Apr 2026 19:12:57 -0600

Durante esta revisão, encontrei alguns casos particulares que convém ter em consideração antes de encerrar esta etapa, dou alguns exemplos:

1. Chips com rasters incompletos

Encontrei vários chips em que os rasters estão incompletos, mas que estavam completos na versão de 6 bandas. Acho que é um erro na geração dos vchips com 10 bandas. Então, se isto for corrigido , todos são bons casos a usar.

    vchip_580315_4590735_20200727_mask
    vchip_581895_4190475_20220416_mask
    vchip_582565_4591395_20200717_mask
    vchip_600285_4472825_20210603_mask
    vchip_601485_4417555_20200913_mask
    vchip_601575_4435605_20200705_mask
    vchip_470175_4299905_20220629_mask
    vchip_508255_4370295_20220128_mask
    vchip_508895_4395825_20220709_mask

2. Chips que foram interpretáveis com os rasters normais, mas não com os novos rasters de 10 bandas

Em alguns casos, o chip pôde ser interpretado usando os rasters normais de 6 bandas, mas com os novos rasters de 10 bandas a interpretação já não é possível, principalmente devido à presença de nuvens ou porque o filtro de nuvens excluiu zonas que antes eram interpretáveis:

    vchip_521725_4631435_20200910_mask
    vchip_539065_4650125_20200913_mask
    vchip_563035_4363975_20200421_mask
    vchip_602325_4352735_20200421_mask
    vchip_602435_4352325_20200421_mask
    vchip_602465_4352605_20200421_mask
    vchip_603035_4352655_20200506_mask
    vchip_604815_4429955_20200209_mask

Neste último caso, com as bandas normais era possível interpretar as alterações, mas no raster de 10 bandas o after aparece coberto por nuvens.

3. Chips onde a alteração era clara no raster normal, mas não no de 10 bandas

Nestes casos, os rasters normais mostravam alterações mais claras, enquanto nos novos rasters de 10 bandas a alteração já não se observa com a mesma clareza ou desaparece na maioria dos polígonos:

    vchip_577975_4189085_20200501_mask
    vchip_578875_4185935_20200501_mask
    vchip_580765_4189915_20200501_mask

Em geral, a revisão mostra que os novos rasters de 10 bandas ajudam a melhorar a interpretação na maior parte dos casos, mas as alterações no método de mascaramento fazem com que algumas imagens sejam bastante diferentes relativamente às de 6 bandas. Por isso, demorei mais do que o normal. Além disso, algumas imagens mostram um deslocamento relativamente às de 6 bandas, pelo que várias máscaras tiveram de ser ajustadas.
