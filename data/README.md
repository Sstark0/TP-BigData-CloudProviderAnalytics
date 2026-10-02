# Datos


El dataset para la realización del proyecto (`cloud_provider_challenge_dataset_v1`)fue provisto por el docente, sin embargo, no se encuentra en el repositorio.

Para acceder a dicho material, es necesario

1. Descargar y descomprimir el dataset.
2. Copiar el contenido de `cloud_provider_challenge_dataset_v1/datalake/landing/` dentro de
   `datalake/landing/` de este repositorio. Debiendo quedar de la siguiente manera:

```
datalake/landing/
├── billing_monthly.csv      customers_orgs.csv   marketing_touches.csv
├── nps_surveys.csv          resources.csv        support_tickets.csv   users.csv
└── usage_events_stream/     events_part_0000.jsonl … events_part_0119.jsonl  (120 archivos)
```

O bien, el dataset puede residir en otra carpeta, lo que vuelve necesario definir `LANDING_PATH`.


## Muestra versionada (`data/sample/`)

Como la misma consigna lo solicita, el directorio `data/` debe contener datos de muestra o instrucciones para su obtención, asimismo se vuelve necesaria la existencia de datos de prueba suficientemente pequeños para una demostración remota. 

En linea con el cumplimiento de esto, mediante comando se tomaron las primeras 20 filas de cada archivo .csv (más el encabezado); y un archivo de eventos completo (`events_part_0000.jsonl`).

Es imperativo remarcar que esta muestra no conserva la integridad referencial. Un evento o un ticket de la muestra puede referirse a un `org_id` que no está entre las 20 organizaciones de `customers_orgs.csv`. Por tal razón, solo sirve para probar lectura y transformaciones, no para validar joins ni totales.


`README_dataset_original.txt` es el README del docente, sin modificaciones.
