# Guía de la etapa 3: procesador de flujos y TimescaleDB (ES-06, ES-01, S16)

## 1. Copiar los archivos
Descomprime `smartgrid-eco-etapa3.zip` y cópialo sobre tu carpeta, igual que la vez pasada:
```
Expand-Archive -Path "$env:USERPROFILE\Downloads\smartgrid-eco-etapa3.zip" -DestinationPath "$env:TEMP\sge3" -Force
Copy-Item -Path "$env:TEMP\sge3\smartgrid-eco\*" -Destination . -Recurse -Force
git status
```

## 2. Pruebas (sin Docker)
```
python -m unittest discover -s tests -t . -v
```
Deben salir **65 pruebas, OK, 8 omitidas (skipped)**. Las 8 omitidas son las de PostgreSQL: se activan en el paso 5.

## 3. Commits sugeridos (uno por uno)
```
git add cloud/__init__.py cloud/core tests/test_cloud_processing.py
git commit -m "Procesador de flujos: validación, deduplicación y escritura transaccional (núcleo)"

git add cloud/adapters/__init__.py cloud/adapters/store_sqlite.py cloud/adapters/consumer_memory.py cloud/adapters/deadletter_memory.py cloud/adapters/clock.py tests/test_e2e_facturacion.py
git commit -m "Almacén SQLite, log en memoria y prueba de punta a punta de facturación (ES-06)"

git add cloud/sql cloud/adapters/store_timescale.py tests/test_store_contract.py
git commit -m "Esquema SQL y almacén TimescaleDB con contrato común de pruebas"

git add cloud/adapters/consumer_kafka.py cloud/adapters/deadletter_file.py cloud/processor_main.py cloud/check_run.py tests/test_kafka_consumer_adapter.py tests/test_architecture.py
git commit -m "Consumidor Kafka con offsets en la BD, ejecutable del procesador y verificador de corridas"

git add cloud/store_bench.py infra requirements.txt .gitignore
git commit -m "Benchmark de S16 y TimescaleDB en Docker Compose"

git add README.md GUIA_ETAPA3.md
git commit -m "Documentación de la etapa 3"
git push
```

## 4. Levantar todo en Docker (Docker Desktop abierto)
```
pip install -r requirements.txt
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
```
Debe haber `sge-redpanda` y `sge-timescaledb` en estado *healthy* (la primera vez descarga la imagen de TimescaleDB, unos minutos).
Comprueba que `readings` es una hipertabla:
```
docker exec sge-timescaledb psql -U sge -d sge -c "select hypertable_name from timescaledb_information.hypertables;"
```

## 5. Pruebas del almacén contra TimescaleDB real
En PowerShell:
```
$env:SGE_PG_DSN = "postgresql://sge:sge@localhost:5433/sge"
python -m unittest tests.test_store_contract -v
```
Deben salir **16 pruebas OK y 0 omitidas** (8 de SQLite y 8 de TimescaleDB).

## 6. Medir S16 (la hipótesis de mayor riesgo)
```
python -m cloud.store_bench --rows 300000
```
Escribe `results/s16_timescale.json`. Pégame la salida completa. Ojo: el objetivo de 30.000 filas/s es para 2 nodos de 8 vCPU y 64 GiB; tu laptop dentro de Docker es una referencia, no la validación final. Un resultado por debajo del objetivo NO es un fracaso: es un dato que va al documento con su explicación.

## 7. Punta a punta con corte de red (ES-03 + ES-06)
Terminal 1: corre la demo del borde y **anota el identificador de la corrida** (aparece como `Corrida a1b2c3`):
```
python -m edge.forwarder_demo --readings 6000 --rate 100
```
Mientras corre, en otra terminal corta y vuelve a levantar Redpanda como en la etapa 2. Al terminar debe decir `PERDIDAS: 0`.
Luego procesa el log hacia la base:
```
python -m cloud.processor_main
python -m cloud.check_run --run a1b2c3 --expected 6000
```
(cambia `a1b2c3` por tu identificador). Debe decir `PASA`.

## 8. Reproceso (Kappa): la prueba que más impresiona en la defensa
Borra los offsets y relee todo el log con un grupo nuevo. Si la escritura es idempotente, no cambia nada:
```
docker exec sge-timescaledb psql -U sge -d sge -c "truncate consumer_offsets;"
python -m cloud.processor_main --group repro-1
python -m cloud.check_run --run a1b2c3 --expected 6000
```
Al final de `processor_main` debe verse `nuevos=0` y muchos `duplicados`; `check_run` sigue diciendo `PASA`.

Guarda la salida de cada paso: es la evidencia para el documento y el artículo.
