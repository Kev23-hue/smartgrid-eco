# Guía de la etapa 4: comandos firmados, prueba de carga y fallas (ES-05, ES-07, ES-01, ES-03, ES-06)

Hazlo en orden y **un comando a la vez**, mirando que la salida se parezca a la que se describe.
Lo que esté marcado con 📎 es evidencia: guárdalo y mándamelo para armar el informe y el documento.

## 1. Copiar los archivos
```
Expand-Archive -Path "$env:USERPROFILE\Downloads\smartgrid-eco-etapa4.zip" -DestinationPath "$env:TEMP\sge4" -Force
Copy-Item -Path "$env:TEMP\sge4\smartgrid-eco\*" -Destination . -Recurse -Force
git status
```
(Si prefieres arrastrar y reemplazar como la vez pasada, también sirve.) `git status` debe mostrar archivos nuevos en `loadtest/`, `edge/`, `tests/`, `infra/`.

## 2. Instalar lo nuevo
```
pip install -r requirements.txt
```
Instala `cryptography` (firmas). Lo demás ya lo tenías.

## 3. Pruebas unitarias
```
python -m unittest discover -s tests -t . -v
```
Deben salir **81 pruebas, OK, 8 omitidas (skipped)**. Una de ellas (`test_loadtest_smoke`) arranca varios procesos y tarda unos 20–40 s: es normal.
Si falla algo, copia el mensaje completo y mándamelo.

## 4. Demostración de comandos firmados (ES-05 y ES-07) 📎
```
python -m edge.commands_demo | Tee-Object results\comandos_demo.txt
```
Debes ver, entre otras, estas líneas:
- `Rechazados: 1000/1000 | registrados en la auditoría: 1000/1000 | llegaron al inversor: 0` y `ES-05 -> PASA`
- `ES-07 -> PASA`
- `cadena de hashes íntegra: True` y `Intento de borrar la auditoría: bloqueado`
- La última línea da la latencia de verificar (p50 y p99 en ms) con nonces y auditoría **en disco**. Es el dato de T4.

## 5. Commits de esta parte (uno por uno, pega cada bloque por separado)
```
git add edge/core/commands.py edge/adapters/sign_ed25519.py edge/adapters/nonce_sqlite.py edge/adapters/clock.py tests/test_commands.py
git commit -m "Pasarela de comandos: firma, nonce, vigencia y doble aprobación (ES-05, ES-07)"

git add edge/commands_demo.py results/comandos_demo.txt
git commit -m "Demostración de ES-05/ES-07 y medición del costo de verificar"
```

## 6. Ensayo de la prueba de carga SIN Docker (para ver cómo se ve)
```
python -m loadtest.run --backend file --scenario chaos --nominal 800 --phase-secs 5
```
Dura ~30 s. Al final debe decir `SIN PERDIDAS NI DUPLICADOS: PASA`. Aquí el "log" es un archivo, así que **estas cifras no sirven como evidencia de rendimiento**; solo prueba que el montaje funciona.

## 7. Prueba REAL con Redpanda y TimescaleDB 📎
Cierra navegador y programas pesados: los generadores, los procesadores y Docker compiten por el mismo equipo.

Abre Docker Desktop y levanta el log y la base (una vez):
```
docker compose -f infra/docker-compose.yml up -d
```
Espera ~30 s y comprueba que `redpanda` y `timescaledb` estén `healthy`:
```
docker compose -f infra/docker-compose.yml ps
```

**7.1 Ráfaga (ES-01 a escala)** — nominal 1.000 lecturas/s, ráfaga ×3, ~2 min:
```
python -m loadtest.run --backend kafka --scenario burst --nominal 1000 --generators 2 --processors 2
```
Mira la línea `Recuperación tras la ráfaga` (la medida es ≤ 300 s) y `PERDIDAS: 0`.

**7.2 Escalones hasta saturar (para la extrapolación)** — cada escalón dura 15 s; repite con 1, 2 y 4 procesadores para ver si escala:
```
python -m loadtest.run --backend kafka --scenario ramp --nominal 500 --phase-secs 15 --generators 4 --processors 1
python -m loadtest.run --backend kafka --scenario ramp --nominal 500 --phase-secs 15 --generators 4 --processors 2
python -m loadtest.run --backend kafka --scenario ramp --nominal 500 --phase-secs 15 --generators 4 --processors 4
```
Fíjate en `insertadas/s` por escalón: donde deja de seguir a `generadas/s` está el techo de tu equipo.

**7.3 Fallas (ES-03 y ES-06 bajo falla)** — tumba Redpanda y TimescaleDB de verdad (el script ejecuta `docker compose stop/start` por ti), mata un procesador, pierde acks y mete 2% de duplicados y 2% de lecturas tardías:
```
python -m loadtest.run --backend kafka --scenario chaos --nominal 1000 --phase-secs 20 --generators 2 --processors 1
```
Resultado esperado: `PERDIDAS: 0`, `diferencia: 0.0`, `SIN PERDIDAS NI DUPLICADOS: PASA`, y `reconexiones del procesador` ≥ 1.
Si dice `FALLA`, **no lo repitas hasta que sale bien**: mándame la salida completa, eso es un hallazgo.

Cada corrida deja `results\carga_<escenario>_kafka_<id>.json` y `.csv`. Al terminar:
```
docker compose -f infra/docker-compose.yml ps
```
(los servicios deben quedar arriba; si alguno quedó detenido: `docker compose -f infra/docker-compose.yml start`).

## 8. Cuatro contenedores en marcha (corte vertical)
```
docker compose -f infra/docker-compose.yml --profile app up -d --build
```
La primera vez tarda unos minutos (construye la imagen). Luego:
```
docker compose -f infra/docker-compose.yml ps
```
Deben aparecer en marcha: `sge-redpanda`, `sge-timescaledb`, `sge-processor` y `sge-edge-forwarder` (el servicio `topics` termina solo: es normal). Verifica que los datos fluyen, ejecutando dos veces con unos segundos de diferencia:
```
docker exec sge-timescaledb psql -U sge -d sge -c "select count(*) from readings where meter_id like 'compose%';"
```
El número debe crecer. Captura la pantalla de `ps` 📎. Para apagar la parte de aplicación:
```
docker compose -f infra/docker-compose.yml --profile app down
```
(no borra tus datos). Apágala antes de repetir el paso 7: compite por recursos.

## 9. Evidencia de ES-04 (biomasa sin tocar el núcleo)
El adaptador de biomasa ya existe, pero quedó dentro de un commit grande. Para tener la evidencia limpia en el historial, **quítalo y vuelve a ponerlo con commits propios**:
1. Quita `SimulatedBiomass` de `edge/adapters/simulated.py`, la línea `"biomasa": SimulatedBiomass,` y su importación en `edge/composition.py`, y la prueba de biomasa de `tests/test_composition.py`.
2. `git add -A` y `git commit -m "Quitar la fuente de biomasa para repetir la medición de ES-04"`
3. `git revert --no-edit HEAD` (esto la vuelve a agregar en UN commit).
4. `git show --stat HEAD` → **debe listar solo** `simulated.py`, `composition.py` y la prueba; **ningún archivo de `edge/core`** 📎.
5. Anota cuánto tardaste en hacer el cambio (ES-04 mide días-persona) y dímelo. En el documento diré que la medición se repitió quitando y reponiendo la fuente, no que fue la primera vez.

## 10. Subir todo
```
git add loadtest edge cloud tests infra .dockerignore .gitignore requirements.txt README.md GUIA_ETAPA4.md results
git commit -m "Prueba de carga, inyección de fallas, contenedores del corte vertical y guía de la etapa 4"
git push
```
Si `git log` abre un paginador, presiona `q`.

## Qué mandarme de vuelta
1. La salida de los pasos 3 y 4 (o captura).
2. Los `.json` de `results\` de las corridas 7.1, 7.2 (las tres) y 7.3.
3. Captura de `docker compose ps` del paso 8.
4. La salida de `git show --stat HEAD` del paso 9 y el tiempo que tardaste.
5. Cualquier error, completo.

Con eso armo el informe de pruebas de carga (PDF con p50/p95/p99 y la extrapolación justificada) y la sección de validación del documento.
