# SmartGrid-Eco: prototipo arquitectónico ejecutable

Corte vertical del caso SmartGrid-Eco (Examen de Suficiencia, CADI Profundización III – Estructura y
Arquitectura de Software). Valida con evidencia las decisiones de mayor riesgo del Documento de Diseño
Arquitectónico: ingesta con broker y procesador de flujos, núcleo hexagonal extensible y lazo de control
del borde.

**Autor:** Kevin Santiago Plazas Cubillos, Ingeniería de Software, Universidad de Cundinamarca.
**Uso de IA generativa:** declarado en la sección 17 del documento de diseño (qué partes y con qué propósito).

## Qué contiene

| Contenedor (Docker Compose) | Función | Decisión que valida |
|---|---|---|
| `sge-redpanda` | Log de eventos compatible con Kafka, 24 particiones | ADR-003 (Kappa), ES-01 |
| `sge-timescaledb` | Lecturas, agregados de 15 min y offsets (TimescaleDB / PostgreSQL 16) | ADR-004, ES-06 |
| `sge-processor` | Procesador de flujos: valida, descarta duplicados, escribe lecturas + agregados + offsets en **una** transacción | ADR-004 (efectivamente una vez) |
| `sge-edge-forwarder` | Reenviador del borde con cola durable en disco | ES-03, ES-01 |

Además, el núcleo hexagonal del controlador de borde (`edge/core`), la pasarela de comandos firmados y
la prueba de carga (`loadtest/`).

## Requisitos

- Python 3.12 y Docker Desktop (Compose v2).
- `pip install -r requirements.txt` (confluent-kafka, psycopg, cryptography). Las pruebas unitarias
  básicas solo usan la biblioteca estándar; las que dependen de PostgreSQL o `cryptography` se omiten si
  faltan.

## Despliegue (los 4 contenedores)

```
docker compose -f infra/docker-compose.yml --profile app up -d --build
docker compose -f infra/docker-compose.yml ps          # deben aparecer los 4 servicios activos
```

Comprobar que el flujo funciona (el contador debe crecer entre una consulta y otra):

```
docker exec sge-timescaledb psql -U sge -d sge -c "SELECT count(*) FROM readings;"
```

Apagar y limpiar: `docker compose -f infra/docker-compose.yml --profile app down -v`.
El esquema se aplica solo la primera vez (volumen vacío); para rehacerlo hay que usar `down -v`.

## Pruebas automáticas

```
python -m unittest discover -s tests -t . -v
```

Resultado esperado: **81 pruebas OK, 8 omitidas** (las omitidas requieren PostgreSQL real).
Incluye una prueba de arquitectura (`tests/test_architecture.py`) que falla si el núcleo importa un adaptador.

## Pruebas de carga y de fallos (informe: `SmartGridEco_Informe_Pruebas_de_Carga.pdf`)

Requieren Redpanda y TimescaleDB levantados (`docker compose -f infra/docker-compose.yml up -d`).

```
python -m loadtest.run --scenario burst --processors 2          # ráfaga x3 y recuperación
python -m loadtest.run --scenario ramp  --processors 2          # escalones hasta saturar
python -m loadtest.run --scenario chaos --processors 2 \
    --chaos "20:broker_down:5,45:db_down:5,70:kill_processor:0"  # inyección de fallos
```

Cada corrida verifica contra la base: lecturas únicas generadas = lecturas almacenadas, suma de kWh
exacta, cola del borde vacía, 0 mensajes muertos. Los resultados (JSON con p50/p95/p99) quedan en `results/`.
Opciones completas: `python -m loadtest.run --help`. Con `--backend file` se puede ensayar sin Docker.

**Por qué un generador propio y no k6/JMeter/Gatling.** El estímulo no es HTTP: es un reenviador del borde
con cola durable que publica por el protocolo de Kafka. El generador usa el mismo código del borde y mide
p50/p95/p99 de ingesta, publicación y extremo a extremo.

### Resultados principales (portátil Windows 11, 12 CPU, Python 3.12)

| Verificación | Resultado |
|---|---|
| Lecturas perdidas / diferencia de kWh en 6 corridas con Redpanda real | 0 / 0 |
| Caos (log caído, BD caída, procesador muerto) | 79.979 generadas = 79.979 almacenadas; 1.408 duplicados descartados |
| Recuperación tras ráfaga | 1,0 s con 2 procesadores; 13,1 s con 1 |
| Capacidad sostenida (1 / 2 / 4 procesadores) | ≈ 1,7 k / 4,1 k / 7,9 k filas/s |
| Escritura idempotente, mejor caso en un contenedor (S16) | 22.558 filas/s (la meta de 30.000 **no** se alcanzó) |

## Otras demostraciones

```
python -m edge.bench --events 1000                                   # ES-02: latencia del lazo, sin retardo de campo
python -m edge.bench --events 400 --io-delay-ms 50 --out results/es02_retardo_50ms.json
python -m edge.queue_bench --rows 20000                              # cola en disco (lecturas/s)
python -m edge.commands_demo                                         # ES-05 y ES-07: comandos firmados
python -m cloud.store_bench                                          # S16: inserción idempotente
```

| Escenario | Resultado |
|---|---|
| ES-02 lazo de control | p99 de 0,0117 ms de software; 50,8 ms con 50 ms de E/S simulada |
| ES-04 agregar biomasa | 3 archivos, 26 líneas, **0 líneas en `edge/core`** (ver commits) |
| ES-05 comandos no autenticados | 1.000 / 1.000 rechazados y auditados; 0 llegan al inversor |
| ES-07 doble aprobación | Un crítico con una sola firma queda pendiente; el mismo operador no puede aprobarse |

Evidencia de ES-04 en el historial: `git log --stat --oneline -- edge/adapters/simulated.py edge/composition.py`.

## Estructura

```
edge/core/           NÚCLEO del borde: modelo, puertos, balanceo, ciclo de control, reenvío, comandos.
edge/adapters/       Fuentes simuladas, actuador, auditoría con hash encadenado, cola SQLite, Kafka, firma Ed25519.
edge/composition.py  Único lugar que une núcleo y adaptadores (registro de fuentes).
cloud/core/          NÚCLEO de la nube: procesador de flujos e informe de huecos.
cloud/adapters/      Almacenes (SQLite, TimescaleDB), consumidor Kafka, mensajes muertos.
cloud/sql/           Esquema PostgreSQL / TimescaleDB.
loadtest/            Generadores, procesadores y orquestador de escenarios.
infra/               docker-compose.yml y Dockerfile.
tests/               Pruebas automáticas.
results/             Evidencia: JSON de benchmarks y de pruebas de carga.
GUIA_*.md            Guías paso a paso de cada etapa.
```

## Limitaciones conocidas (también en la sección 16 del documento)

- Un solo nodo de Redpanda y de TimescaleDB; no se prueba replicación ni falla de disco.
- El actuador y las fuentes son simulados: la latencia real de Modbus/IEC 61850 no está medida.
- Los kWh se guardan como `double`; ADR-008 (energía en Wh enteros) está decidido pero **no implementado**.
- Tras una caída de la base de datos, el procesador puede tardar ≈ 21 s en reanudar (falta tiempo máximo de conexión).
- La inyección `lose_acks` solo aplica al backend de archivo.
- No se alcanzaron las 30.000 filas/s (S16); la extrapolación al volumen del diseño está en la sección 16.6.

## Desarrollo incremental y autoría

Los commits están hechos desde la cuenta del autor, en pequeños pasos por etapa (`GUIA_COMMITS.md`).
`git log --oneline --stat` muestra la evolución: núcleo, adaptadores, reenviador, procesador, comandos,
carga y resultados.
