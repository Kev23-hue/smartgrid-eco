# SmartGrid-Eco: prototipo (etapas 1 y 2)

Prototipo del núcleo hexagonal del controlador de borde y de su lazo de balanceo.
Corresponde a ADR-001 y ADR-006 y a los escenarios ES-02 (latencia) y ES-04 (modificabilidad)
del Documento de Diseño Arquitectónico.

No necesita instalar nada: solo Python 3.12 (usa únicamente la biblioteca estándar).

## Cómo ejecutarlo (PowerShell, dentro de la carpeta del proyecto)

```
python -m unittest discover -s tests -t . -v      # 36 pruebas
python -m edge.bench --events 1000                # ES-02 sin retardo de campo
python -m edge.bench --events 400 --io-delay-ms 50 --out results/es02_retardo_50ms.json
```

## Estructura

```
edge/core/           NÚCLEO: modelo, puertos, política de balanceo, ciclo de control.
                     No importa adaptadores ni librerías de E/S (lo verifica una prueba).
edge/adapters/       ADAPTADORES: fuentes simuladas, actuador simulado, auditoría SQLite
                     con hash encadenado, repositorio de políticas, reloj.
edge/core/forwarding.py  NÚCLEO del reenviador (etapa 2): cola primero, red después, reintento con espera.
edge/composition.py  Único lugar que une núcleo y adaptadores (registro de fuentes).
edge/config/         Configuración de la microred y política de balanceo.
edge/bench.py        Benchmark de latencia del lazo (p50, p95, p99).
edge/queue_bench.py  Benchmark de la cola en disco (lecturas/s, ES-01).
edge/forwarder_demo.py  Demostración contra Redpanda real con corte de red (ES-03).
infra/               docker-compose con Redpanda de un nodo.
tests/               Pruebas automáticas.
results/             Salidas de los benchmarks (evidencia para el documento).
```

## Qué demuestra y qué NO demuestra

| Escenario | Qué se verifica | Límite |
|---|---|---|
| ES-02 | El tiempo que añade el software (decidir y ordenar) es de microsegundos; el presupuesto de 2 s lo consumen el sondeo y el bus de campo. | El actuador es simulado. La latencia real de Modbus/IEC 61850 debe medirse con hardware o un simulador de protocolo. |
| ES-04 | Agregar biomasa = un adaptador nuevo + una línea en la configuración, sin tocar `edge/core`. La prueba `test_architecture` falla si el núcleo importa adaptadores. | Los días-persona de ES-04 se miden con el historial de commits. |
| ES-05 (parcial) | La auditoría es de solo inserción y detecta manipulación por hash. | Falta la firma de comandos, nonce y caducidad (etapa siguiente). |

## Etapa 2: reenviador con cola en disco (ES-01, ES-03)

```
pip install -r requirements.txt                    # solo para la demo con Redpanda
docker compose -f infra/docker-compose.yml up -d   # levanta Redpanda y crea el tema telemetry.raw
python -m edge.forwarder_demo --readings 6000 --rate 100
docker compose -f infra/docker-compose.yml stop redpanda    # durante la demo: corta el log
docker compose -f infra/docker-compose.yml start redpanda   # y vuélvelo a levantar
python -m edge.queue_bench --rows 20000            # lecturas/s que aguanta la cola en disco
```

Reglas de diseño que se demuestran: la ingesta nunca espera a la red (se guarda primero en disco);
una lectura sale de la cola solo cuando el log la confirma; si falla, se reintenta con espera
creciente y aleatoria; el envío es al menos una vez (puede haber duplicados) y la clave
(meter_id, measured_at) permite descartarlos aguas abajo (etapa 3).

| Escenario | Qué se verifica | Límite |
|---|---|---|
| ES-03 | Con el log caído, la ingesta sigue, la cola crece, y al volver se reconcilia todo sin pérdidas. | Un solo nodo de Redpanda y un solo reenviador. No se prueba la caída del disco del borde. |
| ES-01 | La cola en disco aguanta el pico por microred (30.000 / 12 = 2.500 lecturas/s). | No es la prueba de 30.000/s del lado de la nube: eso es la etapa 4. |

## Próximas etapas

3. Procesador de flujos y TimescaleDB con escritura idempotente: ES-06 y S16.
4. Generador de carga, inyección de fallos e informe de pruebas.
