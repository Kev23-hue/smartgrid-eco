# SmartGrid-Eco: prototipo, etapa 1 (controlador de borde)

Prototipo del núcleo hexagonal del controlador de borde y de su lazo de balanceo.
Corresponde a ADR-001 y ADR-006 y a los escenarios ES-02 (latencia) y ES-04 (modificabilidad)
del Documento de Diseño Arquitectónico.

No necesita instalar nada: solo Python 3.12 (usa únicamente la biblioteca estándar).

## Cómo ejecutarlo (PowerShell, dentro de la carpeta del proyecto)

```
python -m unittest discover -s tests -t . -v      # 16 pruebas
python -m edge.bench --events 1000                # ES-02 sin retardo de campo
python -m edge.bench --events 400 --io-delay-ms 50 --out results/es02_retardo_50ms.json
```

## Estructura

```
edge/core/           NÚCLEO: modelo, puertos, política de balanceo, ciclo de control.
                     No importa adaptadores ni librerías de E/S (lo verifica una prueba).
edge/adapters/       ADAPTADORES: fuentes simuladas, actuador simulado, auditoría SQLite
                     con hash encadenado, repositorio de políticas, reloj.
edge/composition.py  Único lugar que une núcleo y adaptadores (registro de fuentes).
edge/config/         Configuración de la microred y política de balanceo.
edge/bench.py        Benchmark de latencia del lazo (p50, p95, p99).
tests/               Pruebas automáticas.
results/             Salidas de los benchmarks (evidencia para el documento).
```

## Qué demuestra y qué NO demuestra

| Escenario | Qué se verifica | Límite |
|---|---|---|
| ES-02 | El tiempo que añade el software (decidir y ordenar) es de microsegundos; el presupuesto de 2 s lo consumen el sondeo y el bus de campo. | El actuador es simulado. La latencia real de Modbus/IEC 61850 debe medirse con hardware o un simulador de protocolo. |
| ES-04 | Agregar biomasa = un adaptador nuevo + una línea en la configuración, sin tocar `edge/core`. La prueba `test_architecture` falla si el núcleo importa adaptadores. | Los días-persona de ES-04 se miden con el historial de commits. |
| ES-05 (parcial) | La auditoría es de solo inserción y detecta manipulación por hash. | Falta la firma de comandos, nonce y caducidad (etapa siguiente). |

## Próximas etapas

2. Reenviador con cola en disco y Redpanda (Docker Compose): ES-01 y ES-03.
3. Procesador de flujos y TimescaleDB con escritura idempotente: ES-06 y S16.
4. Generador de carga, inyección de fallos e informe de pruebas.
