# Guía de commits (hazlos tú, con tu nombre)

La carta evalúa "historial de commits que evidencie el desarrollo incremental y la autoría".
Por eso los commits deben salir de tu usuario de Git y ser pequeños y frecuentes.
No hagas un solo commit con todo.

## Una sola vez

```
git config --global user.name "Kevin Santiago Plazas Cubillos"
git config --global user.email "ksplazas@ucundinamarca.edu.co"
cd C:\ruta\a\smartgrid-eco
git init
git branch -M main
```

## Commits sugeridos, en este orden

Antes de cada commit, abre el archivo, léelo y entiéndelo. Es parte del estudio.

```
git add edge/core/model.py edge/core/ports.py edge/__init__.py edge/core/__init__.py
git commit -m "Núcleo: modelo de dominio y puertos"

git add edge/core/balancing.py tests/test_balancing.py tests/__init__.py
git commit -m "Núcleo: política de balanceo pura con pruebas"

git add edge/core/control_loop.py
git commit -m "Núcleo: ciclo de control con medición de latencia"

git add edge/adapters/__init__.py edge/adapters/simulated.py edge/adapters/clock.py edge/adapters/policy_file.py edge/config
git commit -m "Adaptadores simulados, reloj y repositorio de políticas"

git add edge/adapters/audit_sqlite.py tests/test_loop_and_audit.py
git commit -m "Auditoría de solo inserción con hash encadenado"

git add tests/test_architecture.py
git commit -m "Prueba de arquitectura: el núcleo no importa adaptadores (ES-04)"

git add edge/bench.py results
git commit -m "Benchmark de latencia ES-02 y resultados"

git add README.md .gitignore GUIA_COMMITS.md
git commit -m "README y guía"
```

## Evidencia de ES-04 (el día que lo hagas de verdad)

El adaptador `SimulatedBiomass` y la prueba `test_agregar_biomasa_solo_cambia_la_configuracion`
ya existen. Para el historial, haz un commit propio que agregue SOLO el adaptador y la línea de
`SOURCE_ADAPTERS`, y comprueba que no toca `edge/core`:

```
git show --stat HEAD
```
