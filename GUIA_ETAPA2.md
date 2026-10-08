# Guía de la etapa 2

## 1. Copiar los archivos nuevos
Descomprime `smartgrid-eco-etapa2.zip` y copia su contenido sobre tu carpeta `smartgrid-eco`
(acepta reemplazar). `git status` mostrará archivos modificados y nuevos.

## 2. Pruebas (no necesitan Docker)
```
python -m unittest discover -s tests -t . -v
```
Deben salir 36 pruebas en OK.

## 3. Commits sugeridos (uno por uno)
```
git add edge/core/model.py edge/core/ports.py edge/core/forwarding.py tests/test_forwarding.py
git commit -m "Reenviador: modelo Reading, puertos y lógica de store-and-forward con pruebas"

git add edge/adapters/codec.py edge/adapters/queue_sqlite.py edge/adapters/broker_fake.py tests/test_queue_sqlite.py
git commit -m "Cola duradera en SQLite (WAL) y log simulado para pruebas"

git add edge/adapters/broker_kafka.py tests/test_kafka_adapter.py requirements.txt
git commit -m "Adaptador Kafka/Redpanda: productor idempotente con acks=all"

git add infra edge/forwarder_demo.py edge/queue_bench.py
git commit -m "Docker Compose con Redpanda, demo de corte de red y benchmark de la cola"

git add README.md GUIA_ETAPA2.md .gitignore
git commit -m "Documentación de la etapa 2"
git push
```

## 4. Pruebas con Redpanda real (necesitan Docker Desktop abierto)
```
pip install -r requirements.txt
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
python -m edge.forwarder_demo --readings 6000 --rate 100
```
Mientras corre (se ve una línea por segundo), abre OTRA terminal y ejecuta:
```
docker compose -f infra/docker-compose.yml stop redpanda
```
Espera unos 20 segundos viendo cómo crece `en_cola`, y luego:
```
docker compose -f infra/docker-compose.yml start redpanda
```
Al final debe imprimir `PERDIDAS: 0 -> PASA`. Guarda la salida completa de la terminal: es evidencia
para el documento y para el artículo.

## 5. Benchmark de la cola en tu disco
```
python -m edge.queue_bench --rows 20000
```
Genera `results/cola_en_disco.json`. Los números dependen de tu disco; usa los tuyos, no los míos.
