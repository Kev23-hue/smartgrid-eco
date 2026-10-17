"""Orquestador de la prueba de carga y de la inyección de fallas.

Lanza G generadores (reenviadores del borde) y P procesadores como procesos separados, ejecuta una
línea de tiempo de fallas, muestrea el avance cada medio segundo y al final VERIFICA contra la BD:
  - lecturas únicas generadas == lecturas en la BD   (0 perdidas)
  - suma de kWh == lo esperado                        (0 duplicadas contadas en la factura)
  - cola del borde vacía, mensajes muertos == 0.
Escribe un JSON con todo (series de tiempo, percentiles, veredictos).

Escenarios:
  burst : nominal -> ráfaga (x3) -> nominal. ES-01 a escala: tiempo de recuperación tras la ráfaga.
  chaos : carga constante con fallas (log caído, BD caída, proceso procesador muerto, acks perdidos,
          duplicados y llegadas tardías). ES-03 y ES-06 bajo falla.
  ramp  : escalones de carga crecientes para hallar el punto de saturación (base de la extrapolación).

Ejemplos:
  python -m loadtest.run --backend file --scenario chaos --nominal 800 --out results/ensayo.json
  python -m loadtest.run --backend kafka --scenario burst --nominal 1000 --generators 2 --processors 2
"""
from __future__ import annotations

import argparse
import csv
import os
import platform
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

from .common import now_ms, percentiles, read_json, write_json_atomic

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = os.path.join(RAIZ, "infra", "docker-compose.yml")


def escenario(a) -> tuple[list[tuple[str, float, float]], str]:
    n, b = a.nominal, a.nominal * a.burst_mult
    if a.scenario == "burst":
        return [("nominal_1", n, a.phase_secs), ("rafaga", b, a.burst_secs), ("nominal_2", n, a.phase_secs * 2)], ""
    if a.scenario == "chaos":
        d = a.phase_secs * 4
        return [("carga_con_fallas", n, d)], a.chaos or f"{int(d*.12)}:broker_down:8,{int(d*.35)}:db_down:8,{int(d*.55)}:kill_processor:4" + (f",{int(d*.7)}:lose_acks:3" if a.backend == "file" else "")
    if a.scenario == "ramp":
        return [(f"escalon_{int(n*m)}", n * m, a.phase_secs) for m in (1, 2, 4, 8, 16, 32)], ""
    raise SystemExit("escenario desconocido")


def es_progreso_proc(nombre: str) -> bool:
    return nombre.startswith("proc_p") and nombre.endswith(".json") and ".tmp" not in nombre


class Entorno:
    """Todo lo que cambia entre ensayo (file) y real (kafka): cómo se tumba y levanta cada servicio."""

    def __init__(self, a, workdir):
        self.a, self.dir = a, workdir
        self.ctl = os.path.join(workdir, "control")
        os.makedirs(self.ctl, exist_ok=True)

    def compose(self, accion: str, servicio: str) -> None:
        subprocess.run(["docker", "compose", "-f", COMPOSE, accion, servicio], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def broker(self, abajo: bool) -> None:
        if self.a.backend == "file":
            p = os.path.join(self.ctl, "broker.down")
            (open(p, "w").close() if abajo else (os.path.exists(p) and os.remove(p)))
        else:
            self.compose("stop" if abajo else "start", "redpanda")

    def bd(self, abajo: bool) -> None:
        if self.a.backend == "file":
            p = os.path.join(self.ctl, "db.down")
            (open(p, "w").close() if abajo else (os.path.exists(p) and os.remove(p)))
        else:
            self.compose("stop" if abajo else "start", "timescaledb")

    def perder_acks(self, n: int) -> bool:
        """Solo es aplicable con el log en archivo; con Kafka real no hay forma de perder acks desde fuera."""
        if self.a.backend == "file":
            open(os.path.join(self.ctl, "lose_acks"), "w").write(str(n))
            return True
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["file", "kafka"], default="kafka")
    ap.add_argument("--scenario", choices=["burst", "chaos", "ramp"], default="burst")
    ap.add_argument("--nominal", type=float, default=1000, help="lecturas/s totales en régimen nominal")
    ap.add_argument("--burst-mult", type=float, default=3.0)
    ap.add_argument("--phase-secs", type=float, default=20)
    ap.add_argument("--burst-secs", type=float, default=30)
    ap.add_argument("--generators", type=int, default=2)
    ap.add_argument("--processors", type=int, default=1)
    ap.add_argument("--meters", type=int, default=200, help="contadores por generador")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--sync", default="NORMAL", choices=["FULL", "NORMAL"])
    ap.add_argument("--dup-ratio", type=float, default=None)
    ap.add_argument("--late-ratio", type=float, default=None)
    ap.add_argument("--chaos", default="", help="t:accion:param,... acciones: broker_down, db_down, kill_processor, lose_acks")
    ap.add_argument("--partitions", type=int, default=24)
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--dsn", default="postgresql://sge:sge@localhost:5433/sge")
    ap.add_argument("--drain-timeout", type=float, default=300)
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    if a.backend == "file" and a.processors != 1:
        raise SystemExit("el log en archivo tiene una sola partición: use --processors 1")

    if a.dup_ratio is None:
        a.dup_ratio = 0.02 if a.scenario == "chaos" else 0.0
    if a.late_ratio is None:
        a.late_ratio = 0.02 if a.scenario == "chaos" else 0.0

    run = uuid.uuid4().hex[:6]
    workdir = os.path.join(RAIZ, "results", "carga_tmp", run)
    os.makedirs(workdir, exist_ok=True)
    env = Entorno(a, workdir)
    fases, chaos_txt = escenario(a)
    eventos = sorted((float(t), acc, float(p)) for t, acc, p in (x.split(":") for x in chaos_txt.split(",") if x))
    topic = f"telemetry.load.{run}" if a.backend == "kafka" else "telemetry.raw"
    total_secs = sum(s for _, _, s in fases)
    print(f"Corrida {run} · backend={a.backend} · escenario={a.scenario} · {a.generators} generador(es), "
          f"{a.processors} procesador(es) · duración nominal {total_secs:.0f} s")
    print("Fases:", ", ".join(f"{n}={r:.0f}/s x {s:.0f}s" for n, r, s in fases))
    if eventos:
        print("Fallas:", ", ".join(f"t={t:.0f}s {acc}({p:.0f})" for t, acc, p in eventos))

    comun = ["--backend", a.backend, "--dir", workdir, "--run", run, "--topic", topic,
             "--bootstrap", a.bootstrap, "--dsn", a.dsn]
    if a.backend == "kafka":
        from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
        adm = AdminClient({"bootstrap.servers": a.bootstrap})
        for f in adm.create_topics([NewTopic(topic, num_partitions=a.partitions, replication_factor=1)]).values():
            f.result(timeout=30)
        from cloud.adapters.store_timescale import TimescaleStore
        st = TimescaleStore(a.dsn)
        st.truncate_all()
        st.close()

    def lanzar(modulo, extra, nombre):
        log = open(os.path.join(workdir, f"{nombre}.log"), "a")
        return subprocess.Popen([sys.executable, "-m", modulo, *comun, *extra], cwd=RAIZ, stdout=log, stderr=log)

    procs = {}
    def lanzar_proc(i, inc=0):
        procs[i] = lanzar("loadtest.processor", ["--name", f"p{i}_{inc}", "--group", f"proc-{run}",
                                                  "--batch", str(a.batch)], f"proc_{i}_{inc}")
    incarnaciones = {i: 0 for i in range(a.processors)}
    for i in range(a.processors):
        lanzar_proc(i)
    time.sleep(2.0)
    fases_txt = ",".join(f"{n}:{r / a.generators:.2f}:{s}" for n, r, s in fases)
    gens = [lanzar("loadtest.generator", ["--id", str(g), "--phases", fases_txt, "--meters", str(a.meters),
                                          "--batch", str(a.batch), "--dup-ratio", str(a.dup_ratio),
                                          "--late-ratio", str(a.late_ratio), "--sync", a.sync,
                                          "--drain-timeout", str(a.drain_timeout)], f"gen_{g}")
            for g in range(a.generators)]
    t0 = time.time()
    log_fallas: list[dict] = []

    def ejecutar(acc, p, t):
        reg = {"t": round(t, 1), "accion": acc, "param": p, "inicio_real": round(time.time() - t0, 1)}
        if acc == "broker_down":
            env.broker(True); time.sleep(p); env.broker(False)
        elif acc == "db_down":
            env.bd(True); time.sleep(p); env.bd(False)
        elif acc == "kill_processor":
            v = procs[0]
            v.kill(); v.wait()
            time.sleep(p)
            incarnaciones[0] += 1
            lanzar_proc(0, incarnaciones[0])
        elif acc == "lose_acks":
            reg["aplicada"] = env.perder_acks(int(p))
            if not reg["aplicada"]:
                print("AVISO: lose_acks no se aplica con backend kafka; se registra como no aplicada")
        reg["fin_real"] = round(time.time() - t0, 1)
        log_fallas.append(reg)

    hilos, pend = [], list(eventos)
    serie = []
    ultimo: dict = {}
    deadline = t0 + total_secs + a.drain_timeout + 60
    try:
        while time.time() < deadline:
            t = time.time() - t0
            while pend and pend[0][0] <= t:
                ev = pend.pop(0)
                h = threading.Thread(target=ejecutar, args=(ev[1], ev[2], ev[0]), daemon=True)
                h.start(); hilos.append(h)
            # Lectura robusta: si un archivo de progreso no se puede leer un instante, se conserva su último valor
            # (antes se omitía y la suma caía); los temporales *.tmp* nunca se cuentan como procesadores.
            for g in range(a.generators):
                v = read_json(os.path.join(workdir, f"gen_{g}.json"))
                if v:
                    ultimo[f"gen_{g}.json"] = v
            for f in os.listdir(workdir):
                if es_progreso_proc(f):
                    v = read_json(os.path.join(workdir, f))
                    if v:
                        ultimo[f] = v
            gp = [ultimo[f"gen_{g}.json"] for g in range(a.generators) if f"gen_{g}.json" in ultimo]
            pp = [v for k, v in ultimo.items() if k.startswith("proc_p")]
            gen_u = sum(x["unique_generated"] for x in gp)
            ins = sum(x["inserted"] for x in pp)
            serie.append({"t": round(t, 1), "generadas": gen_u, "publicadas": sum(x["published"] for x in gp),
                          "en_cola_borde": sum(x["backlog"] for x in gp), "insertadas": ins,
                          "consumidas": sum(x["consumed"] for x in pp), "rezago": gen_u - ins,
                          "fase": gp[0]["phase"] if gp else ""})
            if int(t * 2) % 4 == 0:
                print(f"t={t:6.1f}s  fase={serie[-1]['fase']:<16} generadas={gen_u:8d}  en_cola_borde={serie[-1]['en_cola_borde']:7d}  "
                      f"insertadas={ins:8d}  rezago={gen_u - ins:7d}")
            if all(g.poll() is not None for g in gens):
                break
            time.sleep(0.5)
        for h in hilos:
            h.join(timeout=120)
        # Los generadores terminaron (cola del borde vacía o tiempo agotado). Esperar a que el procesador alcance.
        open(os.path.join(env.ctl, "stop"), "w").close()
        for p in procs.values():
            try:
                p.wait(timeout=a.drain_timeout)
            except subprocess.TimeoutExpired:
                p.kill()
    finally:
        for p in list(procs.values()) + gens:
            if p.poll() is None:
                p.kill()
        env.broker(False); env.bd(False)

    # ---------- verificación contra la BD ----------
    gf = [read_json(os.path.join(workdir, f"gen_{g}.json")) or {} for g in range(a.generators)]
    pf = [read_json(os.path.join(workdir, f)) for f in sorted(os.listdir(workdir)) if es_progreso_proc(f)]
    pf = [x for x in pf if x]
    if a.backend == "file":
        from cloud.adapters.store_sqlite import SqliteStore
        store = SqliteStore(os.path.join(workdir, "store.db"))
    else:
        from cloud.adapters.store_timescale import TimescaleStore
        store = TimescaleStore(a.dsn)
    n_db, kwh_db = store.prefix_totals(run)
    store.close()
    esperadas = sum(x.get("unique_generated", 0) for x in gf)
    perdidas = esperadas - n_db
    kwh_esp = 0.5 * esperadas
    en_cola = sum(x.get("backlog", 0) for x in gf)
    rechazadas_cola = sum(x.get("rejected_full", 0) for x in gf)
    muertos = sum(x["rejected"] for x in pf)
    duplicados_vistos = sum(x["duplicates"] for x in pf)

    # ---------- métricas ----------
    inicio_rafaga = fases[0][2] if a.scenario == "burst" else None
    fin_rafaga = inicio_rafaga + fases[1][2] if inicio_rafaga is not None else None
    recuperacion = None
    if fin_rafaga is not None:
        umbral = max(2 * a.nominal, 1000)    # "ingesta nominal restablecida": rezago equivale a <= 2 s de carga nominal
        for s in serie:
            if s["t"] >= fin_rafaga and s["rezago"] <= umbral:
                recuperacion = round(s["t"] - fin_rafaga, 1)
                break
    tasas = []
    for i in range(1, len(serie)):
        for j in range(i - 1, -1, -1):
            if serie[i]["t"] - serie[j]["t"] >= 5:
                tasas.append((serie[i]["insertadas"] - serie[j]["insertadas"]) / (serie[i]["t"] - serie[j]["t"]))
                break
    lat_total: list[float] = []
    for p in pf:
        pass
    # muestras de latencia: cada procesador entrega solo percentiles; se reportan por procesador y el peor p99
    lat_por_proc = [x.get("latency_ms") for x in pf if x.get("latency_ms")]
    fases_info = []
    acum = 0.0
    for n, r, s in fases:
        fases_info.append({"fase": n, "tasa_objetivo": r, "inicio_s": round(acum, 1), "duracion_s": s})
        acum += s
    por_fase = {}
    for f in fases_info:
        pts = [s for s in serie if f["inicio_s"] + 2 <= s["t"] <= f["inicio_s"] + f["duracion_s"]]
        if len(pts) >= 2:
            por_fase[f["fase"]] = {
                "generadas_por_s": round((pts[-1]["generadas"] - pts[0]["generadas"]) / (pts[-1]["t"] - pts[0]["t"]), 1),
                "insertadas_por_s": round((pts[-1]["insertadas"] - pts[0]["insertadas"]) / (pts[-1]["t"] - pts[0]["t"]), 1),
                "rezago_max": max(p["rezago"] for p in pts)}
    resultado = {
        "corrida": run, "fecha_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "entorno": {"backend": a.backend, "so": platform.platform(), "python": platform.python_version(),
                    "cpus": os.cpu_count(), "generadores": a.generators, "procesadores": a.processors,
                    "particiones": a.partitions if a.backend == "kafka" else 1,
                    "lote": a.batch, "sync_cola": a.sync},
        "escenario": a.scenario, "fases": fases_info, "fallas_inyectadas": sorted(log_fallas, key=lambda x: x["t"]),
        "duplicados_inyectados": a.dup_ratio, "tardias_inyectadas": a.late_ratio,
        "verificacion": {
            "lecturas_unicas_generadas": esperadas, "lecturas_en_bd": n_db, "perdidas": perdidas,
            "kwh_esperado": kwh_esp, "kwh_en_bd": kwh_db, "kwh_diferencia": round(kwh_db - kwh_esp, 6),
            "cola_borde_al_final": en_cola, "rechazadas_por_cola_llena": rechazadas_cola,
            "mensajes_muertos": muertos, "errores_inesperados_envio": sum(len(x.get("sender_errors", [])) for x in gf), "duplicados_descartados_por_clave": duplicados_vistos,
            "reconexiones_procesador": sum(x.get("reconnects", 0) for x in pf),
            "PASA_sin_perdidas_ni_duplicados": perdidas == 0 and abs(kwh_db - kwh_esp) < 1e-6 and en_cola == 0
                                                 and rechazadas_cola == 0 and muertos == 0
                                                 and not any(x.get('sender_errors') for x in gf)},
        "rendimiento": {
            "tasa_insertada_max_ventana5s": round(max(tasas), 1) if tasas else None,
            "tasa_insertada_media_total": round(n_db / serie[-1]["t"], 1) if serie else None,
            "por_fase": por_fase,
            "recuperacion_tras_rafaga_s": recuperacion,
            "umbral_rezago_recuperacion": max(2 * a.nominal, 1000) if fin_rafaga is not None else None},
        "latencia_extremo_a_extremo_ms_por_procesador": lat_por_proc,
        "ingesta_en_borde": [{"id": g.get("id"), "lote_ms": g.get("ingest_batch_ms"),
                              "lote_ms_durante_caida": g.get("ingest_batch_ms_during_outage"),
                              "publicacion_ms": g.get("publish_ms"), "fallos_publicacion": g.get("publish_failures")}
                             for g in gf],
        "serie": serie}
    out = a.out or os.path.join(RAIZ, "results", f"carga_{a.scenario}_{a.backend}_{run}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    write_json_atomic(out, resultado)
    with open(out.replace(".json", ".csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(serie[0].keys()))
        w.writeheader(); w.writerows(serie)

    v = resultado["verificacion"]
    print("\n================ RESULTADO ================")
    print(f"Únicas generadas: {v['lecturas_unicas_generadas']}   en BD: {v['lecturas_en_bd']}   PERDIDAS: {v['perdidas']}")
    print(f"kWh esperado: {v['kwh_esperado']}   en BD: {v['kwh_en_bd']}   diferencia: {v['kwh_diferencia']}")
    print(f"Duplicados descartados por clave: {v['duplicados_descartados_por_clave']}  | mensajes muertos: {v['mensajes_muertos']} "
          f"| cola del borde al final: {v['cola_borde_al_final']} | reconexiones del procesador: {v['reconexiones_procesador']}")
    print(f"Tasa insertada máx. (ventana 5 s): {resultado['rendimiento']['tasa_insertada_max_ventana5s']} lecturas/s")
    for n, d in por_fase.items():
        print(f"  fase {n:<18} generadas/s={d['generadas_por_s']:>9}  insertadas/s={d['insertadas_por_s']:>9}  rezago máx={d['rezago_max']}")
    if recuperacion is not None:
        print(f"Recuperación tras la ráfaga: {recuperacion} s   (medida: ≤ 300 s)")
    for i, l in enumerate(lat_por_proc):
        print(f"Latencia extremo a extremo (procesador {i}): p50={l['p50']} p95={l['p95']} p99={l['p99']} ms (n={l['n']})")
    print(f"\nSIN PERDIDAS NI DUPLICADOS: {'PASA' if v['PASA_sin_perdidas_ni_duplicados'] else 'FALLA'}")
    print(f"Resultados: {out}")
    shutil.rmtree(workdir, ignore_errors=True) if v["PASA_sin_perdidas_ni_duplicados"] and not os.environ.get("SGE_KEEP") else None


if __name__ == "__main__":
    main()
