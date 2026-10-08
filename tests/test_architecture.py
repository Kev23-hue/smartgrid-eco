"""ES-04 / ADR-006: el núcleo no puede depender de adaptadores ni de librerías de E/S.

Esta prueba es la que corre en integración continua: si alguien importa un adaptador
desde edge/core, falla.
"""
import ast
import pathlib
import unittest

CORE = pathlib.Path(__file__).resolve().parent.parent / "edge" / "core"
PROHIBIDOS = ("edge.adapters", "edge.composition", "sqlite3", "socket", "requests", "paho", "kafka",
              "confluent_kafka", "time", "os", "subprocess", "json")


def imports_de(path: pathlib.Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                yield n.name
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            yield ("." * node.level) + mod


class ArquitecturaTests(unittest.TestCase):
    def test_el_nucleo_no_importa_adaptadores_ni_io(self):
        archivos = list(CORE.glob("*.py"))
        self.assertTrue(archivos)
        for f in archivos:
            for imp in imports_de(f):
                limpio = imp.lstrip(".")
                if imp.startswith(".."):
                    self.fail(f"{f.name} sale del paquete core: {imp}")
                for p in PROHIBIDOS:
                    self.assertFalse(limpio == p or limpio.startswith(p + "."),
                                     f"{f.name} importa '{imp}', prohibido en el núcleo")


if __name__ == "__main__":
    unittest.main()
