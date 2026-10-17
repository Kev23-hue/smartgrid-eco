"""ES-04: agregar biomasa solo requiere un adaptador + configuración; el núcleo no cambia."""
import json
import os
import tempfile
import unittest

from edge.composition import build_from_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ComposicionTests(unittest.TestCase):
    def setUp(self):
        os.chdir(ROOT)

    def test_config_base(self):
        loop = build_from_config("edge/config/microgrid.json", audit_path=":memory:")
        r = loop.run_cycle()
        self.assertEqual(r.state.generation_kw, 140.0)  # solar 100 + eólica 40

    def test_agregar_biomasa_solo_cambia_la_configuracion(self):
        with open("edge/config/microgrid.json", encoding="utf-8") as f:
            cfg = json.load(f)
        cfg["sources"].append({"adapter": "biomasa", "kw": 30})
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as t:
            json.dump(cfg, t)
        try:
            loop = build_from_config(t.name, audit_path=":memory:")
            self.assertEqual(loop.run_cycle().state.generation_kw, 170.0)
        finally:
            os.unlink(t.name)


if __name__ == "__main__":
    unittest.main()
