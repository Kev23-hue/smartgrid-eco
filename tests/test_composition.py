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

    
if __name__ == "__main__":
    unittest.main()
