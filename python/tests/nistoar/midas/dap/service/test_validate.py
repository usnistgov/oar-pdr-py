import os, json, pdb, logging, tempfile
import unittest as test

import nistoar.midas.dap.service.validate as val
import nistoar.pdr as pdr
import nistoar.nerdm.constants as const

class TestLenientSchemaLoader(test.TestCase):

    def setUp(self):
        self.assertTrue(os.path.isdir(pdr.def_schema_dir))
        self.assertTrue(os.path.isfile(os.path.join(pdr.def_schema_dir, "nerdm-schema.json")))
        self.ldr = val.LenientSchemaLoader.from_directory(pdr.def_schema_dir)

    def test_loading_core(self):
        sch = self.ldr.load_schema(const.CORE_SCHEMA_URI)
        typedef = sch.get("definitions",{}).get("Resource")
        self.assertIn("properties", typedef)
        self.assertNotIn("required", typedef)

        typedef = sch.get("definitions",{}).get("Topic")
        self.assertIn("properties", typedef)
        self.assertIn("required", typedef)

    def test_loading_pub(self):
        sch = self.ldr.load_schema(const.PUB_SCHEMA_URI)
        typedef = sch.get("definitions",{}).get("PublicDataResource")
        self.assertIn("allOf", typedef)
        typedef = typedef.get("allOf", [{},{}])[1]
        self.assertIn("properties", typedef)
        self.assertNotIn("required", typedef)

    def test_loading_rls(self):
        sch = self.ldr.load_schema(const.core_schema_base+"rls/v0.3")
        typedef = sch.get("definitions",{}).get("ReleasedResource")
        self.assertIn("allOf", typedef)
        typedef = typedef.get("allOf", [{},{}])[1]
        self.assertIn("properties", typedef)
        self.assertNotIn("required", typedef)

        




                         
if __name__ == '__main__':
    test.main()
        
        
