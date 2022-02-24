import os, sys, pdb, shutil, logging, json, time, re
from pathlib import Path

import unittest as test
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy

from nistoar.testing import *
from nistoar.pdr import utils
from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.publish.bagger import utils as bagutils
import nistoar.pdr.exceptions as exceptions
from nistoar.pdr.publish import prov

from nistoar.pdr.publish.service import pdp

# datadir = nistoar/preserve/data
datadir = Path(__file__).parents[2] / 'preserve' / 'data'

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
#    logging.basicConfig(filename=os.path.join(tmpdir(),"test_builder.log"),
#                        level=logging.INFO)
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_bagger.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.close()
        loghdlr = None
    rmtmpdir()

def to_dict(odict):
    # converts all OrderDict instances in a Mapping into plain dicts (so that they can be tested for equality)
    out = dict(odict)
    for prop in out:
        if isinstance(out[prop], OrderedDict):
            out[prop] = to_dict(out[prop])
        if isinstance(out[prop], (list, tuple)):
            for i in range(len(out[prop])):
                if isinstance(out[prop][i], OrderedDict):
                    out[prop][i] = to_dict(out[prop][i])
    return out

tstag = prov.PubAgent("test", prov.PubAgent.AUTO, "tester")

class TestPDPublishingService(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.workdir = self.tf.mkdir("work")
        self.mintdir = self.tf.mkdir("idregs")
        self.bagparent = Path(self.workdir) / 'sipbags'
        bgrcfg = {
            "bag_builder": {
                "validate_id": True,
                "init_bag_info": {
                    'NIST-BagIt-Version': "X.3",
                    "Organization-Address": ["100 Bureau Dr.",
                                             "Gaithersburg, MD 20899"]
                },
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.6"
            },
            "finalize": {}
        }
            
        self.cfg = {
            "working_dir": self.workdir,
            "clients": {
                "ncnr": {
                    "default_shoulder": "ncnr0",
                    "localid_provider": True,
                    "auth_key": "NCNRdev"
                },
                "default": {
                    "default_shoulder": "pdp0",
                    "localid_provider": False,
                    "auth_key": "MIDASdev"
                }
            },
            "shoulders": {
                "ncnr0": {
                    "allowed_clients": [ "ncnr" ],
                    "bagger": {
                        "override_config_for": "pdp0"
                    },
                    "id_minter": {
                        "naan": "88434",
                        "sequence_start": 1
                    }
                }
                "pdp0": {
                    "allowed_clients": [ "default" ],
                    "bagger": bgrcfg,
                    "id_minter": {
                        "naan": "88434",
                        "sequence_start": 1
                    }
                }
            }
        }
        self.mntrcfg = {
            "id_shoulder": 'pdp1',
            "naan": "88888",
            "store_dir":  self.mintdir,
            "sequence_start": 17
        }
        self.minter = minter.PDP0Minter(self.mntrcfg)

    def tearDown(self):
        self.tf.clean()

    
