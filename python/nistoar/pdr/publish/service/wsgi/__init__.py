"""
The web service providing the various flavored endpoints for programmatic data publishing (PDP).
"""
from .pdp import PDPApp
from .pdp0 import PDP0App, PDP1App

app = PDPApp
