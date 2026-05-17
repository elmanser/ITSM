import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "streaming", "pipeline"))
sys.path.insert(0, os.path.join(ROOT, "streaming", "ml"))
sys.path.insert(0, os.path.join(ROOT, "streaming", "api"))
