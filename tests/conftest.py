"""Make the project root importable so tests can `import indicators` etc."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
