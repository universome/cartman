import sys
from os import path

from dotenv import load_dotenv

ROOT = path.abspath(path.join(__file__, '../..'))

sys.path[0] = ROOT
load_dotenv(path.join(ROOT, '.env'))
