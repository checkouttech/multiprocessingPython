import json
import datetime
from pathlib import Path
import requests
import math
from urllib.parse import urlparse
import argparse 

def validate_url_format(arg):
    url = urlparse(arg)
    if all((url.scheme, url.netloc)):  
        return arg  # return url in case you need the parsed object
    raise argparse.ArgumentTypeError('Invalid URL')


