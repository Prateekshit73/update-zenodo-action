#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see AUTHORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

import json
import logging
import os
import sys

import requests
import yaml

ACCESS_TOKEN = os.getenv("ZENODO_SAND_TOKEN")
r = requests.get('https://sandbox.zenodo.org/api/deposit/depositions',
                  params={'access_token': ACCESS_TOKEN})
print(r.status_code)
# 200
print(r.json())