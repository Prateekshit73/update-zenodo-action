#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see AUTHORS.md file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(
        name="zenodo-upload",
        version="0.1.0",
        packages=find_packages(),
        install_requires=[
            "requests",  # Add any other dependencies your script needs
            "PyYAML",
        ],
        entry_points={
            "console_scripts": [
                "zenodo-upload=zenodo_upload:main",  # Ensure your zenodo_upload.py has a main function
            ],
        },
    )
