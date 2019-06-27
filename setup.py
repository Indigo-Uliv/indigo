# -*- coding: utf-8 -*-
"""Setup for Indigo - Project RADON version

Copyright 2019 University of Liverpool

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from distutils.core import setup
from setuptools import setup

import indigo


setup(
    name='indigo',
    version=indigo.__version__,
    description='Indigo core library',
    extras_require={},
    long_description="Core library for Indigo development",
    author='Jerome Fuselier',
    maintainer_email='jfuselie@liverpool.ac.uk',
    license="Apache License, Version 2.0",
    url="https://github.com/Indigo-Uliv/indigo",
    entry_points={
        'console_scripts': [
            "iadmin = indigo.cli:main"
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: WWW/HTTP :: WSGI",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware",
        "Topic :: System :: Archiving"
    ],
)
