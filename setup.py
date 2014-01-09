#!/usr/bin/env python
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


"""Setup script for SpatialDataLibrary for Robot Framework"""

from distutils.core import setup

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from SpatialDataLibrary import __version__

def main():
    setup(name         = 'robotframework-spatialdatalibrary',
          version      = __version__,
          description  = 'Spatial data quality testing for Robot Framework',
          author       = 'Astun Technology',
          author_email = 'github@astuntechnology.com',
          url          = 'https://github.com/AstunTechnology/Robotframework-SpatialDataLibrary',
          package_dir  = { '' : 'src'},
          packages     = ['SpatialDataLibrary']
          )
        

if __name__ == "__main__":
    main()
