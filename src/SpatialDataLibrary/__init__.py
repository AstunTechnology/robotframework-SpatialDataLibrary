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

from DatabaseLibrary import DatabaseLibrary

from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn

builtin = BuiltIn()

__version__ = '0.1'

class SpatialDataLibrary(DatabaseLibrary):
    """
    """
    def table_bounds_should_equal(self, tablename, bbox, 
                                  geometry_column='wkb_geometry'):
        """
        
        Checks that the bounding box of a given table matches that specified
        
        Bounding box must be specified as a comma separated string of the form
        MinX,MinY,MaxX,MaxY, e.g.:
            100000,300000,200000,400000
        """
        statement = 'SELECT ST_Extent("{}") FROM {};'.format(geometry_column, 
                                                          tablename)
        bounds = bbox.split(',')
        table_bbox = self.query(statement)[0][0]
        table_bbox = table_bbox[4:-1]  # Remove 'BOX(' at start, ')' from end
        table_bbox = ','.join(table_bbox.split(' '))
        table_bounds = table_bbox.split(',')
        errors = []
        for i in range(4):
            try:
                coord = 'X' if i % 2 else 'Y'
                adj = 'Min' if i < 2 else 'Max'
                error_msg = '{}{} values don\'t match'.format(adj, coord)
                builtin.should_be_equal_as_numbers(bounds[i], 
                                                   table_bounds[i], 
                                                   msg=error_msg, values=True)
            except AssertionError as ae:
               errors.append(str(ae))
        
        if len(errors):
            raise AssertionError('\n'.join(errors))
              
        
        
    
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
