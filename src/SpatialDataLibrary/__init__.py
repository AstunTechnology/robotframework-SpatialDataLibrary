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

    def get_geometry_column(self, tablename):
        """
        Gets the geometry column name from "geometry_columns", example:
        | ${col} | Get Geometry Column | my_table |
        | Should Be Equal | wkb_geometry | ${col} |

        """
        self.table_must_exist(tablename)
        statement = '''
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_name = '{}';'''.format(tablename)
        return self._get_single_result(statement)


    def table_extent_should_equal(self, tablename, extent,
                                  geometry_column=None):
        """

        Checks that the bounding box of a given table matches that specified.

        Bounding box must be specified as a comma separated string of the form
        MinX,MinY,MaxX,MaxY, for example:

        | Table Extent Should Equal | my_table | 100000,300000,200000,400000 |

        Units are those used by the projection defined for the table (metres
        for the above example, where the table is in EPSG:27700).

        If no geometry column name is specified then it is looked for in the
        database, if that fails then it defaults to "wkb_geometry".

        """

        if not geometry_column:
            try:
                geometry_column = self.get_geometry_column(tablename)
            except:
                geometry_column = 'wkb_geometry'

        statement = 'SELECT ST_Extent("{}") FROM {};'.format(geometry_column,
                                                          tablename)
        bounds = extent.split(',')
        if len(bounds) != 4:
            msg = 'Wrong number of points in extent: {}'.format(len(bounds))
            raise RuntimeError(msg)
        table_extent = self._get_single_result(statement)
        table_extent = table_extent[4:-1]  # Chop 'BOX(' and  ')'
        table_extent = ','.join(table_extent.split(' '))
        table_bounds = table_extent.split(',')
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
