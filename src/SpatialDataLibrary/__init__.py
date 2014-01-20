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

import re

from DatabaseLibrary import DatabaseLibrary

from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn

builtin = BuiltIn()

__version__ = '0.1'

SCHEMA = 'public'
GEOMETRY_COLUMN = 'wkb_geometry'

class SpatialDataLibrary(DatabaseLibrary):
    """
    Unless overridden, the following are default values:
        SCHEMA = 'public'
        GEOMETRY_COLUMN = 'wkb_geometry'
    """
    def __init__(self):
        self._schema = None
        self._geometry_column = None

    def get_geometry_column(self, table, schema=SCHEMA,
                            default=GEOMETRY_COLUMN):
        """
        Finds the name for the geometry column for `table`.

        Gets the geometry column name from "geometry_columns", example:
        | ${col} | Get Geometry Column | my_table |
        | Should Be Equal | wkb_geometry | ${col} |

        If nothing is found then GEOMETRY_COLUMN is returned.

        """
        try:
            self.table_must_exist(table)
            statement = '''
                SELECT f_geometry_column
                FROM geometry_columns
                WHERE f_table_name = '{}';'''.format(table)
            return self._get_single_result(statement)
        except:
            return default


    def get_table_SRID(self, table, schema=SCHEMA,
                       geometry_column=GEOMETRY_COLUMN):
        """
        Finds the SRID for `table`
        """
        statement = 'SELECT Find_SRID({},{},{})'.format(schema, table,
                                                        geometry_column)
        srid = None
        try:
            srid = self._get_single_result(statement)
        except:
            pass
        if not srid:
            srid = self.get_query_SRID('SELECT * FROM "{}"."{}"'.format(
                                  schema, table), geometry_column)
        return srid


    def get_query_SRID(self, statement, geometry_column):
        """
        Returns the SRID for the first record returned by `statement`
        """
        statement = statement.rstrip(';')
        srid_sql = 'SELECT ST_SRID("{}") FROM ({}) AS query LIMIT 1;'
        srid_sql = srid_sql.format(geometry_column, statement)
        return self._get_single_result(srid_sql)


    def __format_source(self, source):
        """
        Formats the source for use in SQL statement.

        Assumes that if source is a string then it's a SQL query, otherwise
        the assumption is that it's something like a tuple.
        """
        if isinstance(source, basestring):
            s = '({}) AS query'.format(source)
        else:
            s = '"{0}"."{1}"'.format(*source)
        return s


    def __extent_should_equal(self, source, extent, geometry_column):
        """
        """
        s = self.__format_source(source)
        statement = 'SELECT ST_Extent("{}") FROM {};'.format(geometry_column,
                                                             s)
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


    def data_extent_should_equal(self, statement, extent,
                                 geometry_column=GEOMETRY_COLUMN):
        """
        Checks the extents of the geometries returned by a SELECT query.

        Bounding box must be specified as a comma separated string of the form
        MinX,MinY,MaxX,MaxY, for example:

        | Table Extent Should Equal | SELECT * FROM my_table WHERE type = 1 | 100000,300000,200000,400000 |
        | Table Extent Should Equal | SELECT * FROM my_table WHERE type = 1 | 100000,300000,200000,400000 | geometry_column=my_geom |

        Units are those used by the projection defined for the returned
        geometries. What happens when geometries using more than projection
        are return is undefined.

        Unlike Table Extent Should Equal, the name of the geometry column
        will always be "wkb_geometry" if not specified.

        """
        statement = statement.rstrip(';')
        self.__extent_should_equal(statement, extent, geometry_column)


    def table_extent_should_equal(self, table, extent, schema=SCHEMA,
                                  geometry_column=None):
        """

        Checks that the bounding box of a given table matches that specified.

        Bounding box must be specified as a comma separated string of the form
        MinX,MinY,MaxX,MaxY, for example:

        | Table Extent Should Equal | my_table | 100000,300000,200000,400000 |
        | Table Extent Should Equal | my_table | 100000,300000,200000,400000 | geometry_column=my_geom |

        Units are those used by the projection defined for the geometries
        (metres for the above example, where the table is in EPSG:27700).

        If no geometry column name is specified then it is looked for in the
        database, if that fails then it defaults to GEOMETRY_COLUMN.

        """

        if not geometry_column:
            geometry_column = self.get_geometry_column(table, schema=schema)
        self.__extent_should_equal((schema, table), extent, geometry_column)


    def __remove_geometry_from_columns(self, source, geometry_column,
                                       return_expr=False):
        if isinstance(source, basestring):
            # string is assumed to be query
            columns = self.describe_data(source)
        else:
            # expecting (schema, table)
            columns = self.describe_table(source[1], schema=source[0])
        column_names = []
        for column in columns:
            if column.name != geometry_column.strip('" '):
                column_names.append('"{}"'.format(column.name))
        if not column_names:
            column_names.append("'[only geometry column specified]' as message")
        if return_expr:
            result =  '''
                , '''.join(column_names)
        else:
            result = column_names
        return result


    def __contains_no_slivers(self, source, factor, geometry_column,
                              query=False):
        assert factor < 1
        column_expr = self.__remove_geometry_from_columns(source,
                                                          geometry_column,
                                                          return_expr=True)
        s = self.__format_source(source)
        statement = '''
            SELECT
                {2}
            FROM {0}
            WHERE
                ST_GeometryType({1}) IN ('ST_Polygon', 'ST_MultiPolygon')
                AND	ST_Area({1})/(
                    (
                        ST_Perimeter({1}) * ST_Perimeter({1})
                    )/(
                        4 * pi()
                    )
                ) < 0.10
            ;'''.format(s, geometry_column, column_expr)

        try:
            self.query_should_not_return_rows(statement)
        except AssertionError:
            raise AssertionError('Slivers found, see log for details')



    def query_contains_no_slivers(self, statement, factor=0.05,
                                  geometry_column=GEOMETRY_COLUMN):
        """
        Tests whether the data returned by `statement` contains 'slivers'

        If it does then an AssertionError is thrown and the rows containing
        slivers are logged.

        *Note*: the test will select only POLYGON and MULTIPOLYGON geometries
        from the results: all others are ignored. All matching rows are checked
        every time this test is run, even when one or more fails the test.

        This test works by comparing the area of the geometry to that for an
        circle with the same perimeter - the largest possible area for
        that perimeter. If the area of a geometry is smaller than the 'ideal'
        multiplied by the given `factor` then the test is failed.

        By default `factor` is equal to _0.05_ i.e. all geometries are expected
        to have an area more than 5% of the maximum. This should be tuned for a
        given query if the data contains valid narrow geometries.

        If no `geometry_column` is supplied then GEOMETRY_COLUMN is used.

        Examples:
        | Query Contains No Slivers | SELECT * FROM my_areas | | |
        | Query Contains No Slivers | SELECT * FROM my_areas | factor=0.05 | |
        | Query Contains No Slivers | SELECT * FROM my_areas | geometry_column=my_geom | |
        | Query Contains No Slivers | SELECT * FROM my_areas | factor=0.05 | geometry_column=my_geom |

        """
        statement = statement.rstrip(';')
        self.__contains_no_slivers(statement, factor, geometry_column,
                                   query=True)


    def table_contains_no_slivers(self, table, factor=0.05, schema=SCHEMA,
                                  geometry_column=None):
        """
        Tests whether the data in `table` contains 'slivers'

        See `Query Contains No Slivers` for more information, with the
        following changes:

        If no `geometry_column` is supplied then it is searched for in the
        database, if that fails then GEOMETRY_COLUMN is used.

        Examples:
        | Table Contains No Slivers | my_areas | | |
        | Table Contains No Slivers | my_areas | factor=0.05 | |
        | Table Contains No Slivers | my_areas | geometry_column=my_geom | |
        | Table Contains No Slivers | my_areas | factor=0.05 | geometry_column=my_geom |

        """
        if not geometry_column:
            geometry_column = self.get_geometry_column(table)
        self.__contains_no_slivers((schema, table), factor, geometry_column)


    def get_geometry(self, statement):
        """
        Returns a geometry in WKT format

        The `statement` **must** be a SELECT that returns a single row that
        contains only the geometry column, for example:

        | ${the_geom} | Get Geometry | SELECT wkb_geometry FROM my_table WHERE id = 1 |
        """
        re_result = re.search('SELECT (.*) FROM.*', statement, re.I)
        geometry_column = re_result.group(1)
        if geometry_column:
            logger.debug('Geometry column found: {}'.format(geometry_column))
        else:
            raise RuntimeError('Geometry column not found in {}'.format(
                               statement))
        wkt_statement = ('SELECT ST_AsText({}) FROM ({}) g;'.format(
                         geometry_column, statement.rstrip(';')))
        return self._get_single_result(wkt_statement)


    def __test_intersect(self, geometryA, geometryB):
        return self.call_function('ST_Intersects',
                                  self._value_to_text(geometryA),
                                  self._value_to_text(geometryB))


    def should_intersect(self, geometryA, geometryB):
        """
        Checks that two supplied geometries intersect

        Geometries must be specified as WKT strings, for example:
        | Should Intersect | LINESTRING ( 2 0, 0 2 ) | LINESTRING ( 0 0, 0 2 ) |

        This is probably more useful when used with `Get Geometry`:
        | ${geomA} | Get Geometry | SELECT geom FROM my_areas WHERE id = 1 |
        | ${geomB} | Get Geometry | SELECT geom FROM my_areas WHERE id = 2 |
        | Should Intersect | ${geomA} | ${geomB} |

        """
        if not self.__test_intersect(geometryA, geometryB):
            raise AssertionError('Geometries do not intersect')


    def should_not_intersect(self, geometryA, geometryB):
        """
        Checks that two supplied geometries do not intersect

        Geometries must be specified as WKT strings, for example:
        | Should Intersect | LINESTRING ( 2 0, 0 2 ) | LINESTRING ( 0 0, 0 2 ) |

        This is probably more useful when used with `Get Geometry`:
        | ${geomA} | Get Geometry | SELECT geom FROM my_areas WHERE id = 1 |
        | ${geomB} | Get Geometry | SELECT geom FROM my_areas WHERE id = 2 |
        | Should Intersect | ${geomA} | ${geomB} |

        """
        if self.__test_intersect(geometryA, geometryB):
            raise AssertionError('Geometries do not intersect')


    def __test_intersect_rows(self, geometry, source, geometry_column):

        geom= self._value_to_text(geometry)
        column_expr = self.__remove_geometry_from_columns(source,
                                                          geometry_column,
                                                          return_expr=True)
        s = self.__format_source(source)
        intersect_sql = '''
            SELECT {0}
            FROM {1}
            WHERE ST_Intersects("{2}", ST_GeomFromText({3}))
            ;'''.format(column_expr, s, geometry_column, geom)

        self.query_should_return_rows(intersect_sql)

    def should_intersect_query(self, geometry, statement,
                               geometry_column=GEOMETRY_COLUMN):
        """
        Check that `geometry` intersects with at least one `statement` feature

        Geometries must be specified as WKT strings, for example:
        | Should Intersect Query | LINESTRING ( 2 0, 0 2 ) | SELECT * FROM my_points WHERE type = 1 |

        This is probably more useful when used with `Get Geometry`:
        | ${geomA} | Get Geometry | SELECT geom FROM my_areas WHERE id = 1 |
        | Should Intersect Query | ${geomA} | SELECT * FROM my_points WHERE type = 1 |

        The `geometry_column` must be the name of the column in the results
        of the query. If not specified this defaults to GEOMETRY_COLUMN.

        A single SRID for the query is assumed and the first returned row will
        be used to obtain one for coercing `geometry` (assuming `geometry`
        doesn't contain one).

        """
        statement = statement.rstrip(';')
        if geometry[5:].upper() != 'SRID=':
            srid = self.get_query_SRID(statement, geometry_column)
            geometry = 'SRID={};{}'.format(srid, geometry)
        self.__test_intersect_rows(geometry, statement, geometry_column)


    def should_intersect_table(self, geometry, table, schema=SCHEMA,
                               geometry_column=None):
        """
        Check that `geometry` intersects with at least one feature in `table`

        Geometries must be specified as WKT strings, for example:
        | Should Intersect Query | LINESTRING ( 2 0, 0 2 ) | my_points |

        This is probably more useful when used with `Get Geometry`:
        | ${geomA} | Get Geometry | SELECT geom FROM my_areas WHERE id = 1 |
        | Should Intersect Query | ${geomA} | my_points |

        The `geometry_column` must be the name of the column in the results
        of the query. If not specified this is looked up in the database, if
        it cannot be found there, it defaults to GEOMETRY_COLUMN.

        A single SRID for the query is assumed and, if the SRID cannot be
        looked up in the database, the first returned row will be used to
        obtain one for coercing `geometry` (if `geometry` itself doesn't
        contain one).

        """
        if not geometry_column:
            geometry_column = self.get_geometry_column(table, schema=schema)
        srid = self.get_table_SRID(table, schema=schema,
                                   geometry_column=geometry_column)
        self.__test_intersect_rows(geometry, (schema, table), geometry_column)


    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
