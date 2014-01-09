= SpatialDataLibrary for Robot Framework
== Description
This library is intended for testing of the quality of spatial data stored in a database with a suitable spatial library that implements `ST_` functions.

== Requirements
* Python 2.7
* Robot Framework 2.8
* DatabaseLibrary 0.6-at (Astun extended version)

== Tested environments
So far this library has only been tested in the following environments:
| Database + version | Spatial library + version |
|--------------------|---------------------------|
| PostgreSQL 9.1     | PostGIS 1.5               |

== API ==
See DatabaseLibrary documentation for lower-level actions, including connecting to a database.
*TODO*

== License
```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at


    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

