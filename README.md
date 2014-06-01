USDA Nutrient Database Release 26
=================================

The USDA Nutrient database comes in a very odd file format. This
project contains a Python script that transforms it into a number of
useful file formats. It also includes the results of that
transformation, so you don't have to do it yourself.

Formats currently available:

   * CSV (According to Python's idea of "Excel dialect")
   * JSON
   * Python pickle file
   * SQLITE3 database

The original source for the whole thing is:

http://www.ars.usda.gov/Services/docs.htm?docid=8964

Legal bladibla
--------------

The data in data/ is a work of the US Government, and hence not
covered by copyright. It is readily available from the USDA website,
but included here for your convenience.

The code is MIT licensed. If you want any other license, ask me
(though I can't see why).





