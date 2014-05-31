USDA Nutrient Database Release 26
=================================

The USDA Nutrient database comes in a very odd file format. This
project contains a Python script that transforms it into a Python
"pickle" file and a JSON file, as well as the resulting files.


I have skipped the "abbreviated file", which is simply a flattening
(denormalization) of a subset of what the other files contain into a
single table, and the 

The original source for the whole thing is:

http://www.ars.usda.gov/Services/docs.htm?docid=8964

Legal bladibla
--------------

The data in data/ is a work of the US Government, and hence not
covered by copyright. It is readily available from the USDA website,
but included here for your convenience.

The code is MIT licensed. If you want any other license, ask me
(though I can't see why).





