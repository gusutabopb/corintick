# corintick
Column-based datastore for historical timeseries data.

Corintick was inspired and by and aims to be simplified
 version of Man AHL's [Arcitc](https://github.com/manahl/arctic).

Corintick has a single storage engine, which is column-based and not 
versioned, similar to Arctic's TickStore. However, differently from 
TickStore, it does support arbitrary `object` dtype columns. 

It was designed mainly to dump historical financial data, can be used
to store any arbitrary historical timeseries.

**PROJECT STILL IN ALPHA AND NOT READY FOR GENERAL USAGE**

## Future goals
 - Implement a Python agnostic HTTP API so that clients don't need 
   the Python/Corintick installed locally
 - Add parsers for a few common data sources