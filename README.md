process\_xlog
==============

process\_xlog is a tool for running all but the latest WAL file through a
command.  The intended use case is compressing a WAL archive populated by
pg\_receivexlog; naively compressing all files could confuse pg\_receivexlog
and create gaps in the WAL stream, thus preventing PITR from working correctly.

Probably doesn't work on non-Unix systems.

NO WARRANTY.
