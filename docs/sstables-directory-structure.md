
Introduction
============

SSTables are stored as a set of regular files in the filesystem in
a common directory per-table (a.k.a. column family).

In addition to SSTable files, sub-directories of the table base directory
are used for additional features such as snapshots, and atomic deletions
recovery.

This document summarizes the directory structure and file organization of SSTables.

SSTable Files
=============

SSTables are comprised of multiple component files.
The component file names are self-identifying and denote the component type, as well as per-sstable-format metadata.

Here are the different component types and their naming convention:

Data (`Data.db`)
    The SSTable data.

Primary Index (`Index.db`)
    Index of the row keys with pointers to their positions in the data file.

Bloom filter (`Filter.db`)
    A structure stored in memory that checks if row data exists in the memtable before accessing SSTables on disk.

Compression Information (`CompressionInfo.db`)
    A file holding information about uncompressed data length, chunk offsets and other compression information.

Statistics (`Statistics.db`)
    Statistical metadata about the content of the SSTable and encoding statistics for the data file, starting with the mc format.

Digest (`Digest.crc32`, `Digest.adler32`, `Digest.sha1`)
    A file holding checksum of the data file.

CRC (`CRC.db`)
    A file holding the CRC32 for chunks in an uncompressed file.

SSTable Index Summary (`Summary.db`)
    A sample of the partition index stored in memory.

SSTable Table of Contents (`TOC.txt`)
    A file that stores the list of all components for the SSTable TOC.
    Notes:
    a. When created and initially written, the table of contents is stored in a TemporaryTOC file - TOC.txt.tmp
       It is renamed to TOC.txt when the sstable is sealed and all components are flushed to stable storage and ready to be used.
    b. When a sstable is removed, TOC.txt is first renamed to TOC.txt.tmp, and that atomically marks the sstable as deleted.
    c. Upon startup, the distributed loader detects partially-removed, or partially-created sstables by their temporary TOC, and it removes them. 

Scylla (`Scylla.db`)
    A file holding scylla-specific metadata about the SSTable, such as sharding information, extended features support, and sstabe-run identifier. 

The component file names also contain SSTable format version-specific information as follows:
```
    mc-<generation>-<format>-<component>
    la-<generation>-<format>-<component>
    <keyspace>-<column_family>-ka-<generation>-<component>
```

where:
    `<generation>` is the SSTable generation - a unique positive number identifying the SSTable.
    `<format>` identifies the SSTable format.  Scylla supports only `big` format at this time.

Table Sub-directories
=====================

The per-table directory may contain several sub-directories, as listed below:

Staging directory (`staging`)
    A sub-directory holding materialized views SSTables during their update process.

Snapshots directory (`snapshots`)
    A sub-directory holding snapshots of SSTables, using hard links to the actual SSTable component files in the table base directory.

Upload directory (`upload`)
    Used for ingesting external SSTables into Scylla on startup.

Temporary SSTable directory (`<generation>.sstable`)
    A directory created when writing new SSTables.  The SSTable file are created in this sub-directory and moved to the table base directory.
    On startup, any existing temporary SSTable sub-directories are automatically removed.

Pending-delete directory (`pending_delete`)
    An optional directory that may hold log files for replaying atomic deletion operations of SSTables.
    When such operation is initiated, `delete_atomically` creates a unique, temporary log file in the `pending_delete` sub-directory named:
    `sstables-<min_generation>-<max_generation>.log.tmp`, based on the SSTables to-be-deleted minimum and maximum generation numbers.
    The log file contains the list of SSTables' TOC filenames (basename only, with no leading path), one TOC per line.
    After the temporary log file if writen, flushed, and closed; it is renamed to its final name: `sstables-<min_generation>-<max_generation>.log`.
    Finally, after the SSTables are removed, the log file is removed from the `pending_delete` sub-directory.
    On startup, any temporary `pending_delete` log files are simply removed, rolling the atomic delete operation backwards (as this is an indication that the log file may be partially written and the atomic delete operation had not started to delete any SSTable.
    Sealed `pending_delete` log files are replayed, rolling the atomic delete operation forward.
