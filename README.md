# This is KNO

**Kno** is a platform for high-performance symbolic computing and
qualitative data processing at scale, especially suited for
development and deployment of symbolic AI services and solutions. The
core platform is implemented in C for Unix platforms (including MacOS)
and released under the open-source GPL license.

*Kno* is based on the general architecture and data model of
[http://www.framerd.org/](FramerD) which has been used extensively in
both experimental and commercial deployments supporting tens of
thousands of users.

**Kno** databases readily include millions of searchable frames with
multiple properties and relations.  These databases can be distributed
over multiple networked machines. Kno includes its own native database
formats but also provides backends to libraries such as Google's
LevelDB or RocksDB. Bundled drivers also allow Kno to connect to
external databases such as MongoDB or MySQL.

The *Kno* runtime and language kernel provides:
  * a lockless reference-counting GC suited to real-time applications;
  * full UTF-8 support with conversion from external encodings (using libu8)
  * *zero-cost* object references for operating over large graphs
    using limited memory;
  * data structures (including the lockless GC) optimized for
    multiple threads and CPUs, enabling;
  * optimized data structures and algorithms for many set, lookup, and
    search operations;
  * a simple and powerful abstract *storage layer* for representing
    graphs and graph indexes, with drivers for both native databases
	and popular open-source database libraries or services;
  * an extensible type system available from both C and other hosted
    languages;
  * **Knox**, an extension, application, and scripting language (based
    on Scheme) which leverages all of the above features, and adds:
  * support for *non-deterministic* programming, a natural paradigm
    for many kinds of search and AI algorithms;
  * a mature module system and profiling facility to support
    programming in the large;
  * *parseltongue*, an FFI interface for Python which allows Knox
    programs to transparently access Python libraries and modules;
  * native high-performance modules for text processing using a
    structured pattern language, as well as have facilities for
    stemming and morphological analysis;
  * native modules for parsing and emitting various web formats
    including XML, JSON, MIME, and various URI formats, as well as
	native access to services such as LDNS;
  * native database drivers for connecting with MongoDB and SQL
    (through both ODBC and vendor-provided drivers);
  * Apache integration through a custom `mod_knocgi` module and a
    servlet engine supporting distributed processing and asynchronous
    I/O;
  * native wrappers for:
    * image processing (imagick, qrencode, exif, etc),
	* cryptographic functions,
    * text processing (tidy, markdown,hyphenation, etc),
    * databases including leveldb, rocksdb, sqlite, etc;
    * archival data files including ziptools, libarchive, and libzip,

## Background

KNO was originally implemented as
[FramerD](https://www.beingmeta.com/pubs/FramerD.pdf) in the late
1990s at MIT's Media Laboratory. FramerD was designed to support large
knowledge bases and practical semantic information systems. The
laboratory's support, especially from its *News In the Future*
program, is gratefully acknowledged. This version can be downloaded
from [SourceForge](https://sourceforge.net/projects/framerd/).

Starting in 2005, [beingmeta](https://www.beingmeta.com/) began
developing a new version of FramerD with a focus on scalability in two
directions: down to low-powered devices and up to multi-core high
performance servers and workstations. This included optimizing the
underlying C code for modern cache and pipeline-focused CPU
architectures and the introduction of finer-grained thread locking,
implementation of a lock-free garbage collector, and a compiler to a
SCHEME-like VM.

FramerD was renamed **KNO** in May 2019 and released under an open
source license (the AGPLV2).

**beingmeta**'s implementation of KNO provides a novel query
  optimization technique called *iterated partial evaluation*
  ([patent](https://www.beingmeta.com/pubs/ipeval_patent.pdf)) which
  can optimize complex high-latency queries by up to a factor of
  ten. The method uses progressively complete partial evaluations of a
  query to bundle together data references to both reduce the number
  of round trips and allow remote data sources to optimize retrieval.

## Source modules

KNO comes bundled with modules written in it's native scripting
language (a variant of Scheme) to provide additional functions:

* access to many AWS (Amazon Web Services) APIs, including S3, SQS,
  SES, EC2 and others;
* a facility for generating and using JWTs (Javascript Web Tokens)
  including both symmetric and asymmetric (public key) signatures;
* libraries for DOM manipulation and processing, based on the
  representations generated by FramerD's native XML/HTML parser;
* an extensible generic pathname facility (`gpath`) for working with a
  range of file-like data repositories (including S3, Dropbox,
  in-memory filesystems, zip files, and web resources);
* many tools for accessing the *BRICO* semantic knowledge base;
* modules for advanced parsing and rendering of times and numbers;
* a lightweight implementation of structured records (`ezrecords`);
* facilities for fine-grained logging of program activity;
* various caching facilities for improving performance against
  resource-intensive functions or services;
* support for OAuth2 authorization and API access;
* HTTP access to CouchDB databases and RSS feeds;
* facilities for integrating with SOAP-based web services;
* interfaces to the APIs for Facebook, Gravatar, Dropbox, OpenLibrary,
  LibraryThing, Twilio (SMS), PayPal, and others.
