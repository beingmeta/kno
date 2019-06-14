# This is KNO

**KNO** is a platform for high-performance symbolic computing at
scale, especially suited for development and deployment of symbolic AI
services and solutions. The core platform is implemented in C for Unix
platforms (including MacOS) and is released under the open-source AGPL
license.

*KNO* is based on the general architecture and data model of
[http://www.framerd.org/](FramerD) which has been stable (but growing)
for over twenty years in both experimental and commercial deployments.
Earlier versions of KNO (known as FramerD) have been deployed
commercially in contexts with high availability to tens of thousands
of users and largely maintained by in-house sysops administrators.

**KNO** databases readily include millions of searchable frames with
multiple properties and relations.  These databases can be distributed
over multiple networked machines. In experimental applications, KNO
has supported over a billion frames.

The *KNO* runtime and language kernel provides:
  * a lockless reference-counting GC suited to real-time applications;
  * an extensible type system
  * full UTF-8 support with conversion from external encodings (using libu8)
  * graph representations with *heapless nodes* allowing very large
knowledge and data graphs.
  * a simple and powerful abstract *storage layer* for representing graphs and graph indexes;
  * many optimizations for multi-threaded execution and the writing of
high-utilization threaded services and applications
  * an application language, based on Scheme, with an extensible optimization/compilation facility
  * support for *non-deterministic* programming, a paradigm 
  * high performance data structures (tables, sets, etc) and components (e.g. bloom filters)
  * native modules for text analysis and web format parsing (including XML, JSON, and MIME)
  * native wrappers for:
    * image processing (imagick, qrencode, exif, etc),
	* cryptographic functions,
    * text processing (tidy, markdown,hyphenation, etc),
    * external database libraries (leveldb, rocksdb, sqlite, etc), 
    * external archival data files (ziptools, libarchive, libzip, etc),
  * native database drivers for MongoDB, MySQL, ODBC, etc
  * a performant web *servlet* architecture integrated with Apache
  * a distributed processing and data model

In addition, *Kno* includes **parseltongue**, a library for using and
being used by Python libraries and applications. *Parseltongue*
provides in-memory access to Python libraries and objects as well as
enabling both the KNO runtime and modules to be accessed from Python
code directly. This effectively enables Kno to leverage the vast
variety of existing python libraries as well as it's own advanced
features and components.

## General architecture

*KNO* consists of four main components: 

* a **portable distributed schema-free graph database** designed to
  support the maintenance and sharing of knowledge bases. KNO is
  especially optimized for the sort of pointer-intensive data
  structures used by semantic networks, frame systems, and many
  intelligent agent applications. This database provides a simple and
  flexible object and index model grounded in *drivers* for both KNO's
  native database formats and external formats and services.

* a **Scheme**-based scripting language with special provisions for
  Prolog-style non-deterministic programming and other common AI
  programming tropes. It also includes various facilities from Common
  Lisp, including generic sequences and a formatted output facility
  inspired by Interlisp's `PRINTOUT`. This language and KNO's
  underlying runtime are optimized for multi-threaded programming
  with typesafe object implementations and a lock-free garbage
  collector.

* a range of high-performance utility libraries for:
  * text processing pipelines, including composable pattern matchers,
  stemmers and normalizers (e.g. Soundex or Metaphone), and
  stream-based text matching;
  * standard cryptographic functions as provided by the OpenSSL
    libraries;
  * web-centric computing including: XML/HTML, JSON, and MIME parsing;
  support for Markdown and TIDY web document processors; bindings for
  CURL, LDNS, and other network facilities.
  * integration with external databases, including SQL databases
  (MySQL, MariaDB, and SQLITE), NOSQL databases (MongoDB), and generic
  file database libraries (RocksDB and LevelDB);
  * image manipulation through bindings for the ImageMagick library and
  other utility libraries including EXIF and QRENCODE.
  * bindings for plumbing utilities like libarchive, zlib, and zip
  tools.

* a growing abundance of *source modules*, written in Scheme, for
  document processing, utility computation, and access to external
  APIs. These are listed [Source modules](below).

* **parseltongue**, a native library for directly accessing Python
  libraries and programs from within Scheme programs and modules, and
  vice versa. This is in addition to the platform's support for
  calling external C functions through `libffi`.

* an application framework for implementing web applications and
  services. In this framework, web applications use a custom Apache
  module (`mod_knoweb`) to communicate with multi-threaded KNO
  *servlets* using a binary wire protocol.

KNO is implemented in C for Unix-based platforms including Linux and
macOS.

## Background

The [first version](https://www.beingmeta.com/pubs/FramerD.pdf) of
KNO, called FramerD, was developed at
[MIT's Media Laboratory](www.media.mit.edu) by Kenneth Haase. The
laboratory's support, especially from its *News In the Future*
program, is gratefully acknowledged. This version can be downloaded
from [SourceForge](https://sourceforge.net/projects/framerd/).

Starting in 2005, [beingmeta](https://www.beingmeta.com/) began
developing a new version of this platform with a focus on scalability
both down (to low-powered devices) and up (to multi-core high
performance servers and workstations). This included optimizing the
underlying C code for modern cache and pipeline-focused CPU
architectures, the introduction of finer-grained thread locking,
implementation of a lock-free garbage collector, and a compiler to a
SCHEME-like VM.

Originally called `Enterprise FramerD` or sometimes (confusingly) just
`FramerD`, this was renamed **KNO** in May 2019 and released under an
open source license (the AGPLV2).

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
* libraries for DOM manipulation and processing, based on the
  representations generated by FramerD's native XML/HTML parser;
* many tools for accessing the BRICO semantic knowledge base;
* an extensible generic pathname facility (`gpath`) for working with a
  range of file-like data repositories (including S3, Dropbox,
  in-memory filesystems, zip files, and web resources);
* a facility for checking and generating JWTs (Javascript Web Tokens)
  including both symmetric and asymmetric (public key) signatures;
* various facilities for transforming objects to and from JSON
  (JavaScript Object Notation);
* modules for advanced parsing and rendering of times and numbers;
* a lightweight implementation of records (`ezrecords`);
* facilities for fine-grained logging of program activity;
* various caching facilities for improving performance against
  resource-intensive functions or services;
* support for OAuth2 authorization and API access;
* HTTP access to CouchDB databases and RSS feeds;
* facilities for integrating with SOAP-based web services;
* interfaces to the APIs for Facebook, Gravatar, OpenLibrary,
  LibraryThing, Twilio (SMS), PayPal, and others.
