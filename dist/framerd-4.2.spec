Name:           framerd
Version:        4.2.10
Release:        10%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-4.2.10.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  libu8-devel curl-devel mysql-devel libldns-devel leveldb-devel rocksdb-devel hunspell-devel hyphen-devel libxeif-devel qrencode-devel ImageMagic-devel libffi-devel jemalloc-devel libzip-devel libsnappy-devel mongo-c-driver-devel libzstd-devel
Requires:       libu8 >= 2.2.0 curl >= 7.15.1

%description
FramerD is a distributed knowledge base and application environment.

%package        devel
Summary:        Development files for %{name}
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description    devel
The %{name}-devel package contains libraries and header files for
developing applications that use %{name}.

%package        static
Summary:        Static libraries for %{name}
Group:          Development/Libraries
Requires:       %{name}-devel = %{version}-%{release}

%description    static
The %{name}-static package contains static libraries for
developing statically linked applications that use %{name}.
You probably don\'t need it.

%package        fdserv
Summary:        Apache module for FramerD based applications
Group:          Development/Libraries
BuildRequires:  httpd-devel
Requires:       httpd

%description    fdserv
The %{name}-fdserv package implements the Apache mod_fdserv module for
FramerD based web applications

%package        mysql
Summary:        Module for using MySQL from FramerD
Group:          Development/Libraries
BuildRequires: mysql-devel
Requires:      mysql %{name} = %{version}-%{release}

%description    mysql
The %{name}-mysql package implements external DB bindings to the MySQL
C client libraries

%package        sqlite
Summary:        Module for using Sqlite3 from FramerD
Group:          Development/Libraries
BuildRequires:  sqlite-devel
Requires:       sqlite %{name} = %{version}-%{release}

%description    sqlite
The %{name}-sqlite package implements external DB bindings to the
Sqlite3 library libraries

%package        mongodb
Summary:        Module for using MongoDB from FramerD
Group:          Development/Libraries
BuildRequires: mongo-c-driver-devel
Requires:      mongo-c-driver

%description    mongodb
The %{name}-mongodb package implements bindings to the MongoDB
client library

%package        leveldb
Summary:        Module for using LevelDB from FramerD
Group:          Development/Libraries
BuildRequires: leveldb-devel
Requires:      leveldb

%description    leveldb
The %{name}-leveldb package implements bindings to the LevelDB
database library.

%package        rocksdb
Summary:        Module for using Rocksdb from FramerD
Group:          Development/Libraries
BuildRequires: rocksdb-devel
Requires:      rocksdb

%description    rocksdb
The %{name}-rocksdb package implements bindings to the rocksDB
database library.

%package        graphics
Summary:        Graphics modules for FramerD
Group:          Development/Libraries
BuildRequires: rocksdb-devel
Requires:      rocksdb

%description    graphics
The %{name}-graphics package provides bindings for the ImageMagick
library as well libraries for accessing EXIF metadata and generating
QR codes.

%package        zlib
Summary:        FramerD module for working with zipfiles
Group:          Development/Libraries
BuildRequires:  zlib-devel
Requires:       zlib %{name} = %{version}-%{release}

%description    zlib
The %{name}-zlib package implements external bindings for libz compression and decompression

%package        ziptools
Summary:        FramerD module for working with zipfiles
Group:          Development/Libraries
BuildRequires:  libzip-devel
Requires:       libzip %{name} = %{version}-%{release}

%description    ziptools
The %{name}-ziptools package provides manipulation for zip-encoded files through 
external bindings for the libzip library.

%package        libarchive
Summary:        FramerD bindings for the libarchive multi-format library
Group:          Development/Libraries
BuildRequires:  libarchive-devel
Requires:       libarchive %{name} = %{version}-%{release}

%description    libarchive
The %{name}-archive package provides access to various archive formats
through the libarchive library.

%package        exif
Summary:        FramerD module for getting image metadata
Group:          Development/Libraries
BuildRequires:  libexif-devel
Requires:       libexif %{name} = %{version}-%{release}

%description    exif
The %{name}-exif package provides access to image EXIF data

%package        odbc
Summary:        Module for using Odbc from FramerD
Group:          Development/Libraries
BuildRequires:  unixODBC-devel
Requires:       unixODBC %{name} = %{version}-%{release}
 
%description    odbc
The %{name}-odbc package implements external DB bindings to the ODBC
libraries
 
%package        tidy
Summary:        FramerD module for tidying HTML code
Group:          Development/Libraries

%description    tidy
The %{name}-tidy package implements external bindings to libtidy

%package        markdown
Summary:        FramerD module for converting markdown to HTML
Group:          Development/Libraries

%description    markdown
The %{name}-markdown package provides access to the Sundown library for
generating HTML from markdown.

%package        hyphenate
Summary:        FramerD module for hyphentating text
Group:          Development/Libraries

%description    hyphenate
The %{name}-hyphenate package provides access to the hyphenate library which hyphenates text in multiple languages.

%prep
%setup -q

%build
%configure --prefix=/usr --libdir=%{_libdir} --without-sudo --without-chowning --with-bootsystem=systemd --with-systemd=/etc/systemd/system --without-fastcgi --with-libscm=version --with-admin-group=none --with-fdaemon=none --with-webuser=none --with-webgroup=none --with-apacheinfo=%{_sysconfdir}/httpd/conf.d/ --with-apachelib=%{_libdir}/httpd/modules --disable-devmode
make %{?_smp_mflags}
make mod_fdserv

%pre
if grep -q ^fdaemon /etc/passwd;
    then echo "User fdaemon already exists";
    else useradd -r fdaemon -c "FramerD daemon";
fi
if grep -q ^framerd /etc/group;
    then echo "Group framerd already exists";
    else groupadd -f -r framerd;
fi

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
make setup-sysv DESTDIR=$RPM_BUILD_ROOT
make install-scripts DESTDIR=$RPM_BUILD_ROOT
make install-fdserv DESTDIR=$RPM_BUILD_ROOT
make install-modules DESTDIR=$RPM_BUILD_ROOT
#find $RPM_BUILD_ROOT -name '*.la' -exec rm -f {} ';'

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%attr(-,root,adm) /etc/init.d/framerd
%attr(-,root,adm) /etc/logrotate.d/framerd
%attr(-,root,adm) /etc/systemd/system/fdaemons.service
%attr(-,root,adm) /etc/systemd/system/fdweb.service
%attr(-,root,adm) /etc/systemd/system/framerd.target
%attr(-,fdaemon,adm) /var/run/framerd/daemons
%attr(-,fdaemon,adm) /var/log/framerd/daemons
%attr(-,fdaemon,adm) /var/log/framerd/bugjar
%attr(-,fdaemon,adm) /etc/framerd/servers
%attr(-,fdaemon,adm) /etc/framerd/config
%attr(-,fdaemon,adm) /etc/framerd/boot/*
%attr(-,fdaemon,adm) %{_datadir}/framerd/gdbinit
%attr(-,fdaemon,adm) %{_datadir}/framerd/makefile.include
%attr(-,fdaemon,adm) %{_datadir}/framerd/data/README
%attr(-,fdaemon,adm) %{_datadir}/framerd/data/version
%attr(-,fdaemon,adm) %{_datadir}/framerd/fdconsole.el
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/bugjar/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/bugjar/resources/*
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/optimize/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/storage/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/xhtml/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/local
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/installed
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/aws/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/aws/templates/*.json
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/domutils/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/dropbox/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/facebook/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/fdxml/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/google/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/jwt/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/knodules/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/knodules/data/*
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/morph/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/morph/data/*
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/paypal/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/textindex/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/textindex/data/*
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/twilio/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/twitter/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/tests/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/misc/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/xhtml/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/domutils
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/knodules
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/textindex
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/fifo
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/jwt
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/optimize

%attr(-,fdaemon,adm) %{_datadir}/locale/es/LC_MESSAGES/framerd.mo
%attr(-,fdaemon,adm) %{_datadir}/locale/fr/LC_MESSAGES/framerd.mo
%attr(-,fdaemon,adm) %{_datadir}/locale/nl/LC_MESSAGES/framerd.mo
%{_libdir}/libfdcore.so.*
%{_libdir}/libfdstorage.so.*
%{_libdir}/libfddrivers.so.*
%{_libdir}/libfddbserv.so.*
%{_libdir}/libfdscheme.so.*
%{_libdir}/libfdweb.so.*
%{_libdir}/libtexttools.so.*
%{_libdir}/framerd/crypto.so*
%{_libdir}/framerd/sundown.so*
%{_bindir}/*
%defattr(-,root,root,-)
%doc

%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_libdir}/libfdcore.so
%{_libdir}/libfdstorage.so
%{_libdir}/libfddrivers.so
%{_libdir}/libfddbserv.so
%{_libdir}/libfdscheme.so
%{_libdir}/libfdweb.so
%{_libdir}/libtexttools.so

%files static
%defattr(-,root,root,-)
%doc
%{_libdir}/libfdcore.a
%{_libdir}/libfdstorage.a
%{_libdir}/libfddrivers.a
%{_libdir}/libfddbserv.a
%{_libdir}/libfdscheme.a
%{_libdir}/libfdweb.a
%{_libdir}/libtexttools.a

%files mongodb
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/mongodb.so*
%attr(-,root,adm) %{_datadir}/framerd/libscm/framerd-4.2.10/mongodb/*.scm

%files mysql
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/mysql.so*

%files sqlite
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/sqlite.so*

%files leveldb
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/leveldb.so*

%files rocksdb
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/rocksdb.so*

%files markdown
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/sundown.so*

%files graphics
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/imagick.so*
%{_libdir}/framerd/qrcode.so*
%{_libdir}/framerd/exif.so*

%files hyphenate
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/hyphenate.so*
%{_datadir}/framerd/data/hyph_en_US.dic

%files fdserv
%defattr(-,root,root,-)
%doc
%attr(-,fdaemon,adm) /var/run/framerd/servlets
%attr(-,fdaemon,adm) /var/log/framerd/servlets
%{_sysconfdir}/httpd/conf.d/fdserv.conf
%{_sysconfdir}/httpd/conf.d/fdserv.load
%{_libdir}/httpd/modules/mod_fdserv.*

%files zlib
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/zlib.so*
 
%files ziptools
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/ziptools.so*
 
%files libarchive
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/libarchive.so*
 
%files exif
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/exif.so*

%files odbc
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/odbc.so*
 
%files tidy
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/tidy.so*
