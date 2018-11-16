Name:           framerd
Version:        3.9.6
Release:        6%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-3.9.6.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  libu8-devel curl-devel mysql-devel libtidy-devel libldns-devel
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

%package        tagger
Summary:        Natural language analysis functions for %{name}
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description    tagger
The %{name}-tagger package contains native code implementation
of a natural language tagger and analyzer for English.

# %package        fcgi
# Summary:        FASTCGI executable for %{name}
# Group:          Development/Libraries
# Requires:       %{name} = %{version}-%{release} libfcgi
# 
# %description    fcgi
# The %{name}-fcgi provides the fdfastcgi executable which allows persistent
# web service processes communicating via the FastCGI protocol.  This is an
# alternative to the %{name}-fdserv package.

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

%package        zlib
Summary:        FramerD module for working with zipfiles
Group:          Development/Libraries
BuildRequires:  zlib-devel
Requires:       zlib %{name} = %{version}-%{release}

%description    zlib
The %{name}-zlib package implements external bindings for libz compression and decompression

#%package        ziptools
#Summary:        FramerD module for working with zipfiles
#Group:          Development/Libraries
#BuildRequires:  libzip-devel
#Requires:       libzip %{name} = %{version}-%{release}

#%description    ziptools
#The %{name}-ziptools package provides manipulation for zip-encoded files through 
#external bindings for the libzip library.

#%package        exif
#Summary:        FramerD module for getting image metadata
#Group:          Development/Libraries
#BuildRequires:  libexif-devel
#Requires:       libexif %{name} = %{version}-%{release}

#%description    exif
#The %{name}-exif package provides access to image EXIF data

#%package        odbc
#Summary:        Module for using Odbc from FramerD
#Group:          Development/Libraries
#BuildRequires:  unixODBC-devel
#Requires:       unixODBC %{name} = %{version}-%{release}
# 
#%description    odbc
#The %{name}-odbc package implements external DB bindings to the ODBC
#libraries
 
%package        tidy
Summary:        FramerD module for tidying HTML code
Group:          Development/Libraries
BuildRequires:  libtidy-devel
Requires:       libtidy %{name} = %{version}-%{release}

%description    tidy
The %{name}-tidy package implements external bindings to libtidy

%prep
%setup -q

%build
%configure --prefix=/usr --libdir=%{_libdir} --with-admin-group=none --with-fdaemon=none --with-webuser=none --without-fastcgi --with-apacheinfo=%{_sysconfdir}/httpd/conf.d/ --with-apachelib=%{_libdir}/httpd/modules --without-odbc --without-ziptools --without-qrcode --without-exif --without-imagick --without-sudo --with-bootsystem=sysv --disable-devmode
#make %{?_smp_mflags}
make
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
make copy-modules DESTDIR=$RPM_BUILD_ROOT
#find $RPM_BUILD_ROOT -name '*.la' -exec rm -f {} ';'

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig


%files
%attr(-,root,adm) /etc/init.d/framerd
%attr(-,root,adm) /etc/logrotate.d/framerd
%attr(-,fdaemon,adm) /var/run/framerd
%attr(-,fdaemon,adm) /var/log/framerd
%attr(-,fdaemon,adm) %{_prefix}/etc/framerd/servers
%attr(-,fdaemon,adm) %{_prefix}/etc/framerd/config
%attr(-,fdaemon,adm) %{_datadir}/framerd/data
%attr(-,fdaemon,adm) %{_datadir}/framerd/data/README
%attr(-,fdaemon,adm) %{_datadir}/framerd/fdconsole.el
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/local
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/installed
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/*.js
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/*.css
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/aws/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/aws/*.json
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/brico/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/brico/*.table
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/brico/*.dtype
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/facebook/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/paypal/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/twitter/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/webapi/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/tests/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/misc/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/xhtml/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/xhtml/*.css
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/domutils/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/knodules/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/knodules/*.table
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/knodules/*.dtype
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/textindex/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/modules/builtin/safe/textindex/en.*
%attr(-,fdaemon,adm) %{_datadir}/locale/es/LC_MESSAGES/framerd.mo
%attr(-,fdaemon,adm) %{_datadir}/locale/fr/LC_MESSAGES/framerd.mo
%attr(-,fdaemon,adm) %{_datadir}/locale/nl/LC_MESSAGES/framerd.mo
%{_libdir}/libfdtype.so.*
%{_libdir}/libfddb.so.*
%{_libdir}/libfddbfile.so.*
%{_libdir}/libfdbserv.so.*
%{_libdir}/libfdscheme.so.*
%{_libdir}/libfdschemeio.so.*
%{_libdir}/libfdweb.so.*
%{_libdir}/libtexttools.so.*
%{_libdir}/framerd/crypto.so*
%{_libdir}/framerd/sundown.so*
%{_libdir}/framerd/regex.so*
%{_bindir}/fdexec
%{_bindir}/fdconsole
%{_bindir}/fdbatch
%{_bindir}/fdserver
%{_bindir}/fdservlet
%{_bindir}/fdserv
%{_bindir}/fdbserver
%{_bindir}/fdsetconfig
%{_bindir}/fdgetconfig
%{_bindir}/fdconfig
%{_bindir}/pack-pool
%{_bindir}/pack-index
%{_bindir}/make-hash-index
%{_bindir}/make-oidpool
%{_bindir}/ovmerge
%defattr(-,root,root,-)
%doc

%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_libdir}/libfdtype.so
%{_libdir}/libfddb.so
%{_libdir}/libfddbfile.so
%{_libdir}/libfdbserv.so
%{_libdir}/libfdscheme.so
%{_libdir}/libfdschemeio.so
%{_libdir}/libfdweb.so
%{_libdir}/libtexttools.so
%{_libdir}/framerd/crypto.so
%{_libdir}/framerd/sundown.so
%{_libdir}/framerd/regex.so

%files static
%defattr(-,root,root,-)
%doc
%{_libdir}/libfdtype.a
%{_libdir}/libfddb.a
%{_libdir}/libfddbfile.a
%{_libdir}/libfdbserv.a
%{_libdir}/libfdscheme.a
%{_libdir}/libfdschemeio.a
%{_libdir}/libfdweb.a
%{_libdir}/libtexttools.a

%files mysql
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/mysql.so*

%files sqlite
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/sqlite.so*

%files fdserv
%defattr(-,root,root,-)
%doc
%attr(-,apache,adm) %{_var}/run/fdserv
%attr(-,apache,adm) %{_var}/log/fdserv
%{_sysconfdir}/httpd/conf.d/fdserv.conf
%{_sysconfdir}/httpd/conf.d/fdserv.load
%{_libdir}/httpd/modules/mod_fdserv.*

%files tagger
%defattr(-,root,root,-)
%doc
%{_libdir}/libtagger.so.*
%{_libdir}/libtagger.so
%{_libdir}/libtagger.a

%files zlib
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/zlib.so*
 
#%files ziptools
#%defattr(-,root,root,-)
#%doc
#%{_libdir}/framerd/ziptools.so*
 
#%files exif
#%defattr(-,root,root,-)
#%doc
#%{_libdir}/framerd/exif.so*

#%files odbc
#%defattr(-,root,root,-)
#%doc
#%{_libdir}/framerd/odbc.so*
# 
%files tidy
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/tidy.so*
 
