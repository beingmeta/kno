Name:           framerd
Version:        3.2
Release:        18%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-3.2.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  libu8-devel curl-devel libexif-devel mysql-devel
Requires:       libu8 curl libexif

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
You probably don't need it.

%package        fdserv
Summary:        Apache module for FramerD based applications
Group:          Development/Libraries
BuildRequires:  httpd-devel
Requires:       httpd %{name} = %{version}-%{release}

%description    fdserv
The %{name}-fdserv package implements the Apache mod_fdserv module for
FramerD based web applications

%package        mysql
Summary:        Module for using MySQL from FramerD
Group:          Development/Libraries
BuildRequires: mysql-devel
Requires:      mysql %{name} = %{version}-%{release}

%description    mysql
The %{name}-mysql package implements external DB bindings to the MySQL C client
libraries

%description    mysql
The %{name}-mysql package implements external DB bindings to the MySQL C client
libraries

%package        sqlite
Summary:        Module for using Sqlite3 from FramerD
Group:          Development/Libraries
BuildRequires:  sqlite-devel
Requires:       sqlite %{name} = %{version}-%{release}

%description    sqlite
The %{name}-sqlite package implements external DB bindings to the Sqlite3 library
libraries

# %package        odbc
# Summary:        Module for using Odbc from FramerD
# Group:          Development/Libraries
# BuildRequires:  unixODBC-devel
# Requires:       unixODBC %{name} = %{version}-%{release}
# 
# %description    odbc
# The %{name}-odbc package implements external DB bindings to the ODBC
# libraries
# 
# %package        tidy
# Summary:        FramerD module for tidying HTML code
# Group:          Development/Libraries
# BuildRequires:  libtidy-dev
# Requires:       libtidy %{name} = %{version}-%{release}
# 
# %description    tidy
# The %{name}-tidy package implements external bindings to libtidy
# 
# %package        ziptools
# Summary:        FramerD module for working with zipfiles
# Group:          Development/Libraries
# BuildRequires:  libzip-dev
# Requires:       libzip %{name} = %{version}-%{release}
# 
# %description    ziptools
# The %{name}-ziptools package implements external bindings for libzip

%prep
%setup -q


%build
%configure --prefix=/usr --with-admin-group=none --with-fdaemon=none --with-webuser=none --without-fastcgi --with-apacheinfo=%{_sysconfdir}/httpd/conf.d/ --with-apachelib=%{_libdir}/httpd/modules
make %{?_smp_mflags}
make mod_fdserv

%pre
if grep -q ^fdaemon /etc/passwd; then echo "User fdaemon already exists"; else useradd fdaemon; fi

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
make setup-rc.d DESTDIR=$RPM_BUILD_ROOT
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
%attr(-,fdaemon,adm) /var/run/framerd
%attr(-,fdaemon,adm) /var/log/framerd
%attr(-,fdaemon,adm) %{_prefix}/etc/framerd/servers
%attr(-,fdaemon,adm) %{_prefix}/etc/framerd/config
%attr(-,fdaemon,adm) %{_datadir}/framerd/fdconsole.el
%attr(-,fdaemon,adm) %{_datadir}/framerd/scheme_modules
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/aws/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/brico/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/brico/*.table
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/brico/*.dtype
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/facebook/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/webapi/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/tests/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/misc/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/xhtml/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/domutils/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/knowlets/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/knowlets/*.table
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/knowlets/*.dtype
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/textindex/*.scm
%attr(-,fdaemon,adm) %{_datadir}/framerd/standard_modules/safe/textindex/en.*
%{_libdir}/libfdtype.so.*
%{_libdir}/libfddb.so.*
%{_libdir}/libfddbfile.so.*
%{_libdir}/libfdbserv.so.*
%{_libdir}/libfdscheme.so.*
%{_libdir}/libfdschemeio.so.*
%{_libdir}/libfdweb.so.*
%{_libdir}/libtexttools.so.*
%{_libdir}/framerd/crypto.so*
%{_libdir}/framerd/regex.so*
%{_bindir}/fdexec
%{_bindir}/fdconsole
%{_bindir}/fdbatch
%{_bindir}/fdserv
%{_bindir}/fdbserver
%{_bindir}/fdmanager
%{_bindir}/fdsetconfig
%{_bindir}/fdgetconfig
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

# %files odbc
# %defattr(-,root,root,-)
# %doc
# %{_libdir}/framerd/odbc.so*
# 
# %files tidy
# %defattr(-,root,root,-)
# %doc
# %{_libdir}/framerd/tidy.so*
# 
# %files ziptools
# %defattr(-,root,root,-)
# %doc
# %{_libdir}/framerd/ziptools.so*

%changelog
