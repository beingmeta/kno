Name:           framerd
Version:        3.0
Release:        1%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-3.0.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  openssl-devel doxygen libu8-dev libcurl-dev libexif-dev libmysql-client-dev
Requires:       openssl libu8

%description
libu8 provides portable functions for manipulating unicode as well as
wrappers/implementations for various system-level functions such as
filesystem access, time primitives, mime handling, signature
functions, etc.

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
of a natural language tagger and analyzer for English

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
Build-Requires: apache-dev
Requires:       %{name} = %{version}-%{release}

%description    fdserv
The %{name}-fdserv package implements the Apache mod_fdserv module for
FramerD based web applications

%package        mysql
Summary:        Module for using MySQL from FramerD
Group:          Development/Libraries
Build-Requires: mysql-dev
Requires:       mysql %{name} = %{version}-%{release}

%description    mysql
The %{name}-mysql implements external DB bindings to the MySQL C client
libraries

%package        sqlite3
Summary:        Module for using Sqlite3 from FramerD
Group:          Development/Libraries
Build-Requires: sqlite3-dev
Requires:       sqlite3 %{name} = %{version}-%{release}

%description    sqlite3
The %{name}-sqlite3 implements external DB bindings to the Sqlite3 library
libraries

%package        odbc
Summary:        Module for using Odbc from FramerD
Group:          Development/Libraries
Build-Requires: unixodbc-dev
Requires:       unixodbc %{name} = %{version}-%{release}

%description    unixodbc
The %{name}-odbc implements external DB bindings to the ODBC
libraries

%prep
%setup -q


%build
%configure --prefix=/usr
make %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
make install install-scripts DESTDIR=$RPM_BUILD_ROOT
make copy-modules DESTDIR=$RPM_BUILD_ROOT
#find $RPM_BUILD_ROOT -name '*.la' -exec rm -f {} ';'


%clean
rm -rf $RPM_BUILD_ROOT


%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig


%files
%defattr(-,root,root,-)
%doc
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root,-)
%doc %{_mandir}/man3/*
%{_includedir}/*
%{_libdir}/*.so
%{_bindir}/*

%files static
%defattr(-,root,root,-)
%doc
%{_libdir}/*.a

%files mysql
%defattr(-,root,root,-)
%doc
%{_libdir}/fdb/mysql.so*

%files sqlite
%defattr(-,root,root,-)
%doc
%{_libdir}/fdb/sqlite.so*

%files odbc
%defattr(-,root,root,-)
%doc
%{_libdir}/fdb/odbc.so*

%changelog
