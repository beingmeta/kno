Name:           framerd
Version:        3.2
Release:        1%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-3.2.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  libu8-devel curl-devel libexif-devel mysql-devel
Requires:       libu8 libcurl libexif

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

%package        sqlite
Summary:        Module for using Sqlite3 from FramerD
Group:          Development/Libraries
BuildRequires:  sqlite-devel
Requires:       sqlite %{name} = %{version}-%{release}

%description    sqlite
The %{name}-sqlite package implements external DB bindings to the Sqlite3 library
libraries

%package        odbc
Summary:        Module for using Odbc from FramerD
Group:          Development/Libraries
BuildRequires:  unixODBC-devel
Requires:       unixODBC %{name} = %{version}-%{release}

%description    odbc
The %{name}-odbc package implements external DB bindings to the ODBC
libraries

%prep
%setup -q


%build
%configure --prefix=/usr --with-admin-group=none --with-fdaemon=none --with-webuser=none
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
%attr(-,root,adm) /etc/init.d/framerd
%attr(-,fdaemon,adm) /var/run/framerd
%attr(-,fdaemon,adm) /var/log/framerd
%attr(-,root,adm) /var/run/fdserv
%attr(-,root,adm) /var/log/fdserv
%defattr(-,root,root,-)
%doc
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root,-)
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
%{_libdir}/framerd/mysql.so*

%files sqlite
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/sqlite.so*

%files odbc
%defattr(-,root,root,-)
%doc
%{_libdir}/framerd/odbc.so*

%changelog
