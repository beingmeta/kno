Name:           framerd
Version:        3.0
Release:        1%{?dist}
Summary:        semantic development environment

Group:          System Environment/Libraries
License:        GNU GPL
URL:            http://www.beingmeta.com/
Source0:        framerd-3.0.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  openssl-devel doxygen libu8-dev
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

%package        encodings
Summary:        Extra character set encodings for %{name}
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}

%description    encodings
The %{name}-encodings package contains data files for handling
various character encodings, especially for Asian languages.

%package        static
Summary:        Static libraries for %{name}
Group:          Development/Libraries
Requires:       %{name}-devel = %{version}-%{release}

%description    static
The %{name}-static package contains static libraries for
developing statically linked applications that use %{name}.
You probably don't need it.

%prep
%setup -q


%build
%configure --prefix=/usr
make %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
make install install-docs DESTDIR=$RPM_BUILD_ROOT
#find $RPM_BUILD_ROOT -name '*.la' -exec rm -f {} ';'


%clean
rm -rf $RPM_BUILD_ROOT


%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig


%files
%defattr(-,root,root,-)
%doc
%{_libdir}/*.so.*
%{_datadir}/libu8/encodings/CP1125
%{_datadir}/libu8/encodings/CP1251
%{_datadir}/libu8/encodings/CP1252
%{_datadir}/libu8/encodings/CP1258
%{_datadir}/libu8/encodings/GREEK7
%{_datadir}/libu8/encodings/ISO_6937
%{_datadir}/libu8/encodings/ISO88591
%{_datadir}/libu8/encodings/ISO885910
%{_datadir}/libu8/encodings/ISO885911
%{_datadir}/libu8/encodings/ISO885913
%{_datadir}/libu8/encodings/ISO885914
%{_datadir}/libu8/encodings/ISO885915
%{_datadir}/libu8/encodings/ISO885916
%{_datadir}/libu8/encodings/ISO88592
%{_datadir}/libu8/encodings/ISO88593
%{_datadir}/libu8/encodings/ISO88594
%{_datadir}/libu8/encodings/ISO88595
%{_datadir}/libu8/encodings/ISO88597
%{_datadir}/libu8/encodings/ISO88598
%{_datadir}/libu8/encodings/ISO88599
%{_datadir}/libu8/encodings/KOI8
%{_datadir}/libu8/encodings/KOI8R
%{_datadir}/libu8/encodings/MACINTOSH


%files encodings
%defattr(-,root,root,-)
%doc
%{_datadir}/libu8/encodings/BIG5
%{_datadir}/libu8/encodings/EUCJP
%{_datadir}/libu8/encodings/EUCKR
%{_datadir}/libu8/encodings/EUCTW
%{_datadir}/libu8/encodings/GB2312
%{_datadir}/libu8/encodings/GBK
%{_datadir}/libu8/encodings/SHIFT_JIS
%{_datadir}/libu8/encodings/SHIFT_JISX0213

%files devel
%defattr(-,root,root,-)
%doc %{_mandir}/man3/*
%{_includedir}/*
%{_libdir}/*.so


%files static
%defattr(-,root,root,-)
%doc
%{_libdir}/*.a

%changelog
