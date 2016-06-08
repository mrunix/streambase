#
# (C) 2007-2010 TaoBao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# oceanbase.spec is for what ...
#
# Version: $id$
#
# Authors:
#   MaoQi maoqi@taobao.com
#

Name: %NAME
Version: %VERSION
Release: %{RELEASE}
#Release: 1
Summary: TaoBao distributed database
Group: Application
URL: http:://yum.corp.alimama.com
Packager: taobao
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root
BuildRequires: t-csrd-tbnet-devel >= 1.0.4 LZO >= 2.03 Snappy >= 1.0.2 numactl-devel >= 0.9.8 libaio-devel >= 0.3

%description
OceanBase is a distributed database 

%define _unpackaged_files_terminate_build 0

%prep
%setup

%build
chmod u+x build.sh
./build.sh init
./configure RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common
#./configure --prefix=%{_prefix} --with-test-case=no --with-release=yes
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install
cp %{_sourcedir}/oceanbase-client-rm-1.1.1-SNAPSHOT.jar $RPM_BUILD_ROOT%{_prefix}/lib

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755, admin, admin)
%{_prefix}/bin/rootserver
%{_prefix}/bin/updateserver
%{_prefix}/bin/mergeserver
%{_prefix}/bin/chunkserver
%{_prefix}/bin/rs_admin
%{_prefix}/bin/ups_admin
%{_prefix}/bin/cs_admin
%{_prefix}/bin/ob_ping
%{_prefix}/bin/ups_mon
%{_prefix}/bin/str2checkpoint
%{_prefix}/bin/checkpoint2str
%{_prefix}/bin/schema_reader
%{_prefix}/bin/lsyncserver
%{_prefix}/bin/msyncclient
%{_prefix}/lib
%{_prefix}/etc
