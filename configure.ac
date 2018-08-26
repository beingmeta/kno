AC_PREREQ(2.50)
AC_INIT(include/framerd/dtype.h)
AC_PREFIX_PROGRAM(autoconf)

AC_DEFUN([AC_CHECK_USER],[
AC_MSG_CHECKING([for user $1])
  found_user="none";
  for user in $2;		    
  do
  if grep ^$user: /etc/passwd > /dev/null; 
  then
	found_user=$user;
	break;
  fi;
  done;
  $1=$found_user;
  AC_MSG_RESULT($found_user)])

AC_DEFUN([AC_CHECK_GROUP],[
AC_MSG_CHECKING([for group $1])
  found_group="none";
  for group in $2;
  do if grep ^$group: /etc/group > /dev/null; 
  then
	found_group=$group;
	break;
  fi; done;
  $1=$found_group;
  AC_MSG_RESULT($found_group)])

# Override the default value for localstatedir
AC_ARG_WITH(statedir,--with-statedir where to keep local state e.g. /var,localstatedir=$withval,localstatedir="/var")

CC=${CC}
DEFAULTMAKETARGET=update
threads_default="yes"
shared_default="yes"
shared_suffix="so"
rpath_default=$exec_prefix
tls_default="no"
global_ipeval_default="no"
enable_ipeval_default="no"
install_dir_opts="-d -m 0775"
install_file_opts="-m 0664"
install_exe_opts="-m 0775"
webuser=""
wwwinstall=""
admin_group=""
admininstall=""
fdaemon=""
fdaemoninstall=""
libc_pthreads="no"
libc_dlopen="no"
calltracko=""
curlo=""
ldnso=""
ipevalo=""
ipevalprimso=""
curlconfig=""
curl_default="no"
dns_default="no"
exifo=""
doi18n="no"
rpmflags=""
rmpdir="dist"
exif_default="no"
dynamic_cflag=""
xstatic_exe_libs=""
xstatic_scheme_libs=""
xdynamic_scheme_libs=""
scheme_builtins_init="fd_init_scheme()"
definefileinfo=""
versionsh=""
suffix=""
rpath=""
daemon_identity_default="daemon"
setup_target="setup-unix"
extra_sources=
DREENTRANT="-D_REENTRANT"
MKSTATIC="ld -r -o"
TESTLIBS=
TESTLDFLAGS=
STATICLDFLAGS=
EXEFLAGS=
EXELIBS=
BINPATH=${PATH}
RPATHFLAGS="-Wl,'-rpath \$(RPATH)'"
SHARED_LIB_TARGET=shared-libs
FDMAJOR=4
FDMINOR=2
FDRELEASE=`cat etc/release`
FDVERSION=${FDMAJOR}.${FDMINOR}.${FDRELEASE}
VERSION=$FDVERSION
RPMVERSION=${FDRELEASE}
DEBVERSION=${FDRELEASE}
mshared_suffix=so.${FDMAJOR}
mmshared_suffix=so.${FDMAJOR}.${FDMINOR}
vshared_suffix=so.${FDVERSION}
I18N=""
INSTALLI18N=""
WWWUSER="apache"
apache_modules_dir="/usr/lib/apache2/modules"
apache_conf_dir="/etc/apache2"
apache_log_dir=""
apache_modinfo="/etc/apache2"
APXSFLAGS=
A2EN="none"
builtin_module_dir=""
builtin_safe_module_dir=""
installed_module_dir=""
installed_safe_module_dir=""
local_module_dir=""
default_module_path=""
default_safe_module_path=""
module_path=""
safe_module_path=""
source_dir=""
CMODULES=""
INSTALLCMODULES=""
DBMODULES=""
INSTALLDBMODULES=""
DATAFILES="etc/version"
SUDO=""
SUINSTALL=
RPMDIR=${RPMDIR:-dist}
RPMFLAGS=${RPMFLAGS:-}
APTREPO=${APTREPO:-/srv/repo/apt}
GPG=`which gpg2 || which gpg || gpg`
GPGID=${GPGID:-E5AFF294}
GITDESCRIBE=""
TAGS="TAGS SCHEMETAGS"
LIBSCM="\${REVISION}"
default_source_dir="\$prefix/src"
default_config_dir="\$prefix/etc/framerd"
local_config="\$prefix/etc/framerd"
shared_config="\$prefix/etc/framerd"
rundir=$localstatedir/run
logdir=$localstatedir/log
framerd_rundir="$rundir/framerd"
framerd_logdir="$logdir/framerd"
daemon_logdir="$framerd_logdir/daemons"
servlet_logdir="$framerd_logdir/servlets"
daemon_rundir="$framerd_rundir/daemons"
servlet_rundir="$framerd_rundir/servlets"
default_bugjar="$framerd_logdir/bugjar"
share_dir=""
data_dir=""
archflags=""
with_ziptools_default="yes"
with_mysql_default="yes"
with_crypto_default="yes"
with_imagick_default="yes"
with_hyphenate_default="yes"
use_dtblock_default="yes"
default_bootsystem="none"
BOOTSETUP="none"
CFLAGS="-DLIBU8_SOURCE -DFRAMERD_SOURCE"
OPTFLAGS=
XOPTFLAGS=
CLANG=no
MALLOC_FLAGS="-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
MYSQL_CFLAGS=
MYSQL_LDFLAGS=
MONGODB_ENABLED=
MONGODB_BUNDLED=
MONGODB_CFLAGS=
MONGODB_LDFLAGS=
IMAGICK_CFLAGS=
IMAGICK_LDFLAGS=
LIBZIP_CFLAGS=
LIBZIP_LDFLAGS=
DEFAULTISASYNC=0
USEDTBLOCK=0
MSG=echo
debugcflags=" -Og -g3 -ggdb3 -fno-inline -fno-omit-frame-pointer "
debugldflags=""
debuglibs=""
manually_disabled=""
auto_disabled=""
force_enabled=""

test_env=
memtest_env="MALLOC_CHECK_=3 "
leaktester_env=""
TESTER=./runtest
MEMTESTER=./memtest
LEAKTESTER=./leaktest
MEMTEST_CMD=
LEAKTEST_CMD=
test_config="TESTSIZE=64"
memtest_config=
leaktester_config=

test_size=512
small_test_size=64

use_duma_lib="no"
custom_malloc=""
using_malloc=""
default_malloc=${FD_DEFAULT_MALLOC:-libc}
gperftools=""
asan=""
static_libgcc="no"
CODENAME="beingmeta"

gperftools_default="yes"

if which lsb_release 2>&1 > /dev/null; then
   CODENAME=`lsb_release -cs || echo beingmeta`
fi

if (test "$rpath_default" = "NONE") || (test "$rpath_default" = ""); then
   rpath_default=$prefix;
fi

AC_CONFIG_AUX_DIR(etc)
AC_CONFIG_HEADER(include/framerd/fdsource.h include/framerd/config.h)

AC_SUBST(DEFAULTMAKETARGET)
AC_SUBST(install_dir_opts)
AC_SUBST(install_file_opts)
AC_SUBST(install_exe_opts)
AC_SUBST(shared_suffix)
AC_SUBST(vshared_suffix)
AC_SUBST(mshared_suffix)
AC_SUBST(mmshared_suffix)
AC_SUBST(dynamic_cflag)
AC_SUBST(OPTFLAGS)
AC_SUBST(XOPTFLAGS)
AC_SUBST(rpath)
AC_SUBST(curlconfig)
AC_SUBST(calltracko)
AC_SUBST(curlo)
AC_SUBST(ldnso)
AC_SUBST(exifo)
AC_SUBST(ipevalo)
AC_SUBST(ipevalprimso)
AC_SUBST(suffix)
AC_SUBST(xstatic_exe_libs)
AC_SUBST(xstatic_scheme_libs)
AC_SUBST(xdynamic_scheme_libs)
AC_SUBST(scheme_builtins_init)
AC_SUBST(systemd_loc)
AC_SUBST(share_dir)
AC_SUBST(unpackage_dir)
AC_SUBST(builtin_module_dir)
AC_SUBST(builtin_safe_module_dir)
AC_SUBST(installed_module_dir)
AC_SUBST(installed_safe_module_dir)
AC_SUBST(local_module_dir)
AC_SUBST(local_safe_module_dir)
AC_SUBST(default_module_path)
AC_SUBST(default_safe_module_path)
AC_SUBST(config_dir)
AC_SUBST(local_config)
AC_SUBST(shared_config)
AC_SUBST(data_dir)
AC_SUBST(source_dir)
AC_SUBST(apache_log_dir)
AC_SUBST(apache_modules_dir)
AC_SUBST(apache_conf_dir)
AC_SUBST(apache_modinfo)
AC_SUBST(webuser)
AC_SUBST(wwwinstall)
AC_SUBST(admin_group)
AC_SUBST(admininstall)
AC_SUBST(fdaemon)
AC_SUBST(fdaemoninstall)
AC_SUBST(rundir)
AC_SUBST(logdir)
AC_SUBST(bugjar)
AC_SUBST(framerd_rundir)
AC_SUBST(framerd_logdir)
AC_SUBST(daemon_rundir)
AC_SUBST(daemon_logdir)
AC_SUBST(daemon_bugjar)
AC_SUBST(servlet_rundir)
AC_SUBST(servlet_logdir)
AC_SUBST(servlet_bugjar)
AC_SUBST(archflags)
AC_SUBST(versionsh)
AC_SUBST(definefileinfo)
AC_SUBST(setup_target)
AC_SUBST(extra_sources)
AC_SUBST(debugcflags)
AC_SUBST(debugldflags)
AC_SUBST(debuglibs)

AC_SUBST(test_env)
AC_SUBST(memtest_env)
AC_SUBST(leaktest_env)
AC_SUBST(TESTER)
AC_SUBST(MEMTESTER)
AC_SUBST(LEAKTESTER)
AC_SUBST(LEAKTEST_CMD)
AC_SUBST(MEMTEST_CMD)
AC_SUBST(test_config)
AC_SUBST(memtest_config)
AC_SUBST(leaktest_config)
AC_SUBST(test_size)
AC_SUBST(small_test_size)

AC_SUBST(TAGS)
AC_SUBST(DREENTRANT)
AC_SUBST(SHARED_LIB_TARGET)
AC_SUBST(SHARED_LIB)
AC_SUBST(SHARED_LIBV)
AC_SUBST(test_env)
AC_SUBST(STATICLDFLAGS)
AC_SUBST(TESTLDFLAGS)
AC_SUBST(TESTLIBS)
AC_SUBST(EXEFLAGS)
AC_SUBST(EXELIBS)
AC_SUBST(RPATHFLAGS)
AC_SUBST(FDVERSION)
AC_SUBST(FDMAJOR)
AC_SUBST(FDMINOR)
AC_SUBST(FDRELEASE)
AC_SUBST(RPMVERSION)
AC_SUBST(DEBVERSION)
AC_SUBST(VERSION)
AC_SUBST(PROFILING)
AC_SUBST(MKSTATIC)
AC_SUBST(GITDESCRIBE)
AC_SUBST(I18N)
AC_SUBST(INSTALLI18N)
AC_SUBST(SUDO)
AC_SUBST(SUINSTALL)
AC_SUBST(WWWUSER)
AC_SUBST(CMODULES)
AC_SUBST(DBMODULES)
AC_SUBST(DATAFILES)
AC_SUBST(BOOTSETUP)
AC_SUBST(INSTALLCMODULES)
AC_SUBST(INSTALLDBMODULES)
AC_SUBST(MALLOC_FLAGS)
AC_SUBST(MYSQL_CFLAGS)
AC_SUBST(MYSQL_LDFLAGS)
AC_SUBST(MONGODB_BUNDLED)
AC_SUBST(MONGODB_CFLAGS)
AC_SUBST(MONGODB_LDFLAGS)
AC_SUBST(LIBZIP_CFLAGS)
AC_SUBST(LIBZIP_LDFLAGS)
AC_SUBST(IMAGICK_CFLAGS)
AC_SUBST(IMAGICK_LDFLAGS)
AC_SUBST(DEFAULTISASYNC)
AC_SUBST(DEFAULT_DLOAD_PATH)
AC_SUBST(USEDTBLOCK)
AC_SUBST(MSG)
AC_SUBST(BINDIR)
AC_SUBST(CHOWN)
AC_SUBST(CHMOD)
AC_SUBST(DELETEFILE)
AC_SUBST(COPYFILE)
AC_SUBST(TOUCHFILE)
AC_SUBST(MKDIR)
AC_SUBST(CHGRP)
AC_SUBST(CODENAME)
AC_SUBST(BINPATH)
AC_SUBST(LIBSCM)
AC_SUBST(ODBC_LIBS)

AC_ARG_VAR(RPMDIR,"Location for writing built RPMs")
AC_ARG_VAR(RPMFLAGS,"Default flags for building RPMs")
AC_ARG_VAR(GPG,"GPG executable")
AC_ARG_VAR(APTREPO,"Location of the APT repository")
AC_SUBST(RPMDIR)
AC_SUBST(RPMFLAGS)
AC_SUBST(GPGID)
AC_SUBST(APTREPO)
AC_SUBST(GPG)

AC_PATH_PROG(binenv,env)
AC_CHECK_PROG(ECHO,echo,,/bin/echo)
AC_CHECK_PROG(MKDIR_DEFAULT,mkdir,,/bin/mkdir)
AC_CHECK_PROG(CHMOD_DEFAULT,chmod,,/bin/chmod)
AC_CHECK_PROG(CHOWN_DEFAULT,chown,,/bin/chown)
AC_CHECK_PROG(CHGRP_DEFAULT,chgrp,,/bin/chgrp)
AC_CHECK_PROG(COPYFILE_DEFAULT,cp,,/bin/cp)
AC_CHECK_PROG(DELETEFILE_DEFAULT,rm,,/bin/rm)
AC_CHECK_PROG(TOUCHFILE_DEFAULT,touch,,/bin/touch)
AC_PATH_PROG(VALGRIND,valgrind,none)
AC_PATH_PROG(DUMA,duma,none)
AC_PATH_PROG(CLANG,clang,none)
AC_CHECK_PROGS(APXS,/usr/local/bin/apxs2 /usr/local/bin/apxs /usr/bin/apxs2 /usr/bin/apxs /usr/sbin/apxs2 /usr/sbin/apxs apxs2 apxs)
AC_CHECK_PROG(CREATEREPO,createrepo,,echo No createrepo for )
AC_CHECK_PROGS(LLVM_SYMBOLIZER,llvm-symbolizer-3.5,llvm-symbolizer-3.4)
AC_PATH_PROG(LLVM_SYMBOLIZER_PATH,$LLVM_SYMBOLIZER)
AC_PATH_PROG(GOOGLE_PPROF,google-pprof)
AC_PATH_PROG(XGETTEXT,xgettext)
AC_PATH_PROG(MSGMERGE,msgmerge)
AC_PATH_PROG(MSGFMT,msgfmt)

AC_ARG_WITH(mkdir,--with-mkdir command,with_mkdir=$withval,with_mkdir=$MKDIR_DEFAULT)
MKDIR=$with_mkdir
AC_ARG_WITH(chmod,--with-chmod command,with_chmod=$withval,with_chmod=$CHMOD_DEFAULT)
CHMOD=$with_chmod
AC_ARG_WITH(chown,--with-chown command,with_chown=$withval,with_chown=$CHOWN_DEFAULT)
CHOWN=$with_chown
AC_ARG_WITH(chgrp,--with-chgrp command,with_chgrp=$withval,with_chgrp=$CHGRP_DEFAULT)
CHGRP=$with_chgrp
AC_ARG_WITH(deletefile,--with-deletefile command,with_deletefile=$withval,with_deletefile=$DELETEFILE_DEFAULT)
DELETEFILE=$with_deletefile
AC_ARG_WITH(copyfile,--with-copyfile command,with_copyfile=$withval,with_copyfile=$COPYFILE_DEFAULT)
COPYFILE=$with_copyfile
AC_ARG_WITH(touchfile,--with-touchfile command,with_touchfile=$withval,with_touchfile=$TOUCHFILE_DEFAULT)
TOUCHFILE=$with_touchfile
AC_ARG_WITH(apxs,--with-apxs command,with_apxs=$withval,with_apxs=$APXS)
APXS=$with_apxs

AC_ARG_WITH(make-target,--with-make-target default-target,with_make_target=$withval,with_make_target=update)
DEFAULTMAKETARGET=$with_make_target

AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_PROG_CC([${CC} cc gcc clang])
AC_USE_SYSTEM_EXTENSIONS

AC_PROG_INSTALL
AC_PROG_RANLIB

CFLAGS="-I$prefix/include/$host/ $CFLAGS"

AC_ARG_WITH(rpath,--with-rpath dir,rpath=$withval,rpath=$rpath_default)

if test "$rpath" != "none" && test "$rpath" != "/usr" && test "$rpath" != "/usr/local"; then
  EXEFLAGS=" $EXEFLAGS \$(RPATHFLAGS) "
fi

AC_ARG_WITH(fileinfo,--with[out]-fileinfo record fileinfo,,with_fileinfo="yes")
if test "$with_fileinfo" = "yes" ; then
   definefileinfo="-D_FILEINFO=\"\\\"\`./fileinfo ./$< .\`\\\"\""
   versionsh="include/framerd/versions.h"
fi

AC_ARG_WITH(codename,--with-codename for distribution,with_codename=$withval,with_codename=$CODENAME)
CODENAME=$with_codename

AC_CANONICAL_HOST
AC_USE_SYSTEM_EXTENSIONS
AC_PROG_INSTALL
AC_PROG_RANLIB
AC_CHECK_DECL(__clang__,CLANG=yes)

if test "${CLANG}" = "yes"; then
   CFLAGS="${CFLAGS} -fno-strict-aliasing -g3 -ggdb"
   OPTFLAGS="-O2"
   XOPTFLAGS="-O3 -funroll-loops"
elif test "${GCC}" = "yes"; then
   CFLAGS="${CFLAGS} -fno-strict-aliasing -g3 -ggdb"
   OPTFLAGS="-O2"
   XOPTFLAGS="-O3 -funroll-loops"
   gcc_version=`gcc -dumpversion`;
   dynamic_cflag="-rdynamic";
fi;

m4_include([etc/ax_pthread.m4])
m4_include([etc/ax_check_link_flag.m4])
m4_include([etc/ax_python.m4])

AC_CHECK_LIB(pthread,pthread_rwlock_wrlock)

case $host_os in
  darwin*)
   MKSTATIC="libtool -static -o "
   dynamic_cflag="-dynamic"
   shared_suffix="dylib"
   vshared_suffix=${FDVERSION}.dylib
   mshared_suffix=${FDMAJOR}.dylib
   mmshared_suffix=${FDMAJOR}.${FDMINOR}.dylib
   APXS="apxs -Wc,'-arch x86_64' -Wl,'-arch x86_64'  -a $APXSFLAGS"
   CFLAGS="-D_DARWIN_C_SOURCE $CFLAGS"
   RPATHFLAGS=" -rpath \$(RPATH) "
   STATICLDFLAGS=" -lc++"
   ifmacosx=""
   setup_target="setup-osx"
   gperftools_default="no"
   ;;
esac  

AX_CHECK_LINK_FLAG("-static-libgcc",static_libgcc="yes")

if test "${static_libgcc}" == "yes"; then
   LDFLAGS="-static-libgcc -Wl,--as-needed ${LDFLAGS}"
fi

AC_ARG_WITH(stackcheck,--with-stackcheck Build with native stack checking,with_stackcheck=$withval,with_stackcheck="yes")
if ! test "${with_stackcheck}" = "no"; then
   AC_DEFINE_UNQUOTED(FD_STACKCHECK,1)
fi

AC_DEFINE_UNQUOTED(FD_VERSION,"$FDVERSION")
AC_DEFINE_UNQUOTED(FD_MAJOR_VERSION,$FDMAJOR)
AC_DEFINE_UNQUOTED(FD_MINOR_VERSION,$FDMINOR)
AC_DEFINE_UNQUOTED(FD_RELEASE_VERSION,$FDRELEASE)

AC_CHECK_PROGS(buildrpm,"lsb-rpmbuild","rpmbuild","lsb-rpm","na")
AC_CHECK_PROGS(rpm,"lsb-rpm","rpm","na")

AC_CHECK_GROUP(default_admin_group,framerd _framerd adm admin)
AC_CHECK_USER(default_fdaemon,fdaemon _fdaemon daemon nobody)
AC_CHECK_USER(default_daemon,xdaemon fdaemon _fdaemon daemon nobody)
AC_CHECK_USER(default_webuser,www apache httpd www-data _www)

AC_ARG_WITH(execwrapper,--with-execwrapper program to use when calling exec(),,with_execwrapper=$binenv)
if test "$with_execwrapper" != ""; then
  AC_DEFINE_UNQUOTED(FD_EXEC_WRAPPER,"$with_execwrapper")
fi

if test -f .ipeval; then
  enable_ipeval_default="yes;
else enable_ipeval_default="no;
fi

AC_ARG_ENABLE(thread_debug,--enable-thread-debug Use libu8 thread tracing and debugging,enable_thread_debug=$enableval,enable_thread_debug="no")

if test "${enable_thread_debug}" != "no"; then
   AC_DEFINE(U8_THREAD_DEBUG,1)
fi

AC_ARG_WITH(optinclude,--with-optinclude Use optional include directory,,with_optinclude="none")
if test $with_optinclude != "none"; then
  CPPFLAGS="$CPPFLAGS -I$with_optinclude"
  CFLAGS="$CFLAGS -I$with_optinclude"
fi

AC_ARG_WITH(optlib,--with-optlib Use optional library directory,,with_optlib="none")
if test $with_optlib != "none"; then
  LDFLAGS="$LDFLAGS -L$with_optlib"
fi

AC_ARG_WITH(sourcedir,--with-sourcedir source directory,source_dir=$withval,source_dir=$prefix/src)

AC_ARG_WITH(extra_sources,--with-extra_sources Configure extra source directories for gdb,,with_extra_sources="none")
if test $with_extra_sources != "none"; then
   extra_sources=${with_extra_sources};
fi

AC_ARG_WITH(optdir,--with-optdir Use optional header/lib,,with_optdir="/opt/local")
if test -d $with_optdir/include; then
  CPPFLAGS="$CPPFLAGS -I$with_optdir/include"
  CFLAGS="$CFLAGS -I$with_optdir/include"
  APXSFLAGS="$APXSFLAGS -I$with_optdir/include"
fi
if test -d $with_optdir/lib; then
  LDFLAGS="$LDFLAGS -L$with_optdir/lib"
fi
if test -d $with_optdir/bin; then
  PATH="$PATH:$with_optdir/bin"
fi

dnl Specifies an admin group; files written by installation scripts will be
dnl owned by this group and the group will have full rwX access.
AC_ARG_WITH(admin_group,--with-admin-group Install files with admin group and group write permissions,,with_admin_group=$default_admin_group)
if test $with_admin_group != "none"; then
  install_dir_opts="-g $with_admin_group -m 0775 $install_dir_opts"
  install_file_opts="-g $with_admin_group $install_file_opts"
  install_exe_opts="-g $with_admin_group $install_exe_opts"
  admin_group=$with_admin_group
  admininstall="-g $with_admin_group"
else
  install_dir_opts="$install_dir_opts"
  install_file_opts="$install_file_opts"
  install_exe_opts="$install_exe_opts"
  admin_group=$default_admin_group
  admininstall=""
fi

dnl Specifies a daemon identity user/group for managed FramerD servers
AC_ARG_WITH(fdaemon,--with-fdaemon The identity (user) for FramerD servers to run as,,with_fdaemon=$default_fdaemon)
if test $with_fdaemon != "none"; then
   fdaemoninstall="-o $with_fdaemon"
   fdaemon=$with_fdaemon
else
   fdaemoninstall=""
   fdaemon=$default_fdaemon
fi

dnl Specifies the user which the Web server runs as
dnl This is used to set up the directories used by fdserv
AC_ARG_WITH(webuser,--with-webuser The user for the webserver,,with_webuser=$default_webuser)
if test "$with_webuser" != "none"; then
   webuser=$with_webuser
   wwwinstall="-o $webuser"
else
   webuser=$default_webuser
   wwwinstall=""
fi

AC_MSG_CHECKING(apache log directory)
if test -d ${apache_conf_dir}/log; then
   if test -h ${apache_conf_dir}/log; then
     apache_log_dir=$(readlink ${apache_conf_dir}/log);
   else apache_log_dir=${apache_conf_dir}/log;
   fi
elif test -d ${apache_conf_dir}/logs; then
   if test -h ${apache_conf_dir}/logs; then
     apache_log_dir=$(readlink ${apache_conf_dir}/logs);
   else apache_log_dir=${apache_conf_dir}/logs;
   fi
elif test -d ${local_state_dir}/logs/apache; then
    apache_log_dir=${local_state_dir}/logs/apache;
elif test -d ${local_state_dir}/logs/apache2; then
    apache_log_dir=${local_state_dir}/logs/apache2;
elif test -d ${local_state_dir}/logs/httpd; then
    apache_log_dir=${local_state_dir}/logs/httpd;
elif test -d ${local_state_dir}/logs/http; then
    apache_log_dir=${local_state_dir}/logs/http;
elif test -d /var/log/apache; then
    apache_log_dir=/var/log/apache;
elif test -d /var/log/apache2; then
    apache_log_dir=/var/log/apache2;
elif test -d /var/log/httpd; then
    apache_log_dir=/var/log/httpd;
elif test -d /var/log/http; then
    apache_log_dir=/var/log/http;
elif test -d /usr/log/apache; then
    apache_log_dir=/usr/log/apache;
elif test -d /usr/log/apache2; then
     apache_log_dir=/usr/log/apache2;
elif test -d /usr/log/httpd; then
    apache_log_dir=/usr/log/httpd;
elif test -d /usr/log/http; then
    apache_log_dir=/usr/log/http;
else
    apache_log_dir=""
fi;

if test -z ${apache_log_dir}; then
 AC_MSG_RESULT(none found, assuming /var/log/apache2/)
 apache_log_dir=/var/log/apache;
else
 AC_MSG_RESULT($apache_log_dir);
fi;

if test ! -z "$APXS"; then
  apache_conf_dir=`$APXS -q SYSCONFDIR`
  apache_modules_dir=`$APXS -q LIBEXECDIR`
  if test -d ${apache_conf_dir}/mods-available; then
    apache_modinfo=${apache_conf_dir}/mods-available;
  elif test -d ${apache_conf_dir}/mods_available; then
    apache_modinfo=${apache_conf_dir}/mods_available;
  elif test -d ${apache_conf_dir}.d; then
    apache_modinfo=${apache_conf_dir}.d;
  elif test -d ${apache_conf_dir}/other; then
    apache_modinfo=${apache_conf_dir}/other;
  else apache_modinfo=$apache_conf_dir;
  fi;
else
  echo "Warning: No apache development environment, can't build mod_fdserv"
fi

dnl Allow overrides of APXS apacheinfo
AC_ARG_WITH(apacheinfo,--with-apacheinfo where Apache module info lives,,with_apacheinfo=$apache_modinfo)

AC_ARG_WITH(apachelib,--with-apachelib where Apache extensions live,,with_apachelib=$apache_modules_dir)

apache_modinfo=$with_apacheinfo
apache_modules_dir=$with_apachelib

dnl Specifies where server state files should live
AC_ARG_WITH(rundir,--with-rundir The default location for server state files,,with_rundir="$localstatedir/run")
rundir=$with_rundir
AC_DEFINE_UNQUOTED(FD_RUN_DIR,"$rundir")

dnl Specifies where log files should live
AC_ARG_WITH(logdir,--with-logdir The default location for log files,,with_logdir="$localstatedir/log")
logdir=$with_logdir

dnl Specifies where the default bugjar location
AC_ARG_WITH(bugjar,--with-bugjar The default location for bugjar reports,,with_bugjar="$default_bugjar")
bugjar=$with_bugjar

AC_DEFINE_UNQUOTED(FD_LOG_DIR,"$logdir")

AC_DEFINE_UNQUOTED(FD_DAEMON_LOG_DIR,"$daemon_logdir")

AC_DEFINE_UNQUOTED(FD_SERVLET_LOG_DIR,"$servlet_logdir")

AC_DEFINE_UNQUOTED(FD_DAEMON_RUN_DIR,"$daemon_rundir")

AC_DEFINE_UNQUOTED(FD_SERVLET_RUN_DIR,"$daemon_rundir")

AC_DEFINE_UNQUOTED(FD_BUGJAR_ROOT,"$bugjar")

AC_AIX

AC_ARG_WITH(duma,--with-duma Build with Detect Unintended Memory Access,with_duma=$withval,with_duma="no")

if test "$with_duma" != "no"; then
   default_malloc="libc";
fi

AC_ARG_WITH(malloc,--with-malloc Use custom malloc,with_malloc=$withval,with_malloc=$default_malloc)

AC_MSG_CHECKING(malloc library)

if test -z "$with_malloc" || 
   test "$with_malloc" = "libc" ||
   test "$with_malloc" = "builtin" ; then
   AC_MSG_RESULT(libc default)
   custom_malloc="";
   using_malloc="libc"
elif test "$with_duma" != "no"; then
   AC_MSG_ERROR(DUMA can only be used with libc malloc)
   exit;
elif test "$with_malloc" = "tcmalloc"; then
  gperftools="yes";
  custom_malloc="tcmalloc"
  using_malloc="tcmalloc"
  AC_MSG_RESULT(tcmalloc)
elif test "$with_malloc" = "gperftools"; then
  gperftools="profile";
  custom_malloc="tcmalloc"
  using_malloc="gperftools"
  AC_MSG_RESULT(tcmalloc profiling)
elif test "$with_malloc" = "tcmalloc_debug"; then
  gperftools="debug";
  custom_malloc="tcmalloc_debug"
  using_malloc="tcmalloc_debug"
  AC_MSG_RESULT(tcmalloc debug)
elif test "$with_malloc" = "tcmalloc_minimal"; then
  gperftools="minimal";
  custom_malloc="tcmalloc_minimal"
  using_malloc="tcmalloc_minimal"
  AC_MSG_RESULT(tcmalloc minimal)
elif test "$with_malloc" = "jemalloc"; then
  AC_CHECK_LIB(jemalloc,malloc,EXELIBS="-ljemalloc $EXELIBS")
  if test "${ac_cv_lib_jemalloc_malloc}" = "yes"; then
    custom_malloc="jemalloc"
    using_malloc="jemalloc"
    AC_MSG_RESULT(jemalloc)
    memtest_env="junk:true,tcache:false"
    leaktest_env="prof_leak:true,lg_prof_sample:0,prof_final:true"
  else
    AC_MSG_ERROR(jemalloc can't be found)
    exit;
  fi;
elif test "$with_malloc" = "dmalloc"; then
  AC_CHECK_LIB(dmalloc,malloc,EXELIBS="-ldmalloc $EXELIBS")
  if test "${ac_cv_lib_dmalloc_malloc}" = "yes"; then
    custom_malloc="dmalloc"
    using_malloc="dmalloc"
    AC_MSG_RESULT(dmalloc)
  else
    AC_MSG_ERROR(dmalloc can't be found)
    exit;
  fi;
elif test "$with_malloc" = "asan"; then
   CFLAGS="-fsanitize=address -fno-omit-frame-pointer -O0 -g $CFLAGS"
   LDFLAGS="-g -fsanitize=address $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan"
   using_malloc="asan"
   test_env="MEMCHECKING=yes ASAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH ASAN_OPTIONS=malloc_context_size=20 LSAN_OPTIONS=report_objects=1 $test_env";
   leaktester_env="ASAN_OPTIONS=malloc_context_size=20 LSAN_OPTIONS=report_objects=1 ${leaktester_env}";
   asan="yes"
   test_size=64
   AC_MSG_RESULT(ASAN)
elif test "$with_malloc" = "asan_noleak"; then
   CFLAGS="-fsanitize=address -fno-omit-frame-pointer -O0 -g $CFLAGS"
   LDFLAGS="-g -fsanitize=address $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan"
   using_malloc="asan"
   test_env="MEMCHECKING=yes ASAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH ASAN_OPTIONS=detect_leaks=0:malloc_context_size=20 $test_env";
   leaktester_env="ASAN_OPTIONS=malloc_context_size=20:detect_leaks=1 LSAN_OPTIONS=report_objects=1 ${leaktester_env}";
   asan="yes"
   test_size=64
   AC_MSG_RESULT(ASAN)
elif test "$with_malloc" = "asan_thread"; then
   CFLAGS="-fsanitize=thread -g -fno-omit-frame-pointer $CFLAGS"
   LDFLAGS="-g -fsanitize=thread -fno-omit-frame-pointer $LDFLAGS"
   build_static=""
   test_static=""
   install_static=""
   custom_malloc="asan_thread"
   using_malloc="asan_thread"
   test_env="MEMCHECKING=yes MSAN_SYMBOLIZER_PATH=$LLVM_SYMBOLIZER_PATH $test_env";
   asan="thread"
   AC_MSG_RESULT(ASAN thread)
else
   AC_MSG_ERROR(Valid values for malloc are libc tcmalloc jemalloc gperftools dmalloc tcmalloc_debug asan asan_thread)
   exit 1;
fi

if test ! -z "$using_malloc"; then
   AC_DEFINE_UNQUOTED(FD_DEFAULT_MALLOC,"$using_malloc")
fi;

if test ! -z "$asan" && test "$CLANG" = "none"; then
   AC_MSG_ERROR(No clang program to use with ASAN)
fi;

if test -z "$gperftools" || test "$gperftools" = "no"; then
   echo -n;
else
  AC_CHECK_HEADERS(gperftools/profiler.h)
  AC_CHECK_HEADERS(gperftools/heap-profiler.h)
  AC_CHECK_HEADERS(gperftools/heap-checker.h)
  AC_CHECK_HEADERS(gperftools/malloc_extension_c.h)
  if test "$gperftools" = "debug"; then
    AC_CHECK_LIB(tcmalloc_debug,malloc,EXELIBS="-ltcmalloc_debug $EXELIBS")
    AC_CHECK_LIB(profiler,ProfilerStart,EXELIBS="-lprofiler $EXELIBS")
  elif test "$gperftools" = "profile"; then
    AC_CHECK_LIB(tcmalloc,malloc,EXELIBS="-ltcmalloc $EXELIBS")
    AC_CHECK_LIB(profiler,ProfilerStart,EXELIBS="-lprofiler $EXELIBS")
  elif test "$gperftools" = "minimal"; then
    AC_CHECK_LIB(tcmalloc_minimal,malloc,EXELIBS="-ltcmalloc_minimal $EXELIBS")
  else
    AC_CHECK_LIB(tcmalloc,malloc)
  fi;
  if test "${ac_cv_lib_tcmalloc_malloc}" == "yes" || \
     test "${ac_cv_lib_tcmalloc_debug_malloc}" == "yes" || \
     test "${ac_cv_lib_tcmalloc_and_profiler_malloc}" == "yes"; then
    custom_malloc="tcmalloc";
    CFLAGS="${CFLAGS} ";
    leaktest_env="HEAPCHECK=normal PPROF_PATH=${GOOGLE_PPROF}";
    memtest_env="TCMALLOC_DEBUG=5";
  else
    echo "No tcmalloc available";
    custom_malloc="";
    gperftools="no";
  fi;
fi

if test "$with_duma" != "no" && test "$custom_malloc" != ""; then
  echo "Can't use DUMA with other custom MALLOC libraries ($custom_malloc)";
  with_duma="no";
elif test "$with_duma" != "no" && test "$DUMA" = "none"; then
  echo "No DUMA executable";
  with_duma="no";
elif test "$with_duma" != "no"; then
  AC_CHECK_LIB(duma,malloc)
  AC_CHECK_HEADERS(duma.h)
  test_env="DUMA_ALIGNMENT=4 $test_env";
  custom_malloc="DUMA";
fi

if test "${VALGRIND}" != "none" && test -z "${custom_malloc}"; then
  MEMTEST_CMD="FD_INVALGRIND=yes valgrind --tool=memcheck --num-callers=24"
  LEAKTESTER="FD_INVALGRIND=yes valgrind --tool=memcheck --leak-check=full --num-callers=24"
fi

AC_ARG_ENABLE(ffi,--enable-ffi enable the FFI interface,enable_ffi=$enableval,enable_ffi="yes")

if test "${enable_ffi}" == "yes"; then
   AC_CHECK_HEADERS(ffi/ffi.h)
   AC_CHECK_HEADERS(ffi.h)
   AC_CHECK_LIB(ffi,ffi_prep_cif)
fi

if test "${enable_ffi}" != "yes"; then
   manually_disabled="ffi ${manually_disabled}"
elif test ! "${ac_cv_header_ffi_h}" != "yes" &&
     test ! "${ac_cv_header_ffi_ffi_h}" != "yes";  then
   enable_ffi="no";
   auto_disabled="ffi ${auto_disabled}";
elif test "${ac_cv_lib_ffi_ffi_prep_cif}" != "yes"; then
   enable_ffi="no";
   auto_disabled="ffi ${auto_disabled}";
fi

# This cmodule isn't actually implemented yet
AC_CHECK_HEADERS(archive.h)
AC_CHECK_LIB(archive,archive_read_new)
if test "${ac_cv_header_archive_h}" == "yes" && \
   test "${ac_cv_lib_archive_archive_read_new}" == "yes"; then
  CMODULES="lib/framerd/libarchive.${shared_suffix} ${CMODULES}"
  INSTALLCMODULES="install-libarchive ${INSTALLCMODULES}"
fi

DEFAULT_DLOADPATH="${prefix}/lib/framerd/%.${vshared_suffix}:${prefix}/lib/%.${vshared_suffix}:${prefix}/lib/lib%.${vshared_suffix}:${prefix}/lib/framerd/%.${mmshared_suffix}:${prefix}/lib/%.${mmshared_suffix}:${prefix}/lib/lib%.${mmshared_suffix}:${prefix}/lib/framerd/%.${mshared_suffix}:${prefix}/lib/%.${mshared_suffix}:${prefix}/lib/lib%.${mshared_suffix}:${prefix}/lib/framerd/%.${shared_suffix}:${prefix}/lib/%.${shared_suffix}:${prefix}/lib/lib%.${shared_suffix}:"

dnl For specifying default -arch flags for OSX
AC_ARG_WITH(arch,--with[out]-arch build with particular --arch flags,,with_arch="")
if test -z $with_arch; then
  archflags=""
elif test $with_arch = "all"; then
  archflags="-arch i386 -arch x86_64 -arch ppc -arch ppc64";
elif test $with_arch = "all86"; then
  archflags="-arch i386 -arch x86_64";
elif test $with_arch = "allppc"; then
  archflags="-arch ppc -arch ppc64";
else archflags=$with_arch
fi

AC_DEFINE(FD_WORDS_ARE_ALIGNED,1)
AC_TYPE_UID_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_C_INLINE
AC_C_BIGENDIAN
AC_CHECK_SIZEOF(short,2)
AC_CHECK_SIZEOF(int,4)
AC_CHECK_SIZEOF(long,4)
AC_CHECK_SIZEOF(long long,8)
AC_CHECK_SIZEOF(void *,8)
AC_CHECK_SIZEOF(time_t,8)
AC_CHECK_SIZEOF(off_t,8)
AC_CHECK_SIZEOF(size_t,8)
AC_CHECK_TYPES(uchar)

# Check whether to use an architecture specific library directory
if test "${libdir}" != "\${exec_prefix}/lib"; then
  echo checking libdir, using explicit libdir ${libdir};
elif test ${ac_cv_sizeof_long} -eq 4 && test -d ${prefix}/lib32; then
  libdir=${prefix}/lib32;
  echo checking libdir, using 32-bit libdir ${libdir};
elif test ${ac_cv_sizeof_long} -eq 8 && test -d ${prefix}/lib64; then
  libdir=${prefix}/lib64;
  echo checking libdir, using 64-bit libdir ${libdir};
else
  echo checking libdir, using default libdir ${libdir};
fi

AC_HEADER_STDC
AC_HEADER_STAT

AC_CHECK_PROGS(etags,etags,"na")

if test $etags = "na"; then
   TAGS=""
fi

AC_CHECK_PROGS(git,"git","na")
if test $git = "na"; then
   GITDESCRIBE="false"
else
   GITDESCRIBE="$git describe"
fi

dnl Some system headers and members

AC_CHECK_HEADERS(fcntl.h malloc.h mcheck.h sys/resource.h malloc/malloc.h)
AC_CHECK_HEADERS(resource.h sys/resource.h)
AC_CHECK_HEADERS(sys/types.h sys/wait.h sys/stat.h sys/file.h signal.h)
AC_CHECK_HEADERS(sys/utsname.h sys/vfs.h sys/statfs.h sys/sysinfo.h)
AC_CHECK_HEADERS(obstack.h)

AC_CHECK_MEMBERS([struct rusage.ru_inblock, struct rusage.ru_nvcsw, struct rusage.ru_nivcsw],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct rusage.ru_majflt, struct rusage.ru_nswap],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct rusage.ru_inblock, struct rusage.ru_nvcsw, struct rusage.ru_nivcsw],[],[],[#include <sys/resource.h>])
AC_CHECK_MEMBERS([struct stat.st_mtim, struct stat.st_mtimespec],[],[],[#include <sys/stat.h>])
AC_CHECK_DECLS([RUSAGE_THREAD],[],[],[#include <sys/resource.h>])

AC_ARG_WITH(extended_profiling,--enable-rusage-profiling use extended profiling information,with_extended_profiling=$withval,with_extended_profiling="no")
if test "${with_extended_profiling}" != "no"; then
   AC_DEFINE(FD_EXTENDED_PROFILING,1)
else
   AC_DEFINE(FD_EXTENDED_PROFILING,0)
fi;

dnl Various directory/path configuration options

AC_ARG_WITH(suffix,--with-suffix string,,with_suffix="")
suffix=$with_suffix

dnl Specify an additional library
AC_ARG_WITH(extlib,
[  --with-extlib[[=DIR]]       use additional lib dir],[
  if test "$withval" != "no" -a "$withval" != "yes"; then
    LDFLAGS="${LDFLAGS} -L$withval"
  fi
])

AC_ARG_WITH(share_dir,--with-share-dir location,,with_share_dir=$prefix/share/framerd)
AC_DEFINE_UNQUOTED(FD_SHARE_DIR,"$with_share_dir")
share_dir=$with_share_dir

AC_ARG_WITH(data_dir,--with-data-dir location,,with_data_dir=$share_dir/data)
AC_DEFINE_UNQUOTED(FD_DATA_DIR,"$with_data_dir")
data_dir=$with_data_dir

if test $prefix == "/usr"; then
   default_config_dir=/etc/framerd;
else
   default_config_dir=$prefix/etc/framerd;
fi

AC_ARG_WITH(config_dir,--with-config-dir location,,with_config_dir=$default_config_dir)
config_dir=$with_config_dir

AC_ARG_WITH(local_config,--with-local-config location,,with_local_config=$config_dir/config)
local_config=$with_local_config
AC_ARG_WITH(shared_config,--with-shared-config location,,with_shared_config=$share_dir/config)
shared_config=$with_shared_config

AC_DEFINE_UNQUOTED(FD_CONFIG_FILE_PATH,"$with_local_config/%:$with_shared_config/%")

AC_ARG_WITH(unpackage_dir,--with-unpackage-dir location,,with_unpackage_dir=$share_dir/packages)
unpackage_dir=$with_unpackage_dir

AC_ARG_WITH(installed_module_dir,--with-installed-module-dir location,,with_installed_module_dir=$share_dir/modules/installed)
installed_module_dir=$with_installed_module_dir
installed_safe_module_dir=$with_installed_module_dir/safe

AC_ARG_WITH(local_modules,--with-local-modules location,,with_local_modules=$share_dir/modules/local)
local_module_dir=$with_local_modules
local_safe_module_dir=$with_local_modules/safe

AC_ARG_WITH(builtin_module_dir,--with-builtin-module-dir location,,with_builtin_module_dir=$share_dir/modules/builtin)
builtin_module_dir=$with_builtin_module_dir
builtin_safe_module_dir=$builtin_module_dir/safe

LIBSCM="${share_dir}/libscm/\${REVISION}"

AC_ARG_WITH(loadpath,--with-loadpath location)

AC_ARG_WITH(libscm,--with-libscm location)
if test "$with_libscm" == "version"; then
   AC_DEFINE_UNQUOTED(FD_LIBSCM_DIR,"$share_dir/libscm/framerd-$FDVERSION")
   LIBSCM="$share_dir/libscm/framerd-$FDVERSION";
elif test ! -z "$with_libscm"; then
   AC_DEFINE_UNQUOTED(FD_LIBSCM_DIR,"$with_libscm")
   LIBSCM=$with_libscm
fi;

builtin_module_path=$builtin_module_dir/%/module.scm:$builtin_module_dir/%.scm
builtin_safe_module_path=$builtin_module_dir/safe/%/module.scm:$builtin_module_dir/safe/%.scm

installed_module_path=$installed_module_dir/%/module.scm:$installed_module_dir/%.scm
installed_safe_module_path=$installed_safe_module_dir/%/module.scm:$installed_safe_module_dir/%.scm

local_module_path=$local_module_dir/%/module.scm:$local_module_dir/%.scm
local_safe_module_path=$local_safe_module_dir/%/module.scm:$local_safe_module_dir/%.scm

default_module_path=$local_module_path:$installed_module_path:$builtin_module_path
default_safe_module_path=$local_safe_module_path:$installed_safe_module_path:$builtin_safe_module_path

AC_ARG_WITH(module_path,--with-module-path location,,with_module_path=$default_module_path)
AC_ARG_WITH(safe_module_path,--with-safe-module-path location,,with_safe_module_path=$default_safe_module_path)
module_path=$with_module_path
safe_module_path=$with_safe_module_path

if test ! -z ${with_loadpath}; then
   module_path="${with_loadpath}:${module_path}";
fi;

AC_DEFINE_UNQUOTED(FD_LOCAL_MODULE_DIR,"$local_module_dir")
AC_DEFINE_UNQUOTED(FD_LOCAL_SAFE_MODULE_DIR,"$local_safe_module_dir")
AC_DEFINE_UNQUOTED(FD_INSTALLED_MODULE_DIR,"$installed_module_dir")
AC_DEFINE_UNQUOTED(FD_INSTALLED_SAFE_MODULE_DIR,"$installed_safe_module_dir")
AC_DEFINE_UNQUOTED(FD_BUILTIN_MODULE_DIR,"$builtin_module_dir")
AC_DEFINE_UNQUOTED(FD_BUILTIN_SAFE_MODULE_DIR,"$builtin_safe_module_dir")
AC_DEFINE_UNQUOTED(FD_UNPACKAGE_DIR,"$unpackage_dir")

AC_DEFINE_UNQUOTED(FD_DEFAULT_LOADPATH,"$module_path")
AC_DEFINE_UNQUOTED(FD_DEFAULT_SAFE_LOADPATH,"$safe_module_path")

AC_DEFINE_UNQUOTED(FD_DLOAD_SUFFIX,"$shared_suffix")

AC_ARG_WITH(dload_path,--with-dload-path location,,with_dload_path=${DEFAULT_DLOADPATH})
AC_DEFINE_UNQUOTED(FD_DEFAULT_DLOADPATH,"$with_dload_path")

AC_ARG_WITH(boot_config,--with-boot-config configstring,,with_boot_config="")
AC_DEFINE_UNQUOTED(FD_BOOT_CONFIG,"$with_boot_config")

if test $exec_prefix = "NONE"; then
  AC_DEFINE_UNQUOTED(FD_EXEC,"$prefix/bin/fdexec")
  AC_DEFINE_UNQUOTED(FD_DBSERVER,"$prefix/bin/fdserver")
else
  AC_DEFINE_UNQUOTED(FD_EXEC,"$exec_prefix/bin/fdexec")
  AC_DEFINE_UNQUOTED(FD_DBSERVER,"$prefix/bin/fdserver")
fi

AC_CHECK_FUNCS(sigprocmask)
AC_CHECK_FUNCS(sigsetmask)
AC_CHECK_FUNCS(strdup strndup)
AC_CHECK_FUNCS(open_memstream)
AC_CHECK_FUNCS(mstats mtrace muntrace mallinfo malloc_info)
AC_CHECK_FUNCS(flock lockf fcntl)
AC_CHECK_FUNCS(sysconf sysctl sysinfo)
AC_CHECK_FUNCS(uname)
AC_CHECK_FUNCS(sleep)
AC_CHECK_FUNCS(nanosleep)
AC_CHECK_FUNCS(fseeko)
AC_CHECK_FUNCS(mmap)
AC_CHECK_FUNCS(pread)
AC_CHECK_FUNCS(ftime)
AC_CHECK_FUNCS(waitpid)
AC_CHECK_FUNCS(wait4)
AC_CHECK_FUNCS(statfs)
AC_CHECK_FUNCS(gettext)

AC_CHECK_FUNCS(geteuid getuid getgid getegid getumask)
AC_CHECK_FUNCS(seteuid setuid setgid setegid)
AC_CHECK_FUNCS(getpwnam_r getpwnam)
AC_CHECK_FUNCS(getgrnam getgrnam_r)

AC_CHECK_FUNCS(pthread_tryjoin_np pthread_timedjoin_np)

# This gets it in the library list
AC_CHECK_LIB(m,floor)
AC_CHECK_LIB(z,compress2)

# Ditto, though it's almost always there
AC_CHECK_FUNCS(dlopen,libc_dlopen="yes")
if test $libc_dlopen != "yes"; then
   AC_CHECK_LIB(dl,dlopen)
fi

AC_CHECK_HEADERS(dlfcn.h)

AC_CHECK_LIB(dl,dladdr)
AC_CHECK_FUNCS(dladdr)

AC_ARG_ENABLE(maintainer-mode,--en[dis]able-maintainer-mode doesn't do anything)
AC_ARG_ENABLE(dependency-tracking,--en[dis]able-dependency-tracking doesn't do anything)

dnl Build the shared libraries.
AC_ARG_ENABLE(shared,--en[dis]able-shared Build with/without shared libraries,,enable_shared=$shared_default)
if test $enable_shared = "yes"; then
  CFLAGS="-fPIC $CFLAGS"
  SHARED_LIB_TARGET=shared-libs
else
  SHARED_LIB=
  SHARED_LIBV=
  SHARED_LIB_TARGET=
fi

dnl Enable async I/O features
AC_ARG_ENABLE(async,--en[dis]able-async Enable async I/O features)

if test "$enable_async" = "yes"; then
  DEFAULTISASYNC=1
  AC_DEFINE(FD_DEFAULT_ASYNC,1)
else
  DEFAULTISASYNC=0
  AC_DEFINE(FD_DEFAULT_ASYNC,0)
fi

dnl Enable dtblock for improved performance
AC_ARG_ENABLE(dtblock,--en[dis]able-dtblock DTBlock dtype for net I/O,,enable_dtblock=$use_dtblock_default)

if test "$enable_dtblock" = "yes"; then
  USEDTBLOCK=1
  AC_DEFINE(FD_USE_DTBLOCK,1)
else
  USEDTBLOCK=0
  AC_DEFINE(FD_USE_DTBLOCK,0)
fi

# Internationalization
AC_CHECK_HEADERS(libintl.h)
AC_CHECK_FUNCS(gettext,[doi18n="yes"])
if test "$libc_gettext" != "yes"; then
   AC_CHECK_LIB(intl,gettext)
fi

dnl Whether to build message catalogs
AC_ARG_WITH(i18n,--with[out]-i18n built and install message catalogs,,with_i18n="no")
if test $with_i18n = "yes"; then
  AC_CHECK_FUNCS(gettext,[doi18n="yes"])
fi
if test $doi18n = "yes"; then
  I18N="i18n"
  INSTALLI18N="install-i18n"
fi

if test $doi18n = "yes"; then
  I18N="i18n"
  INSTALLI18N="install-i18n"
fi

AX_PTHREAD
AX_PTHREAD(threads_default="yes",threads_default="no")

AC_ARG_WITH(mmap,--with[out]-mmap build without using mmap,,with_mmap="yes")
if test $with_mmap == "no"; then
   AC_DEFINE(FD_WITHOUT_MMAP,1)
fi

AC_CHECK_HEADERS(stdatomic.h)

AC_ARG_ENABLE(lockfree,--en[dis]able-lockfree Enables lock-free reference counting,,enable_lockfree="yes")
if test "$enable_lockfree" = "yes"; then
  AC_DEFINE(FD_ENABLE_LOCKFREE,1)
else
  AC_DEFINE(FD_ENABLE_LOCKFREE,0)
fi


dnl If possible, compile for multi-threaded execution.  This both makes
dnl  the library threadsafe and enables Scheme primitives for spawning threads
dnl  and parallel execution
AC_ARG_WITH(nptrlocks,--with-nptrlocks Sets the number of pointer hash locks to use)
if test ! -z $with_nptrlocks; then
   AC_DEFINE_UNQUOTED(FD_N_PTRLOCKS,$with_nptrlocks)
fi


dnl Use the editline library from fdconsole
AC_ARG_WITH(editline,--with[out]-editline Use the editline with fdconsole,,with_editline="yes")
if test "$with_editline" = "yes"; then
  AC_CHECK_HEADERS(editline/readline.h histedit.h)
  AC_CHECK_LIB(edit,el_init)
  AC_DEFINE(FD_WITH_EDITLINE,1)
else
  AC_DEFINE(FD_WITH_EDITLINE,0)
fi

dnl Use a different directory for libu8
AC_ARG_WITH(libu8,--with[out]-libu8 Use libu8 includes,,with_libu8="")
if test "$with_libu8" != ""; then
  CFLAGS="-I$with_libu8/include $CFLAGS"
  LDFLAGS="-L$with_libu8/lib $LDFLAGS"
  EXEFLAGS="$EXEFLAGS -L$with_libu8/lib"
fi

dnl Use sudo for installations in makefile
AC_CHECK_PROGS(sudo_program,sudo,"na")
if test "$sudoprogram" == "na"; then
  SUDO="";
else
  SUDO=$sudo_program
fi;

AC_ARG_WITH(sudo,--with[out]-sudo use sudo with install,,with_sudo="$SUDO")

if test "$with_sudo" == "yes"; then
   SUINSTALL="${SUDO} ";
elif test "$with_sudo" == "no"; then
   SUINSTALL="";
else
   SUINSTALL="$with_sudo ";
fi

with_fastcgi="yes";
AC_ARG_WITH(fastcgi,--with[out]-fastcgi Use fastcgi,with_fastcgi=no,[AC_DEFINE(WITH_FASTCGI,1)])
if test "$with_fastcgi" != "no"; then
  AC_CHECK_HEADERS(fcgiapp.h)
  AC_CHECK_LIB(fcgi,FCGX_Init)
fi

AC_CHECK_HEADERS(sql.h)

AC_CHECK_PROGS(mysql_config,mysql_config /usr/local/bin/mysql_config /opt/bin/mysql_config /opt/local/bin/mysql_config ,"na")
AC_CHECK_PROGS(pkg_config,pkg-config /usr/local/bin/pkg-config /opt/bin/pkg-config /opt/local/bin/pkg-config ,"na")

if test $mysql_config != "na"; then
   with_mysql_default="auto"
else
   with_mysql_default="no";
fi

AC_ARG_ENABLE(leveldb,--en[dis]able-leveldb Build and install LevelDB module,,enable_leveldb="yes")
leveldb_ok="no"

if test "${enable_leveldb}" = "yes" ||
   test "${enable_leveldb}" = "force"; then
   AC_CHECK_HEADERS(leveldb/c.h)
   AC_CHECK_LIB(leveldb,leveldb_open,leveldb_ok="yes")
   if test "${enable_leveldb}" = "yes" &&
      test "${ac_cv_header_leveldb_c_h}" = "yes" &&
      test "${leveldb_ok}" = "yes"; then
      DBMODULES="${DBMODULES} lib/framerd/leveldb.${shared_suffix}";
     INSTALLDBMODULES="${INSTALLDBMODULES} install-leveldb";
   elif test "${enable_leveldb}" = "force"; then
     DBMODULES="lib/framerd/leveldb.${shared_suffix} ${DBMODULES}";
     INSTALLDBMODULES="${INSTALLDBMODULES} install-leveldb";
     force_enabled="leveldb ${force_enabled}";
   else
     auto_disabled="leveldb ${auto_disabled}";
     enable_leveldb="no";
   fi;
else
   manually_disabled="leveldb ${auto_disabled}";
fi

AC_ARG_ENABLE(rocksdb,--en[dis]able-rocksdb Build and install Rocksdb module,,enable_rocksdb="yes")
rocksdb_ok="no"

if test "${enable_rocksdb}" = "yes" ||
   test "${enable_rocksdb}" = "force"; then
   AC_CHECK_HEADERS(rocksdb/c.h)
   AC_CHECK_LIB(rocksdb,rocksdb_open,rocksdb_ok="yes")
   if test "${enable_rocksdb}" = "yes" && 
      test "${ac_cv_header_rocksdb_c_h}" = "yes";
      test "${rocksdb_ok}" = "yes"; then
     DBMODULES="lib/framerd/rocksdb.${shared_suffix} ${DBMODULES}";
     INSTALLDBMODULES="install-rocksdb ${INSTALLDBMODULES}";
   elif test "${enable_rocksdb}" = "force"; then
     DBMODULES="lib/framerd/rocksdb.${shared_suffix} ${DBMODULES}";
     INSTALLDBMODULES="install-rocksdb ${INSTALLDBMODULES}";
     force_enabled="rocksdb ${force_enabled}";
   else
     auto_disabled="rocksdb ${auto_disabled}";
     enable_rocksdb="no";
   fi;
else
   manually_disabled="rocksdb ${auto_disabled}";
fi

AC_ARG_ENABLE(imagick,--en[dis]able-imagick Build and install imagemagick module,,enable_imagick="yes")

if test "$enable_imagick" = "yes" &&
   test $pkg_config != "na" &&
   $pkg_config --exists MagickWand; then
  IMAGICK_CFLAGS=`$pkg_config MagickWand --cflags`;
  IMAGICK_LDFLAGS=`$pkg_config MagickWand --libs`;
  CMODULES="lib/framerd/imagick.$shared_suffix ${CMODULES}";
  INSTALLCMODULES="install-imagick ${INSTALLCMODULES}";
elif test "$enable_imagick" = "force"; then
  CMODULES="lib/framerd/imagick.$shared_suffix ${CMODULES}";
  INSTALLCMODULES="install-imagick ${INSTALLCMODULES}";
elif test "$enable_imagick" = "yes"; then
  auto_disabled="imagick ${auto_disabled}";
  enable_imagick="failed";
else
  manually_disabled="imagick ${manually_disabled}";
fi

AC_ARG_ENABLE(ziptools,--with-ziptools Build and install ziptools,,enable_ziptools="yes")

if test "$enable_ziptools" = "yes" ||
   test "$enable_ziptools" = "force"; then
  AC_CHECK_HEADER(zip.h,have_zip_h="yes",have_zip_h="no")
  AC_CHECK_LIB(zip,zip_add,have_libzip="yes",have_libzip="no")
  AC_CHECK_FUNCS(zip_set_file_compression zip_set_file_comment zip_set_file_extra)
  if test $pkg_config != "na" &&
     $pkg_config --exists libzip; then
    LIBZIP_CFLAGS=`$pkg_config libzip --cflags` 
    LIBZIP_LDFLAGS=`$pkg_config libzip --libs` 
    CFLAGS="${CFLAGS} ${LIBZIP_CFLAGS}"
    CPPFLAGS="${CPPFLAGS} ${LIBZIP_CFLAGS}"
    LDFLAGS="${LDFLAGS} ${LIBZIP_LDFLAGS}"
    CMODULES="lib/framerd/ziptools.$shared_suffix ${CMODULES}";
    INSTALLCMODULES="install-ziptools ${INSTALLCMODULES}";
  elif test "$have_zip_h" = "yes" &&
       test "$have_libzip" = "yes"; then
    CMODULES="lib/framerd/ziptools.$shared_suffix ${CMODULES}";
    INSTALLCMODULES="install-ziptools ${INSTALLCMODULES}";
  elif test "$enable_ziptools" = "force"; then
    CMODULES="lib/framerd/ziptools.$shared_suffix ${CMODULES}";
    INSTALLCMODULES="install-ziptools ${INSTALLCMODULES}";
    force_enabled="ziptools ${force_enabled}";
  else
    auto_disabled="ziptools ${auto_disabled}";
    enable_ziptools="failed";
  fi;
else
    manually_disabled="ziptools ${manually_disabled}";
fi

AC_ARG_ENABLE(mongodb,--en[dis]able-mongodb Build and install MongoDB module,,enable_mongodb="yes")

if test "$enable_mongodb" = "yes" ||
   test "$enable_mongodb" = "force"; then
  if test $pkg_config != "na" &&
     $pkg_config --exists libmongoc-1.0; then
    MONGODB_CFLAGS=`$pkg_config libmongoc-1.0 --cflags` 
    MONGODB_LDFLAGS=`$pkg_config libmongoc-1.0 --libs` 
    DBMODULES="lib/framerd/mongodb.$shared_suffix ${DBMODULES}";
    INSTALLDBMODULES="install-mongodb ${INSTALLDBMODULES}";
    MONGODB_ENABLED=yes
  elif test "$enable_mongodb" = "force"; then
    force_enabled="mongodb ${force_enabled}";
    DBMODULES="lib/framerd/mongodb.$shared_suffix ${DBMODULES}";
    INSTALLDBMODULES="install-mongodb ${INSTALLDBMODULES}";
    MONGODB_ENABLED=yes
  else
    auto_disabled="mongodb ${auto_disabled}";
  fi;
elif test "$enable_mongodb" = "bundled"; then
    MONGODB_BUNDLED=mongodb.bundled
    MONGODB_CFLAGS="-I./include/bson -I./include/mongoc"
    MONGODB_LDFLAGS="-Wl,--whole-archive -Wl,-Bstatic -lmongoc -Wl,--no-whole-archive  -lbson -Wl,-Bdynamic"
    DBMODULES="lib/framerd/mongodb.$shared_suffix ${DBMODULES}";
    INSTALLDBMODULES="install-mongodb ${INSTALLDBMODULES}";
else
    manually_disabled="mongodb ${manually_disabled}";      
fi;

AC_ARG_WITH(mysql,--with[out]-mysql Use alternate mysql lib,,with_mysql=$with_mysql_default)

if test "$with_mysql" = "auto"; then
  MYSQL_CFLAGS=`$mysql_config --cflags`
  MYSQL_LDFLAGS=`$mysql_config --libs_r`
elif test "$with_mysql" = "yes"; then
  MYSQL_CFLAGS=\`$mysql_config --cflags\`
  MYSQL_LDFLAGS=\`$mysql_config --libs_r\`
elif test "$with_mysql" != "no"; then
  MYSQL_CFLAGS="-I$with_mysql/include"
  MYSQL_LDFLAGS="-I$with_mysql/lib"
fi

if test "$with_mysql" != "no"; then
  DBMODULES="lib/framerd/mysql.$shared_suffix ${DBMODULES}"
  INSTALLDBMODULES="install-mysql ${INSTALLDBMODULES}"
fi

AC_ARG_ENABLE(odbc,--en[dis]able-odbc Build odbc modules,,enable_odbc="yes")
ODBC_LIBS=

if test "$enable_odbc" = "yes"; then
   AC_CHECK_HEADER(sql.h,have_sql_h="yes",have_sql_h="no")
   if test "$have_sql_h" = "yes"; then
     AC_CHECK_LIB(iodbc,SQLNumResultCols,ODBC_LIBS="-liodbc")
     if test ! -z "${ODBC_LIBS}"; then
       DBMODULES="lib/framerd/odbc.$shared_suffix ${DBMODULES}"
       INSTALLDBMODULES="install-odbc ${INSTALLDBMODULES}"
     fi;
   fi;
fi;

if test "$have_sql_h" = "yes"; then
  AC_CHECK_LIB(odbc,SQLNumResultCols)
fi

AC_CHECK_HEADER(sqlite3.h,have_sqlite3_h="yes",have_sqlite3_h="no")
sqlite3_ok="no"
sqlite3v2_ok="no"

if test $have_sqlite3_h = "yes"; then
  AC_DEFINE(HAVE_SQLITE3_H,1)
  AC_CHECK_LIB(sqlite3,sqlite3_open,sqlite3_ok="yes")
  AC_CHECK_LIB(sqlite3,sqlite3_open_v2,sqlite3v2_ok="yes")
  if test "$sqlite3_ok" = "yes"; then
    DBMODULES="lib/framerd/sqlite.$shared_suffix ${DBMODULES}";
    INSTALLDBMODULES="install-sqlite ${INSTALLDBMODULES}";
    if test "${sqlite3v2_ok}" == "yes"; then
      AC_DEFINE(HAVE_SQLITE3_V2,1)
    fi;
  fi;
fi

AC_CHECK_HEADER(regex.h,have_regex_h="yes",have_regex_h="no")
if (test "$have_regex_h" = "yes"); then
  AC_CHECK_FUNC(regcomp,have_regcomp="yes",have_regcomp="no")
fi
if (test "$have_regex_h" = "yes") && (test "$have_regcomp" = "no"); then
  AC_CHECK_LIB(regex,regcomp,have_regcomp="yes",have_regcomp="no")
fi

AC_CHECK_HEADER(zlib.h,have_zlib_h="yes",have_zlib_h="no")
if (test "$have_zlib_h" = "yes"); then
  AC_CHECK_LIB(z,compress2,have_libz="yes",have_libz="no")
fi

if (test "$have_libz" = "yes"); then
  CMODULES="lib/framerd/zlib.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-zlib ${INSTALLCMODULES}"
fi

AC_CHECK_HEADER(zlib.h,have_zlib_h="yes",have_zlib_h="no")
if test "${have_zlib_h}" = "yes"; then
   AC_CHECK_LIB(zlib,uncompress)
   AC_DEFINE(HAVE_ZLIB_H,1)
fi;

AC_CHECK_HEADER(snappy-c.h,have_snappyc_h="yes",have_snappyc_h="no")
if test "${have_snappyc_h}" = "yes"; then
   AC_CHECK_LIB(snappy,snappy_compress)
   AC_DEFINE(HAVE_SNAPPYC_H,1)
fi;
LIBS="${LIBS} -lsnappy"


AC_CHECK_HEADER(zstd.h,have_zstd_h="yes",have_zstd_h="no")
if test "${have_zstd_h}" = "yes"; then
   AC_CHECK_LIB(zstd,ZSTD_decompress)
   AC_DEFINE(HAVE_ZSTD_H,1)
fi;

AC_ARG_ENABLE(tidy,--en[dis]able-tidy Build and install tidy module,,enable_tidy="yes")

if test "$enable_tidy" = "yes"; then
  CMODULES="lib/framerd/tidy.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-tidy ${INSTALLCMODULES}"
fi

AC_ARG_ENABLE(hyphenate,--en[dis]able-hyphenate Build and install the hyphenate module,,enable_hyphenate="yes")
hyphen_ok="no"

if test "$enable_hyphenate" = "yes"; then
  AC_CHECK_HEADERS(hyphen.h)
  AC_CHECK_LIB(hyphen,hnj_hyphen_hyphenate2,hyphen_ok="yes")
fi

if test "$enable_hyphenate" = "yes" &&
   test "$ac_cv_header_hyphen_h" = "yes" &&
   test "$hyphen_ok" = "yes"; then
  CMODULES="lib/framerd/hyphenate.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-hyphenate ${INSTALLCMODULES}"
  DATAFILES="src/cmodules/hyph_en_US.dic ${DATAFILES}"
elif test "$enable_hyphenate" = "force"; then
  CMODULES="lib/framerd/hyphenate.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-hyphenate ${INSTALLCMODULES}"
  DATAFILES="src/cmodules/hyph_en_US.dic ${DATAFILES}"
  force_enabled="hyphenate ${force_enabled}";
elif test "$enable_hyphenate" = "yes"; then
  auto_disabled="hyphenate $auto_disabled";
  enable_hyphenate="failed";
else
  manually_disabled="hyphenate $manually_disabled";
fi

AC_ARG_ENABLE(hunspell,--en[dis]able-hunspell Build and install the hunspell module,,enable_hunspell="yes")
hyphen_ok="no"

if test "$enable_hunspell" = "yes"; then
  AC_CHECK_HEADERS(hunspell/hunspell.h)
  AC_CHECK_LIB(hunspell,Hunspell_create,hunspell_ok="yes")
fi

if test "$enable_hunspell" = "yes" &&
   test "$ac_cv_header_hunspell_hunspell_h" = "yes" &&
   test "$hunspell_ok" = "yes"; then
  CMODULES="lib/framerd/hunspell.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-hunspell ${INSTALLCMODULES}"
elif test "$enable_hunspell" = "force"; then
  CMODULES="lib/framerd/hunspell.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-hunspell ${INSTALLCMODULES}"
  force_enabled="hunspell ${force_enabled}";
elif test "$enable_hunspell" = "yes"; then
  auto_disabled="hunspell $auto_disabled";
  enable_hunspell="failed";
else
  manually_disabled="hunspell $manually_disabled";
fi



# For OS X
AC_CHECK_HEADERS(CommonCrypto/CommonDigest.h CommonCrypto/CommonHMAC.h)
AC_CHECK_FUNCS(CCHmacInit)
# OpenSSL checks
AC_CHECK_HEADERS(openssl/hmac.h)
AC_CHECK_HEADERS(openssl/sha.h)
AC_CHECK_LIB(crypto,EVP_CIPHER_CTX_init)
AC_CHECK_FUNCS(SHA1_Init EVP_get_cipherbyname EVP_CIPHER_CTX_init)
AC_CHECK_FUNCS(CRYPTO_set_locking_callback)
# For UUID functions
AC_CHECK_LIB(uuid,uuid_generate_time)
AC_CHECK_FUNCS(uuid_generate_time)

dnl Compile for access to large files, essentially defining off_t as 64 bits.
AC_ARG_ENABLE(largefiles,--en[dis]able-largefiles Enable use of large (> 2GB) file support,,enable_largefiles=yes)
if test $enable_largefiles = "yes"; then
  AC_DEFINE(FD_LARGEFILES_ENABLED,1)
else
  AC_DEFINE(FD_LARGEFILES_ENABLED,0)
fi

AC_CHECK_LIB(u8,u8_raise)
AC_CHECK_LIB(u8io,u8_open_xinput)
AC_CHECK_LIB(u8data,u8_init_chardata_c)
dnl Check for cryptic in in libu8
AC_CHECK_FUNCS(u8_cryptic,with_crypto_default="yes")
AC_ARG_WITH(crypto,--with[out]-crypto Compile the crypto module,,with_crypto=$with_crypto_default)
if test $with_crypto = "yes"; then
  CMODULES="lib/framerd/crypto.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-crypto ${INSTALLCMODULES}"
fi

AC_ARG_WITH(sundown,--with[out]-sundown Compile the sundown module,,with_sundown="yes")
if test "$with_sundown" = "yes"; then
  CMODULES="lib/framerd/sundown.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-sundown ${INSTALLCMODULES}"
fi

dnl This compiles a version to be used with the GNU profiling tools
AC_ARG_WITH(profiling,--with/out-profiling compile to generate profile information,,with_profiling="no")
if test $with_profiling = "yes"; then
  AC_DEFINE(FD_WITH_PROFILING,1)
  PROFILING="-O0 -pg -fno-inline -fprofile-arcs -ftest-coverage"
else 
  PROFILING=""
fi

dnl This is used to disable the simplest automatic database prefetches.
dnl This is typically used to configure for benchmarking prefetch mechanisms
dnl against the base case of no prefetches at all.
AC_ARG_ENABLE(prefetching,--en[dis]able-prefetching make frame operations prefetch when they can,,enable_prefetching="yes")
if test $enable_prefetching = "yes"; then
  AC_DEFINE(FD_PREFETCHING_ENABLED,1)
else
  AC_DEFINE(FD_PREFETCHING_ENABLED,0)
fi

dnl This builds in file config lookups
AC_ARG_ENABLE(fileconfig,--en[dis]able-fileconfig enable file-based configuration,,enable_fileconfig="yes")
if test "$enable_fileconfig" = "yes"; then
  AC_DEFINE(FD_FILECONFIG_ENABLED,1)
else
  AC_DEFINE(FD_FILECONFIG_ENABLED,0)
fi

dnl This integrates the fdweb module into executables by default; otherwise
dnl  it is built and installed as a plugin library
AC_ARG_ENABLE(htmldump,--en[dis]able-htmldump write HTML backtraces,,enable_htmldump="yes")
if test $enable_htmldump = "yes"; then
  scheme_builtins_init="$scheme_builtins_init; fd_init_fdweb()"
  xdynamic_scheme_libs="$xdynamic_scheme_libs -lfdweb${suffix}"
  xstatic_exe_libs="lib/libfdweb${suffix}.a $xstatic_exe_libs"
  xstatic_scheme_libs=" ../../lib/libfdweb${suffix}.a $xstatic_scheme_libs"
  AC_DEFINE(FD_HTMLDUMP_ENABLED,1)
fi

dnl ***********************************
dnl *** Checks for working __thread ***
dnl ***********************************
AC_MSG_CHECKING(for working __thread)
AC_TRY_COMPILE([
            #include <pthread.h>
            __thread int i;
	    static int res1, res2;
	],[
	     void thread_main (void *arg)
	     {
		i = arg;
		sleep (1);
		if (arg == 1) res1 = (i == arg);
		else res2 = (i == arg);
	     }

	     int main () {
		pthread_t t1, t2;
 		i = 5;
		pthread_create (&t1, NULL, thread_main, 1);
		pthread_create (&t2, NULL, thread_main, 2);

		pthread_join (t1, NULL);
		pthread_join (t2, NULL);
		return !(res1 + res2 == 2);
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_THREAD_STORAGE_CLASS)],
	       [AC_MSG_RESULT(no)])


dnl ***********************************
dnl *** Checks for working __builtin_expect ***
dnl ***********************************
AC_MSG_CHECKING(for working __builtin_expect)
AC_TRY_COMPILE([],[
	     int main (int argc,char *argv[]) {
                if (__builtin_expect((argc>2),1)) return 0;
                else return 1;
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_BUILTIN_EXPECT)],
	       [AC_MSG_RESULT(no)])


dnl ***********************************
dnl *** Checks for working __builtin_prefetch ***
dnl ***********************************
AC_MSG_CHECKING(for working __builtin_prefetch)
AC_TRY_COMPILE([],[
	     static int data[]={0,1,2,3};

	     int main (int argc,char *argv[]) {
                __builtin_prefetch(&data[2],0,3);
		return 0;
		}
	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_BUILTIN_PREFETCH)],
	       [AC_MSG_RESULT(no)])


dnl ***********************************
dnl *** Checks for working constructor attributes ***
dnl ***********************************
AC_MSG_CHECKING(for working __constructor_attributes)
AC_TRY_COMPILE([],[
             int tmpinit(void) __attribute__ ((constructor));

	    ], [
		AC_MSG_RESULT(yes)
		AC_DEFINE(HAVE_CONSTRUCTOR_ATTRIBUTES)],
	       [AC_MSG_RESULT(no)])

dnl This forces the use of the threadlocal storage functions from the pthreads
dnl  library.  Otherwise, the executable will use __thread storage declarations
dnl  when available
AC_ARG_WITH(tls,--with[out]-tls Force use of threadlocal storage,,with_tls=$tls_default)
if test $with_tls = "yes"; then
  AC_DEFINE(FD_FORCE_TLS,1)
else
  AC_DEFINE(FD_FORCE_TLS,0)
fi

dnl This enables the ipeval implementation within FramerD
AC_ARG_ENABLE(ipeval,--en[dis]able-ipeval Enable IPEVAL,,enable_ipeval=$enable_ipeval_default)
if test $enable_ipeval = "yes"; then
  ipevalo=src/storage/ipeval.o
  ipevalprimso=src/prims/ipevalprims.o
  AC_DEFINE(FD_IPEVAL_ENABLED,1)
else
  AC_DEFINE(FD_IPEVAL_ENABLED,0)
fi

dnl This forces the use of a global lock for ipeval, which can reduce
dnl  parallellism  but may help track down subtle bugs
AC_ARG_WITH(global_ipeval,--with[out]-global-ipeval Use global lock and variables for ipeval,,with_global_ipeval=$global_ipeval_default)
if test $with_global_ipeval = "yes"; then
  AC_DEFINE(FD_GLOBAL_IPEVAL,1)
else
  AC_DEFINE(FD_GLOBAL_IPEVAL,0)
fi

dnl check for building with parseltongue
AC_ARG_WITH(parseltongue,--with[out]-parseltongue Build parseltongue libraries for connecting FramerD and Python)
if test "$with_parseltongue" = "yes"; then
   AX_PYTHON2
fi;
if test "$with_parseltongue" = "yes" &&  test ! -z "${ax_python2_lib}"; then
   CMODULES="lib/framerd/parseltongue.$shared_suffix ${CMODULES}"
   INSTALLCMODULES="install-parseltongue ${INSTALLCMODULES}"
fi

dnl Build with primitives accessing libcurl, if it is available
AC_CHECK_LIB(curl,curl_easy_init,curl_default="yes")
AC_ARG_WITH(curl,--with[out]-curl Use libcurl in fdweb,,with_curl=$curl_default)
if test $with_curl = "yes"; then
  AC_CHECK_LIB(curl,curl_easy_init)
  AC_CHECK_HEADERS(curl/curl.h)
  AC_DEFINE(FD_WITH_CURL,1)
  curlo="src/web/curl.o"
  curlconfig="\`curl-config --libs\`"
else
  AC_DEFINE(FD_WITH_CURL,0)
fi

dnl Build with primitives accessing DNS resolution functions, if available
AC_CHECK_HEADERS(resolv.h)
AC_CHECK_HEADERS(arpa/nameser.h)
AC_CHECK_HEADERS(netinet/in.h)
AC_CHECK_HEADERS(netdb.h)
AC_CHECK_LIB(ldns,ldns_resolver_query,dns_default="yes")
AC_ARG_WITH(dns,--with[out]-dns Use dns in web,,with_dns=$dns_default)
if test "$with_dns" = "yes"; then
  AC_CHECK_LIB(ldns,ldns_resolver_query)
  AC_DEFINE(FD_WITH_DNS,1)
  ldnso="src/web/ldns.o"
else
  AC_DEFINE(FD_WITH_DNS,0)
fi

dnl Build with primitives accessing libexif, if it is available
AC_CHECK_LIB(exif,exif_data_new_from_data,exif_default="yes")
AC_CHECK_HEADER(libexif/exif-data.h,exif_default=$exif_default,exif_default="no")
AC_ARG_WITH(exif,--with[out]-exif Use libexif in fdweb,,with_exif=$exif_default)
if test "$with_exif" = "yes"; then
  AC_CHECK_HEADERS(libexif/exif-data.h)
  CMODULES="lib/framerd/exif.$shared_suffix ${CMODULES}"
  INSTALLCMODULES="install-exif ${INSTALLCMODULES}"
fi

dnl Build with primitives accessing qrcoding, if available
AC_CHECK_HEADER(png.h,have_png_h="yes",have_png_h="no")
AC_CHECK_HEADER(qrencode.h,have_qrencode_h="yes",have_qrencode_h="no")

AC_ARG_WITH(qrcode,--with[out]-qrcode Create qrcode binary module,,with_qrcode="yes")
libpng_ok="no"
qrcode_ok="no"

if (test "$with_qrcode" = "yes"); then
  AC_CHECK_LIB(png,png_create_write_struct,libpng_ok="yes")
  AC_CHECK_LIB(qrencode,QRcode_encodeString8bit,qrcode_ok="yes")
  if ( (test "$have_png_h" = "yes") && (test "$have_qrencode_h" = "yes") &&
       (test "$libpng_ok" = "yes") && (test "$qrcode_ok" = "yes") ); then
    CMODULES="lib/framerd/qrcode.$shared_suffix ${CMODULES}"
    INSTALLCMODULES="install-qrcode ${INSTALLCMODULES}"
  fi;
fi

echo "with_qrcode=$with_qrcode have_png_h=$have_png_h have_qrencode_h=$have_qrencode_h libpng_ok=$libpng_ok qrcode_ok=$qrcode_ok"

if test -d /etc/systemd; then
  default_bootsystem="systemd";
elif test -d /lib/systemd; then
  default_bootsystem="systemd";
elif test -d /etc/init; then
  default_bootsystem="upstart";
 elif test -d /etc/init.d; then
  default_bootsystem="sysv";
 else
  default_bootsystem="none"
fi

AC_ARG_WITH(bootsystem,--with-bootsystem Setup boot method,,with_bootsystem=$default_bootsystem)

if test $with_bootsystem != "none"; then
  BOOTSETUP="setup-$with_bootsystem"
fi

if test -d /lib/systemd/system;
  then default_systemd_loc=/lib/systemd/system;
elif test -d /usr/lib/systemd/system;
  then default_systemd_loc=/usr/lib/systemd/system;
elif test -d /etc/systemd/system;
  then default_systemd_loc=/etc/systemd/system;
else default_systemd_loc=/usr/lib/systemd/system;
fi

AC_ARG_WITH(systemd,--with-systemd Specify systemd install directory,,with_systemd=$default_systemd_loc)
systemd_loc=$with_systemd

if test "$prefix" != "/usr"; then
  CFLAGS="$CFLAGS -I$prefix/include"
  CPPFLAGS="$CPPFLAGS -I$prefix/include"
  LDFLAGS="$LDFLAGS -L$prefix/lib"
fi

AC_DEFINE_UNQUOTED(FD_SCHEME_BUILTINS,$scheme_builtins_init)

if test $exec_prefix = 'NONE';
  then BINDIR="${prefix}/bin";
else BINDIR="${exec_prefix}/bin";
fi

dnl Try to init submodules if needed and possible
if (test ! -f src/scheme/gpath.scm); then
   if (which git 2>&1 > /dev/null) && (test -d .git); then
     git submodule update --init;
   else
     echo "Can't initialize git submodules";
   fi;
fi;

AC_OUTPUT([makefile etc/version etc/makefile.include
           tests/lisp/makefile tests/storage/makefile
	   tests/makefile DIY/makefile 
           etc/gdbinit
	   exe/.gdbinit
	   dbg/runenv.sh dbg/.gdbinit dbg/.lldbinit dbg/.valgrind
	   tests/lisp/.gdbinit tests/storage/.gdbinit tests/.valgrind
	   etc/fd_start_servlets etc/fd_start_daemons etc/fd_stop_jobs
	   etc/fdstart etc/fdstop etc/fdstartall etc/fdstopall etc/fdcleanup
	   etc/fdsignal etc/fdinject etc/fdstatus etc/fdlog 
	   etc/fdreload etc/fdrestart etc/fdctl etc/fdgdb etc/fdbgexec
	   etc/fdsetconfig etc/fdgetconfig etc/fdconfig
	   tests/testenv.sh tests/xtest
	   tests/runtest tests/memtest
	   tests/leaktest tests/fdexectest
	   src/scripts/fdpkg.scm
	   etc/OSXStartupItem/FramerD
	   etc/fdjob
	   etc/logrotate
	   etc/framerd-rc.d
	   etc/framerd.conf
	   etc/ex_servlet.service
	   etc/ex_daemon.service
	   etc/fdaemons.service
	   etc/fdweb.service
	   etc/framerd.service
	   etc/FramerD.plist
	   etc/fdserv.conf 
	   etc/fdserv.load
	   etc/install-script 
	   dist/framerd-4.2.spec],
	   [chmod a+x etc/fd* tests/*test])

echo "Built with malloc=$using_malloc";
echo $using_malloc > ./.malloc;
if test ! -z "$manually_disabled"; then
   echo "+++ These components have been explicitly disabled: $manually_disabled"; fi
if test ! -z "$auto_disabled"; then
   echo "+++ These components can't be built: $auto_disabled"; fi
if test ! -z "$force_enabled"; then
   echo "+++ These components were enabled by force and may have issues: $force_enabled"; fi
if test "$enable_ipeval" = "yes"; then
   echo "+++ You've configured FramerD to use iterated partial evaluation";
   echo "+++ Use of the resulting software may be restricted by patent law. ";
   echo "+++ Contact beingmeta (licensing@beingmeta.com) for more information.";
   echo "+++ Thanks for using FramerD!";
else
   echo "+++ Thanks for using FramerD!";
fi

# Emacs local variables
#  ;;;  Local variables: ***
#  ;;;  compile-command: "autoconf; configure;" ***
#  ;;;  End: ***
