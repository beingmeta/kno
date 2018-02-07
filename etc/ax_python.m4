# ===========================================================================
#        https://www.gnu.org/software/autoconf-archive/ax_python.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_PYTHON
#
# DESCRIPTION
#
#   This macro does a complete Python development environment check.
#
#   It checks for all known versions. When it finds an executable, it looks
#   to find the header files and library.
#
#   It sets PYTHON_BIN to the name of the python executable,
#   PYTHON_INCLUDE_DIR to the directory holding the header files, and
#   PYTHON_LIB to the name of the Python library.
#
#   This macro calls AC_SUBST on PYTHON_BIN (via AC_CHECK_PROG),
#   PYTHON_INCLUDE_DIR and PYTHON_LIB.
#
# LICENSE
#
#   Copyright (c) 2008 Michael Tindal
#
#   This program is free software; you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation; either version 2 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <https://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 18

AC_DEFUN([AX_PYTHON2],
[AC_MSG_CHECKING(for python2 build information)
AC_MSG_RESULT([])
for python2 in python2.7 python2.6 python2.5 python2.4 python2.3 python2.2 python2.1 python; do
AC_CHECK_PROGS(PYTHON2_BIN, [$python2])
ax_python2_bin=$PYTHON2_BIN
if test x$ax_python2_bin != x; then
   AC_CHECK_LIB($ax_python2_bin, main, ax_python2_lib=$ax_python2_bin, ax_python2_lib=no)
   if test x$ax_python2_lib == xno; then
     AC_CHECK_LIB(${ax_python2_bin}m, main, ax_python2_lib=${ax_python2_bin}m, ax_python2_lib=no)
   fi
   if test x$ax_python2_lib != xno; then
     ax_python2_header=`$ax_python2_bin -c "from distutils.sysconfig import *; print(get_config_var('CONFINCLUDEPY'))"`
     if test x$ax_python2_header != x; then
       break;
     fi
   fi
fi
done
if test x$ax_python2_bin = x; then
   ax_python2_bin=no
fi
if test x$ax_python2_header = x; then
   ax_python2_header=no
fi
if test x$ax_python2_lib = x; then
   ax_python2_lib=no
fi

AC_MSG_RESULT([  results of the Python2 check:])
AC_MSG_RESULT([    Binary:      $ax_python2_bin])
AC_MSG_RESULT([    Library:     $ax_python2_lib])
AC_MSG_RESULT([    Include Dir: $ax_python2_header])

if test x$ax_python2_header != xno; then
  PYTHON2_INCLUDE_DIR=$ax_python2_header
  AC_SUBST(PYTHON2_INCLUDE_DIR)
fi
if test x$ax_python2_lib != xno; then
  PYTHON2_LIB=$ax_python2_lib
  AC_SUBST(PYTHON2_LIB)
fi
])dnl

AC_DEFUN([AX_PYTHON3],
[AC_MSG_CHECKING(for python build information)
AC_MSG_RESULT([])
for python3 in python3.7 python3.6 python3.5 python3.4 python3.3 python3.2 python3.1 python3.0; do
AC_CHECK_PROGS(PYTHON3_BIN, [$python3])
ax_python3_bin=$PYTHON3_BIN
if test x$ax_python3_bin != x; then
   AC_CHECK_LIB($ax_python3_bin, main, ax_python3_lib=$ax_python3_bin, ax_python3_lib=no)
   if test x$ax_python3_lib == xno; then
     AC_CHECK_LIB(${ax_python3_bin}m, main, ax_python3_lib=${ax_python3_bin}m, ax_python3_lib=no)
   fi
   if test x$ax_python3_lib != xno; then
     ax_python3_header=`$ax_python3_bin -c "from distutils.sysconfig import *; print(get_config_var('CONFINCLUDEPY'))"`
     if test x$ax_python3_header != x; then
       break;
     fi
   fi
fi
done
if test x$ax_python3_bin = x; then
   ax_python3_bin=no
fi
if test x$ax_python3_header = x; then
   ax_python3_header=no
fi
if test x$ax_python3_lib = x; then
   ax_python3_lib=no
fi

AC_MSG_RESULT([  results of the Python3 check:])
AC_MSG_RESULT([    Binary:      $ax_python3_bin])
AC_MSG_RESULT([    Library:     $ax_python3_lib])
AC_MSG_RESULT([    Include Dir: $ax_python3_header])

if test x$ax_python3_header != xno; then
  PYTHON3_INCLUDE_DIR=$ax_python3_header
  AC_SUBST(PYTHON3_INCLUDE_DIR)
fi
if test x$ax_python3_lib != xno; then
  PYTHON3_LIB=$ax_python3_lib
  AC_SUBST(PYTHON3_LIB)
fi
])dnl
