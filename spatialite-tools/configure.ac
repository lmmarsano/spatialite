#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(spatialite-tools, 2.4.0, a.furieri@lqt.it)
AC_LANG(C)

AM_INIT_AUTOMAKE
AM_MAINTAINER_MODE

# Checks for header files.
AC_CHECK_HEADERS(stdlib.h,, [AC_MSG_ERROR([cannot find stdlib.h, bailing out])])
AC_CHECK_HEADERS(stdio.h,, [AC_MSG_ERROR([cannot find stdio.h, bailing out])])
AC_CHECK_HEADERS(string.h,, [AC_MSG_ERROR([cannot find string.h, bailing out])])
AC_CHECK_HEADERS(memory.h,, [AC_MSG_ERROR([cannot find memory.h, bailing out])])
AC_CHECK_HEADERS(math.h,, [AC_MSG_ERROR([cannot find math.h, bailing out])])
AC_CHECK_HEADERS(float.h,, [AC_MSG_ERROR([cannot find float.h, bailing out])])
AC_CHECK_HEADERS(fcntl.h,, [AC_MSG_ERROR([cannot find fcntl.h, bailing out])])
AC_CHECK_HEADERS(inttypes.h,, [AC_MSG_ERROR([cannot find inttypes.h, bailing out])])
AC_CHECK_HEADERS(stddef.h,, [AC_MSG_ERROR([cannot find stddef.h, bailing out])])
AC_CHECK_HEADERS(stdint.h,, [AC_MSG_ERROR([cannot find stdint.h, bailing out])])
AC_CHECK_HEADERS(sys/time.h,, [AC_MSG_ERROR([cannot find sys/time.h, bailing out])])
AC_CHECK_HEADERS(unistd.h,, [AC_MSG_ERROR([cannot find unistd.h, bailing out])])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_LIBTOOL_WIN32_DLL
AC_PROG_LIBTOOL

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE

# Checks for library functions.
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MEMCMP
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([memset sqrt strcasecmp strerror strncasecmp strstr fdatasync ftruncate getcwd gettimeofday localtime_r memmove strerror])

AC_CONFIG_FILES([Makefile])

#-----------------------------------------------------------------------
#   --enable-readline
#
AC_ARG_ENABLE(readline, [AS_HELP_STRING(
  [--enable-readline],
  [use readline in shell tool (yes, no) [default=yes]])],
  [], [enable_readline=yes])
if test x"$enable_readline" != xno ; then
  sLIBS=$LIBS
  LIBS=""
  AC_SEARCH_LIBS(tgetent, curses, [], [])
  AC_SEARCH_LIBS(readline, readline, [], [enable_readline=no])
  AC_CHECK_FUNCS(readline, [], [])
  READLINE_LIBS=$LIBS
  LIBS=$sLIBS
fi
AC_SUBST(READLINE_LIBS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --with-spatialite-lib
#
AC_ARG_WITH(spatialite-lib,
  [  --with-spatialite-lib=DIR  search for SpatiaLite lib in DIR],
  spatialite_lib="$withval", spatialite_lib="")
if test "$spatialite_lib" == "" ; then
  SPATIALITE_LIB="/usr/local/lib/libspatialite.a"
else
  SPATIALITE_LIB="$spatialite_lib/libspatialite.a"
fi
AC_SUBST(SPATIALITE_LIB)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --with-proj-lib
#
AC_ARG_WITH(proj-lib,
  [  --with-proj-lib=DIR        search for PROJ.4 lib in DIR],
  proj_lib="$withval", proj_lib="")
if test "$proj_lib" == "" ; then
  PROJ_LIB="/usr/local/lib/libproj.a"
else
  PROJ_LIB="$proj_lib/libproj.a"
fi
AC_SUBST(PROJ_LIB)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-proj
#
AC_ARG_ENABLE(proj, [AS_HELP_STRING(
  [--enable-proj], [enables PROJ.4 inclusion [default=yes]])], 
  [], [proj=yes])
if test x"$enable_proj" == "xno"; then
  PROJ_LIB=
  AC_SUBST(PROJ_LIB)
fi
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --with-geos-lib
#
AC_ARG_WITH(geos-lib,
  [  --with-geos-lib=DIR        search for GEOS lib in DIR],
  geos_lib="$withval", geos_lib="")
if test "$geos_lib" == "" ; then
  GEOS_LIB_C="/usr/local/lib/libgeos_c.a"
  GEOS_LIB="/usr/local/lib/libgeos.a"
else
  GEOS_LIB_C="$geos_lib/libgeos_c.a"
  GEOS_LIB="$geos_lib/libgeos.a"
fi
AC_SUBST(GEOS_LIB_C)
AC_SUBST(GEOS_LIB)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-geos
#
AC_ARG_ENABLE(geos, [AS_HELP_STRING(
  [--enable-geos], [enables GEOS inclusion [default=yes]])], 
  [], [geos=yes])
if test x"$enable_geos" == "xno"; then
  GEOS_LIB_C=
  GEOS_LIB=
  AC_SUBST(GEOS_LIB_C)
  AC_SUBST(GEOS_LIB)
fi
#-----------------------------------------------------------------------

# Checking for MinGW
AM_CONDITIONAL([MINGW], [test "$target_alias" = "mingw32"])
# Checking for Mac OsX
AM_CONDITIONAL([MACOSX], [test "$target_alias" = "macosx"])

AC_OUTPUT
