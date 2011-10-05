
AM_CFLAGS = -D_LARGE_FILE=1 -D_FILE_OFFSET_BITS=64 \
	-D_LARGEFILE_SOURCE=1

bin_PROGRAMS =spatialite \
	spatialite_tool \
	spatialite_network \
	shp_doctor \
	exif_loader \
	spatialite_osm

INCLUDES = @CFLAGS@

spatialite_SOURCES = shell.c shapefiles.c
spatialite_tool_SOURCES = spatialite_tool.c shapefiles.c
spatialite_network_SOURCES = spatialite_network.c
shp_doctor_SOURCES = shp_doctor.c
exif_loader_SOURCES = exif_loader.c
spatialite_osm_SOURCES = spatialite_osm.c

if MINGW
spatialite_LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	/usr/local/lib/libiconv.a \
	-lm -lstdc++
LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	/usr/local/lib/libiconv.a \
	-lm -lstdc++
else
if MACOSX
spatialite_LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	-lm -liconv -lstdc++ -lpthread -ldl \
	@READLINE_LIBS@
LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	-lm -liconv -lstdc++ -lpthread -ldl
else
spatialite_LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	-lm -lstdc++ -lpthread -ldl \
	@READLINE_LIBS@
LDADD = @SPATIALITE_LIB@ \
	@GEOS_LIB_C@ \
	@GEOS_LIB@ \
	@PROJ_LIB@ \
	-lm -lstdc++ -lpthread -ldl
endif
endif

EXTRA_DIST = makefile.vc nmake.opt

AUTOMAKE_OPTIONS = dist-zip

