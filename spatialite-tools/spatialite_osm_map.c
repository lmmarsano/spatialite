/* 
/ spatialite_osm_map
/
/ a tool loading OSM-XML maps into a SpatiaLite DB
/
/ version 1.0, 2010 April 8
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ Copyright (C) 2010  Alessandro Furieri
/
/    This program is free software: you can redistribute it and/or modify
/    it under the terms of the GNU General Public License as published by
/    the Free Software Foundation, either version 3 of the License, or
/    (at your option) any later version.
/
/    This program is distributed in the hope that it will be useful,
/    but WITHOUT ANY WARRANTY; without even the implied warranty of
/    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/    GNU General Public License for more details.
/
/    You should have received a copy of the GNU General Public License
/    along with this program.  If not, see <http://www.gnu.org/licenses/>.
/
*/

#if defined(_WIN32) && !defined(__MINGW32__)
/* MSVC strictly requires this include [off_t] */
#include <sys/types.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>

#include <expat.h>

#ifdef SPATIALITE_AMALGAMATION
#include <spatialite/sqlite3.h>
#else
#include <sqlite3.h>
#endif

#include <spatialite/gaiageo.h>
#include <spatialite.h>

#define ARG_NONE		0
#define ARG_OSM_PATH	1
#define ARG_DB_PATH		2
#define ARG_CACHE_SIZE	3

#define MAX_TAG		16

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#endif /* not WIN32 */

#if defined(_WIN32)
#define atol_64		_atoi64
#else
#define atol_64		atoll
#endif

#define BUFFSIZE	8192

#define CURRENT_TAG_UNKNOWN	0
#define CURRENT_TAG_IS_NODE	1
#define CURRENT_TAG_IS_WAY	2
#define CURRENT_TAG_IS_RELATION	3

struct aux_params
{
/* an auxiliary struct used for XML parsing */
    sqlite3 *db_handle;
    sqlite3_stmt *ins_tmp_nodes_stmt;
    sqlite3_stmt *ins_tmp_ways_stmt;
    sqlite3_stmt *ins_generic_point_stmt;
    sqlite3_stmt *ins_addresses_stmt;
    sqlite3_stmt *ins_generic_linestring_stmt;
    sqlite3_stmt *ins_generic_polygon_stmt;
    int current_tag;
};

struct layers
{
    const char *name;
    int ok_point;
    int ok_linestring;
    int ok_polygon;
    sqlite3_stmt *ins_point_stmt;
    sqlite3_stmt *ins_linestring_stmt;
    sqlite3_stmt *ins_polygon_stmt;
} base_layers[] =
{
    {
    "highway", 0, 0, 0, NULL, NULL, NULL},
    {
    "junction", 0, 0, 0, NULL, NULL, NULL},
    {
    "traffic_calming", 0, 0, 0, NULL, NULL, NULL},
    {
    "traffic_sign", 0, 0, 0, NULL, NULL, NULL},
    {
    "service", 0, 0, 0, NULL, NULL, NULL},
    {
    "barrier", 0, 0, 0, NULL, NULL, NULL},
    {
    "cycleway", 0, 0, 0, NULL, NULL, NULL},
    {
    "tracktype", 0, 0, 0, NULL, NULL, NULL},
    {
    "waterway", 0, 0, 0, NULL, NULL, NULL},
    {
    "railway", 0, 0, 0, NULL, NULL, NULL},
    {
    "aeroway", 0, 0, 0, NULL, NULL, NULL},
    {
    "aerialway", 0, 0, 0, NULL, NULL, NULL},
    {
    "power", 0, 0, 0, NULL, NULL, NULL},
    {
    "man_made", 0, 0, 0, NULL, NULL, NULL},
    {
    "leisure", 0, 0, 0, NULL, NULL, NULL},
    {
    "amenity", 0, 0, 0, NULL, NULL, NULL},
    {
    "shop", 0, 0, 0, NULL, NULL, NULL},
    {
    "tourism", 0, 0, 0, NULL, NULL, NULL},
    {
    "historic", 0, 0, 0, NULL, NULL, NULL},
    {
    "landuse", 0, 0, 0, NULL, NULL, NULL},
    {
    "military", 0, 0, 0, NULL, NULL, NULL},
    {
    "natural", 0, 0, 0, NULL, NULL, NULL},
    {
    "geological", 0, 0, 0, NULL, NULL, NULL},
    {
    "route", 0, 0, 0, NULL, NULL, NULL},
    {
    "boundary", 0, 0, 0, NULL, NULL, NULL},
    {
    "sport", 0, 0, 0, NULL, NULL, NULL},
    {
    "abutters", 0, 0, 0, NULL, NULL, NULL},
    {
    "accessories", 0, 0, 0, NULL, NULL, NULL},
    {
    "properties", 0, 0, 0, NULL, NULL, NULL},
    {
    "restrictions", 0, 0, 0, NULL, NULL, NULL},
    {
    "place", 0, 0, 0, NULL, NULL, NULL},
    {
    "building", 0, 0, 0, NULL, NULL, NULL},
    {
    "parking", 0, 0, 0, NULL, NULL, NULL},
    {
NULL, 0, 0, 0, NULL, NULL, NULL},};

struct tag
{
    char *k;
    char *v;
    struct tag *next;
};

struct node
{
    sqlite3_int64 id;
    double lat;
    double lon;
    struct tag *first;
    struct tag *last;
} glob_node;

struct nd
{
    sqlite3_int64 ref;
    char found;
    struct nd *next;
};

struct way
{
    sqlite3_int64 id;
    struct nd *first_nd;
    struct nd *last_nd;
    struct tag *first;
    struct tag *last;
} glob_way;

struct member
{
    sqlite3_int64 ref;
    int is_node;
    int is_way;
    char found;
    char *role;
    gaiaGeomCollPtr geom;
    struct member *next;
};

struct relation
{
    sqlite3_int64 id;
    struct member *first_member;
    struct member *last_member;
    struct tag *first;
    struct tag *last;
} glob_relation;

static void
create_point_table (struct aux_params *params, struct layers *layer)
{
    int ret;
    char *err_msg = NULL;
    char sql[1024];
    sqlite3_stmt *stmt;

/* creating a POINT table */
    sprintf (sql, "CREATE TABLE pt_%s (\n", layer->name);
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "sub_type TEXT,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    sprintf (sql,
	     "SELECT AddGeometryColumn('pt_%s', 'Geometry', 4326, 'POINT', 'XY')",
	     layer->name);
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }

/* creating the insert SQL statement */
    sprintf (sql, "INSERT INTO pt_%s (id, sub_type, name, Geometry) ",
	     layer->name);
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  return;
      }
    layer->ins_point_stmt = stmt;
}

static void
create_linestring_table (struct aux_params *params, struct layers *layer)
{
    int ret;
    char *err_msg = NULL;
    char sql[1024];
    sqlite3_stmt *stmt;

/* creating a LINESTRING table */
    sprintf (sql, "CREATE TABLE ln_%s (\n", layer->name);
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "sub_type TEXT,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'ln_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    sprintf (sql,
	     "SELECT AddGeometryColumn('ln_%s', 'Geometry', 4326, 'MULTILINESTRING', 'XY')",
	     layer->name);
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'ln_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }

/* creating the insert SQL statement */
    sprintf (sql, "INSERT INTO ln_%s (id, sub_type, name, Geometry) ",
	     layer->name);
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  return;
      }
    layer->ins_linestring_stmt = stmt;
}

static void
create_polygon_table (struct aux_params *params, struct layers *layer)
{
    int ret;
    char *err_msg = NULL;
    char sql[1024];
    sqlite3_stmt *stmt;

/* creating a POLYGON table */
    sprintf (sql, "CREATE TABLE pg_%s (\n", layer->name);
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "sub_type TEXT,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pg_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    sprintf (sql,
	     "SELECT AddGeometryColumn('pg_%s', 'Geometry', 4326, 'MULTIPOLYGON', 'XY')",
	     layer->name);
    ret = sqlite3_exec (params->db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pg_%s' error: %s\n", layer->name,
		   err_msg);
	  sqlite3_free (err_msg);
	  return;
      }

/* creating the insert SQL statement */
    sprintf (sql, "INSERT INTO pg_%s (id, sub_type, name, Geometry) ",
	     layer->name);
    strcat (sql, "VALUES (?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  return;
      }
    layer->ins_polygon_stmt = stmt;
}

static void
free_tag (struct tag *p)
{
    if (p->k)
	free (p->k);
    if (p->v)
	free (p->v);
    free (p);
}

static void
free_member (struct member *p)
{
    if (p->role)
	free (p->role);
    free (p);
}

static void
start_node (struct aux_params *params, const char **attr)
{
    int i;
    glob_node.id = -1;
    glob_node.lat = DBL_MAX;
    glob_node.lon = DBL_MAX;
    glob_node.first = NULL;
    glob_node.last = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_node.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "lat") == 0)
	      glob_node.lat = atof (attr[i + 1]);
	  if (strcmp (attr[i], "lon") == 0)
	      glob_node.lon = atof (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_NODE;
}

static void
point_layer_insert (struct aux_params *params, const char *layer_name,
		    sqlite3_int64 id, double lat, double lon,
		    const char *sub_type, const char *name)
{
    struct layers *layer;
    int i = 0;
    while (1)
      {
	  layer = &(base_layers[i++]);
	  if (layer->name == NULL)
	      return;
	  if (strcmp (layer->name, layer_name) == 0)
	    {
		if (layer->ok_point == 0)
		  {
		      layer->ok_point = 1;
		      create_point_table (params, layer);
		  }
		if (layer->ins_point_stmt)
		  {
		      int ret;
		      unsigned char *blob;
		      int blob_size;
		      gaiaGeomCollPtr geom = gaiaAllocGeomColl ();
		      geom->Srid = 4326;
		      gaiaAddPointToGeomColl (geom, lon, lat);
		      sqlite3_reset (layer->ins_point_stmt);
		      sqlite3_clear_bindings (layer->ins_point_stmt);
		      sqlite3_bind_int64 (layer->ins_point_stmt, 1, id);
		      if (sub_type == NULL)
			  sqlite3_bind_null (layer->ins_point_stmt, 2);
		      else
			  sqlite3_bind_text (layer->ins_point_stmt, 2,
					     sub_type, strlen (sub_type),
					     SQLITE_STATIC);
		      if (name == NULL)
			  sqlite3_bind_null (layer->ins_point_stmt, 3);
		      else
			  sqlite3_bind_text (layer->ins_point_stmt, 3, name,
					     strlen (name), SQLITE_STATIC);
		      gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
		      gaiaFreeGeomColl (geom);
		      sqlite3_bind_blob (layer->ins_point_stmt, 4, blob,
					 blob_size, free);
		      ret = sqlite3_step (layer->ins_point_stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  return;
		      fprintf (stderr, "sqlite3_step() error: INS_POINT %s\n",
			       layer_name);
		      sqlite3_finalize (layer->ins_point_stmt);
		      layer->ins_point_stmt = NULL;
		  }
		return;
	    }
      }
}

static void
point_generic_insert (struct aux_params *params, sqlite3_int64 id, double lat,
		      double lon, const char *name)
{
    if (params->ins_generic_point_stmt)
      {
	  int ret;
	  unsigned char *blob;
	  int blob_size;
	  gaiaGeomCollPtr geom = gaiaAllocGeomColl ();
	  geom->Srid = 4326;
	  gaiaAddPointToGeomColl (geom, lon, lat);
	  sqlite3_reset (params->ins_generic_point_stmt);
	  sqlite3_clear_bindings (params->ins_generic_point_stmt);
	  sqlite3_bind_int64 (params->ins_generic_point_stmt, 1, id);
	  if (name == NULL)
	      sqlite3_bind_null (params->ins_generic_point_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_generic_point_stmt, 2, name,
				 strlen (name), SQLITE_STATIC);
	  gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	  gaiaFreeGeomColl (geom);
	  sqlite3_bind_blob (params->ins_generic_point_stmt, 3, blob,
			     blob_size, free);
	  ret = sqlite3_step (params->ins_generic_point_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr, "sqlite3_step() error: INS_GENERIC_POINT\n");
	  sqlite3_finalize (params->ins_generic_point_stmt);
	  params->ins_generic_point_stmt = NULL;
      }
}

static void
address_insert (struct aux_params *params, sqlite3_int64 id, double lat,
		double lon, const char *country, const char *city,
		const char *postcode,
		const char *street,
		const char *housename, const char *housenumber)
{
    if (params->ins_addresses_stmt)
      {
	  int ret;
	  unsigned char *blob;
	  int blob_size;
	  gaiaGeomCollPtr geom = gaiaAllocGeomColl ();
	  geom->Srid = 4326;
	  gaiaAddPointToGeomColl (geom, lon, lat);
	  sqlite3_reset (params->ins_addresses_stmt);
	  sqlite3_clear_bindings (params->ins_addresses_stmt);
	  sqlite3_bind_int64 (params->ins_addresses_stmt, 1, id);
	  if (country == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 2, country,
				 strlen (country), SQLITE_STATIC);
	  if (city == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 3);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 3, city,
				 strlen (city), SQLITE_STATIC);
	  if (postcode == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 4);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 4, postcode,
				 strlen (postcode), SQLITE_STATIC);
	  if (street == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 5);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 5, street,
				 strlen (street), SQLITE_STATIC);
	  if (housename == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 6);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 6, housename,
				 strlen (housename), SQLITE_STATIC);
	  if (housenumber == NULL)
	      sqlite3_bind_null (params->ins_addresses_stmt, 7);
	  else
	      sqlite3_bind_text (params->ins_addresses_stmt, 7, housenumber,
				 strlen (housenumber), SQLITE_STATIC);
	  gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	  gaiaFreeGeomColl (geom);
	  sqlite3_bind_blob (params->ins_addresses_stmt, 8, blob, blob_size,
			     free);
	  ret = sqlite3_step (params->ins_addresses_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr, "sqlite3_step() error: INS_ADDRESSES\n");
	  sqlite3_finalize (params->ins_addresses_stmt);
	  params->ins_addresses_stmt = NULL;
      }
}

static void
eval_node (struct aux_params *params)
{
    struct tag *p_tag;
    const char *p;
    int i = 0;
    const char *layer_name = NULL;
    char *sub_type = NULL;
    char *name = NULL;
    char *country = NULL;
    char *city = NULL;
    char *postcode = NULL;
    char *street = NULL;
    char *housename = NULL;
    char *housenumber = NULL;
    if (glob_node.first == NULL)
	return;
    while (1)
      {
	  p = base_layers[i++].name;
	  if (!p)
	      break;
	  p_tag = glob_node.first;
	  while (p_tag)
	    {
		if (strcmp (p_tag->k, p) == 0)
		  {
		      layer_name = p;
		      sub_type = p_tag->v;
		  }
		if (strcmp (p_tag->k, "name") == 0)
		    name = p_tag->v;
		p_tag = p_tag->next;
	    }
	  if (layer_name)
	      break;
      }
    if (layer_name)
      {
	  point_layer_insert (params, layer_name, glob_node.id, glob_node.lat,
			      glob_node.lon, sub_type, name);
	  return;
      }
    else if (name != NULL)
      {
	  point_generic_insert (params, glob_node.id, glob_node.lat,
				glob_node.lon, name);
	  return;
      }

/* may be an house address */
    p_tag = glob_node.first;
    while (p_tag)
      {
	  if (strcmp (p_tag->k, "addr:country") == 0)
	      country = p_tag->v;
	  if (strcmp (p_tag->k, "addr:city") == 0)
	      city = p_tag->v;
	  if (strcmp (p_tag->k, "addr:postcode") == 0)
	      postcode = p_tag->v;
	  if (strcmp (p_tag->k, "addr:street") == 0)
	      street = p_tag->v;
	  if (strcmp (p_tag->k, "addr:housename") == 0)
	      housename = p_tag->v;
	  if (strcmp (p_tag->k, "addr:housenumber") == 0)
	      housenumber = p_tag->v;
	  p_tag = p_tag->next;
      }
    if (country || city || postcode || street || housename || housenumber)
	address_insert (params, glob_node.id, glob_node.lat, glob_node.lon,
			country, city, postcode, street, housename,
			housenumber);
}

static void
tmp_nodes_insert (struct aux_params *params, sqlite3_int64 id, double lat,
		  double lon)
{
    int ret;
    if (params->ins_tmp_nodes_stmt == NULL)
	return;
    sqlite3_reset (params->ins_tmp_nodes_stmt);
    sqlite3_clear_bindings (params->ins_tmp_nodes_stmt);
    sqlite3_bind_int64 (params->ins_tmp_nodes_stmt, 1, id);
    sqlite3_bind_double (params->ins_tmp_nodes_stmt, 2, lat);
    sqlite3_bind_double (params->ins_tmp_nodes_stmt, 3, lon);
    ret = sqlite3_step (params->ins_tmp_nodes_stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	return;
    fprintf (stderr, "sqlite3_step() error: INS_TMP_NODES\n");
    sqlite3_finalize (params->ins_tmp_nodes_stmt);
    params->ins_tmp_nodes_stmt = NULL;
}

static void
end_node (struct aux_params *params)
{
    struct tag *p_tag;
    struct tag *p_tag2;

    tmp_nodes_insert (params, glob_node.id, glob_node.lat, glob_node.lon);
    eval_node (params);
    p_tag = glob_node.first;
    while (p_tag)
      {
	  p_tag2 = p_tag->next;
	  free_tag (p_tag);
	  p_tag = p_tag2;
      }
    glob_node.id = -1;
    glob_node.lat = DBL_MAX;
    glob_node.lon = DBL_MAX;
    glob_node.first = NULL;
    glob_node.last = NULL;
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_way (struct aux_params *params, const char **attr)
{
    int i;
    glob_way.id = -1;
    glob_way.first_nd = NULL;
    glob_way.last_nd = NULL;
    glob_way.first = NULL;
    glob_way.last = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_way.id = atol_64 (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_WAY;
}

static gaiaGeomCollPtr
build_linestring (sqlite3 * db_handle)
{
    gaiaGeomCollPtr geom;
    gaiaLinestringPtr ln;
    int points = 0;
    int tbd;
    int block = 128;
    int base = 0;
    int how_many;
    int ind;
    int ret;
    int count;
    char sql[8192];
    sqlite3_stmt *stmt;
    sqlite3_int64 id;
    double lat;
    double lon;
    struct nd *p_nd = glob_way.first_nd;
    while (p_nd)
      {
	  points++;
	  p_nd = p_nd->next;
      }
    if (!points)
	return NULL;
    geom = gaiaAllocGeomColl ();
    geom->Srid = 4326;
    ln = gaiaAddLinestringToGeomColl (geom, points);
    tbd = points;
    while (tbd > 0)
      {
	  /* 
	     / fetching node coords
	     / requesting max 128 points at each time 
	   */
	  if (tbd < block)
	      how_many = tbd;
	  else
	      how_many = block;
	  strcpy (sql, "SELECT id, lat, lon FROM osm_tmp_nodes ");
	  strcat (sql, "WHERE id IN (");
	  for (ind = 0; ind < how_many; ind++)
	    {
		if (ind == 0)
		    strcat (sql, "?");
		else
		    strcat (sql, ",?");
	    }
	  strcat (sql, ")");
	  ret = sqlite3_prepare_v2 (db_handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (db_handle));
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  ind = 1;
	  count = 0;
	  p_nd = glob_way.first_nd;
	  while (p_nd)
	    {
		if (count < base)
		  {
		      count++;
		      p_nd = p_nd->next;
		      continue;
		  }
		if (count >= (base + how_many))
		    break;
		sqlite3_bind_int64 (stmt, ind, p_nd->ref);
		ind++;
		count++;
		p_nd = p_nd->next;
	    }
	  while (1)
	    {
		/* scrolling the result set */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		  {
		      /* there are no more rows to fetch - we can stop looping */
		      break;
		  }
		if (ret == SQLITE_ROW)
		  {
		      /* ok, we've just fetched a valid row */
		      id = sqlite3_column_int64 (stmt, 0);
		      lat = sqlite3_column_double (stmt, 1);
		      lon = sqlite3_column_double (stmt, 2);
		      p_nd = glob_way.first_nd;
		      count = 0;
		      while (p_nd)
			{
			    if (p_nd->ref == id)
			      {
				  p_nd->found = 1;
				  gaiaSetPoint (ln->Coords, count, lon, lat);
			      }
			    count++;
			    p_nd = p_nd->next;
			}
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (db_handle));
		      sqlite3_finalize (stmt);
		      gaiaFreeGeomColl (geom);
		  }
	    }
	  sqlite3_finalize (stmt);
	  tbd -= how_many;
	  base += how_many;
      }

/* final checkout */
    p_nd = glob_way.first_nd;
    while (p_nd)
      {
	  if (p_nd->found == 0)
	    {
#if defined(_WIN32) || defined(__MINGW32__)
		/* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
		fprintf (stderr, "UNRESOLVED-NODE %I64d\n", p_nd->ref);
#else
		fprintf (stderr, "UNRESOLVED-NODE %lld\n", p_nd->ref);
#endif
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  p_nd = p_nd->next;
      }
    return geom;
}

static void
line_layer_insert (struct aux_params *params, const char *layer_name,
		   sqlite3_int64 id, unsigned char *blob, int blob_size,
		   const char *sub_type, const char *name)
{
    struct layers *layer;
    int i = 0;
    while (1)
      {
	  layer = &(base_layers[i++]);
	  if (layer->name == NULL)
	      return;
	  if (strcmp (layer->name, layer_name) == 0)
	    {
		if (layer->ok_linestring == 0)
		  {
		      layer->ok_linestring = 1;
		      create_linestring_table (params, layer);
		  }
		if (layer->ins_linestring_stmt)
		  {
		      int ret;
		      sqlite3_reset (layer->ins_linestring_stmt);
		      sqlite3_clear_bindings (layer->ins_linestring_stmt);
		      sqlite3_bind_int64 (layer->ins_linestring_stmt, 1, id);
		      if (sub_type == NULL)
			  sqlite3_bind_null (layer->ins_linestring_stmt, 2);
		      else
			  sqlite3_bind_text (layer->ins_linestring_stmt, 2,
					     sub_type, strlen (sub_type),
					     SQLITE_STATIC);
		      if (name == NULL)
			  sqlite3_bind_null (layer->ins_linestring_stmt, 3);
		      else
			  sqlite3_bind_text (layer->ins_linestring_stmt, 3,
					     name, strlen (name),
					     SQLITE_STATIC);
		      sqlite3_bind_blob (layer->ins_linestring_stmt, 4, blob,
					 blob_size, SQLITE_STATIC);
		      ret = sqlite3_step (layer->ins_linestring_stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  return;
		      fprintf (stderr,
			       "sqlite3_step() error: INS_LINESTRING %s\n",
			       layer_name);
		      sqlite3_finalize (layer->ins_linestring_stmt);
		      layer->ins_linestring_stmt = NULL;
		  }
		return;
	    }
      }
}

static void
polygon_layer_insert (struct aux_params *params, const char *layer_name,
		      sqlite3_int64 id, unsigned char *blob, int blob_size,
		      const char *sub_type, const char *name)
{
    struct layers *layer;
    int i = 0;
    while (1)
      {
	  layer = &(base_layers[i++]);
	  if (layer->name == NULL)
	      return;
	  if (strcmp (layer->name, layer_name) == 0)
	    {
		if (layer->ok_polygon == 0)
		  {
		      layer->ok_polygon = 1;
		      create_polygon_table (params, layer);
		  }
		if (layer->ins_polygon_stmt)
		  {
		      int ret;
		      sqlite3_reset (layer->ins_polygon_stmt);
		      sqlite3_clear_bindings (layer->ins_polygon_stmt);
		      sqlite3_bind_int64 (layer->ins_polygon_stmt, 1, id);
		      if (sub_type == NULL)
			  sqlite3_bind_null (layer->ins_polygon_stmt, 2);
		      else
			  sqlite3_bind_text (layer->ins_polygon_stmt, 2,
					     sub_type, strlen (sub_type),
					     SQLITE_STATIC);
		      if (name == NULL)
			  sqlite3_bind_null (layer->ins_polygon_stmt, 3);
		      else
			  sqlite3_bind_text (layer->ins_polygon_stmt, 3, name,
					     strlen (name), SQLITE_STATIC);
		      sqlite3_bind_blob (layer->ins_polygon_stmt, 4, blob,
					 blob_size, SQLITE_STATIC);
		      ret = sqlite3_step (layer->ins_polygon_stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  return;
		      fprintf (stderr,
			       "sqlite3_step() error: INS_POLYGON %s\n",
			       layer_name);
		      sqlite3_finalize (layer->ins_polygon_stmt);
		      layer->ins_polygon_stmt = NULL;
		  }
		return;
	    }
      }
}

static void
line_generic_insert (struct aux_params *params, sqlite3_int64 id,
		     unsigned char *blob, int blob_size, const char *name)
{
    if (params->ins_generic_linestring_stmt)
      {
	  int ret;
	  sqlite3_reset (params->ins_generic_linestring_stmt);
	  sqlite3_clear_bindings (params->ins_generic_linestring_stmt);
	  sqlite3_bind_int64 (params->ins_generic_linestring_stmt, 1, id);
	  if (name == NULL)
	      sqlite3_bind_null (params->ins_generic_linestring_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_generic_linestring_stmt, 2, name,
				 strlen (name), SQLITE_STATIC);
	  sqlite3_bind_blob (params->ins_generic_linestring_stmt, 3, blob,
			     blob_size, SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_generic_linestring_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr, "sqlite3_step() error: INS_GENERIC_LINESTRING\n");
	  sqlite3_finalize (params->ins_generic_linestring_stmt);
	  params->ins_generic_linestring_stmt = NULL;
      }
}

static void
polygon_generic_insert (struct aux_params *params, sqlite3_int64 id,
			unsigned char *blob, int blob_size, const char *name)
{
    if (params->ins_generic_polygon_stmt)
      {
	  int ret;
	  sqlite3_reset (params->ins_generic_polygon_stmt);
	  sqlite3_clear_bindings (params->ins_generic_polygon_stmt);
	  sqlite3_bind_int64 (params->ins_generic_polygon_stmt, 1, id);
	  if (name == NULL)
	      sqlite3_bind_null (params->ins_generic_polygon_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_generic_polygon_stmt, 2, name,
				 strlen (name), SQLITE_STATIC);
	  sqlite3_bind_blob (params->ins_generic_polygon_stmt, 3, blob,
			     blob_size, SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_generic_polygon_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr, "sqlite3_step() error: INS_GENERIC_POLYGON\n");
	  sqlite3_finalize (params->ins_generic_polygon_stmt);
	  params->ins_generic_polygon_stmt = NULL;
      }
}

static void
tmp_ways_insert (struct aux_params *params, sqlite3_int64 id, int area,
		 unsigned char *blob, int blob_size)
{
    int ret;
    if (params->ins_tmp_ways_stmt == NULL)
	return;
    sqlite3_reset (params->ins_tmp_ways_stmt);
    sqlite3_clear_bindings (params->ins_tmp_ways_stmt);
    sqlite3_bind_int64 (params->ins_tmp_ways_stmt, 1, id);
    sqlite3_bind_int (params->ins_tmp_ways_stmt, 2, area);
    sqlite3_bind_blob (params->ins_tmp_ways_stmt, 3, blob, blob_size,
		       SQLITE_STATIC);
    ret = sqlite3_step (params->ins_tmp_ways_stmt);

    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	return;
    fprintf (stderr, "sqlite3_step() error: INS_TMP_WAYS\n");
    sqlite3_finalize (params->ins_tmp_ways_stmt);
    params->ins_tmp_ways_stmt = NULL;
}

static void
eval_way (struct aux_params *params, int area, unsigned char *blob,
	  int blob_size)
{
    struct tag *p_tag;
    const char *p;
    int i = 0;
    const char *layer_name = NULL;
    char *sub_type = NULL;
    char *name = NULL;
    if (glob_way.first == NULL)
	return;
    while (1)
      {
	  p = base_layers[i++].name;
	  if (!p)
	      break;
	  p_tag = glob_way.first;
	  while (p_tag)
	    {
		if (strcmp (p_tag->k, p) == 0)
		  {
		      layer_name = p;
		      sub_type = p_tag->v;
		  }
		if (strcmp (p_tag->k, "name") == 0)
		    name = p_tag->v;
		p_tag = p_tag->next;
	    }
	  if (layer_name)
	      break;
      }
    if (layer_name)
      {
	  if (area)
	      polygon_layer_insert (params, layer_name, glob_way.id, blob,
				    blob_size, sub_type, name);
	  else
	      line_layer_insert (params, layer_name, glob_way.id, blob,
				 blob_size, sub_type, name);
	  return;
      }
    else if (name != NULL)
      {
	  if (area)
	      polygon_generic_insert (params, glob_way.id, blob, blob_size,
				      name);
	  else
	      line_generic_insert (params, glob_way.id, blob, blob_size, name);
	  return;
      }
}

static int
is_areal_layer ()
{
    struct tag *p_tag;
    const char *p;
    int i = 0;
    const char *layer_name = NULL;
    if (glob_way.first == NULL)
	return;
    while (1)
      {
	  p = base_layers[i++].name;
	  if (!p)
	      break;
	  p_tag = glob_way.first;
	  while (p_tag)
	    {
		if (strcmp (p_tag->k, p) == 0)
		    layer_name = p;
		p_tag = p_tag->next;
	    }
	  if (layer_name)
	      break;
      }
    if (layer_name)
      {
	  /* possible "areal" layers */
	  if (strcmp (layer_name, "amenity") == 0)
	      return 1;
	  if (strcmp (layer_name, "building") == 0)
	      return 1;
	  if (strcmp (layer_name, "historic") == 0)
	      return 1;
	  if (strcmp (layer_name, "landuse") == 0)
	      return 1;
	  if (strcmp (layer_name, "leisure") == 0)
	      return 1;
	  if (strcmp (layer_name, "natural") == 0)
	      return 1;
	  if (strcmp (layer_name, "parking") == 0)
	      return 1;
	  if (strcmp (layer_name, "place") == 0)
	      return 1;
	  if (strcmp (layer_name, "shop") == 0)
	      return 1;
	  if (strcmp (layer_name, "sport") == 0)
	      return 1;
	  if (strcmp (layer_name, "tourism") == 0)
	      return 1;
      }
    return 0;
}

static int
is_closed (gaiaGeomCollPtr geom)
{
    gaiaLinestringPtr ln = geom->FirstLinestring;
    double x0;
    double y0;
    double x1;
    double y1;
    int last = ln->Points - 1;
    gaiaGetPoint (ln->Coords, 0, &x0, &y0);
    gaiaGetPoint (ln->Coords, last, &x1, &y1);
    if (x0 == x1 && y0 == y1)
	return 1;
    return 0;
}

static gaiaGeomCollPtr
convert_to_polygon (gaiaGeomCollPtr old)
{
/* converting a LINESTRING as MULTIPOLYGON */
    gaiaLinestringPtr ln = old->FirstLinestring;
    gaiaPolygonPtr pg;
    gaiaRingPtr rng;
    int iv;
    double x;
    double y;
    gaiaGeomCollPtr geom = gaiaAllocGeomColl ();
    geom->Srid = old->Srid;
    geom->DeclaredType = GAIA_MULTIPOLYGON;
    pg = gaiaAddPolygonToGeomColl (geom, ln->Points, 0);
    rng = pg->Exterior;
    for (iv = 0; iv < rng->Points; iv++)
      {
	  gaiaGetPoint (ln->Coords, iv, &x, &y);
	  gaiaSetPoint (rng->Coords, iv, x, y);
      }
    gaiaFreeGeomColl (old);
    return geom;
}

static void
end_way (struct aux_params *params)
{
    struct tag *p_tag;
    struct tag *p_tag2;
    struct nd *p_nd;
    struct nd *p_nd2;
    unsigned char *blob;
    int blob_size;
    int area = 0;

    gaiaGeomCollPtr geom = build_linestring (params->db_handle);
    if (geom)
      {
	  geom->DeclaredType = GAIA_MULTILINESTRING;
	  gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	  tmp_ways_insert (params, glob_way.id, area, blob, blob_size);
	  area = 0;
	  p_tag = glob_way.first;
	  while (p_tag)
	    {
		if (strcmp (p_tag->k, "area") == 0
		    && strcmp (p_tag->v, "yes") == 0)
		    area = 1;
		p_tag = p_tag->next;
	    }

	  /* attempting to recover undeclared areas */
	  if (is_areal_layer () && is_closed (geom))
	      area = 1;

	  if (area)
	    {
		free (blob);
		geom = convert_to_polygon (geom);
		gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	    }
	  gaiaFreeGeomColl (geom);
	  eval_way (params, area, blob, blob_size);
	  free (blob);
      }
    p_tag = glob_way.first;
    while (p_tag)
      {
	  p_tag2 = p_tag->next;
	  free_tag (p_tag);
	  p_tag = p_tag2;
      }
    p_nd = glob_way.first_nd;
    while (p_nd)
      {
	  p_nd2 = p_nd->next;
	  free (p_nd);
	  p_nd = p_nd2;
      }
    glob_way.id = -1;
    glob_way.first_nd = NULL;
    glob_way.last_nd = NULL;
    glob_way.first = NULL;
    glob_way.last = NULL;
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_nd (const char **attr)
{
    struct nd *p_nd = malloc (sizeof (struct nd));
    int i;
    p_nd->ref = -1;
    p_nd->found = 0;
    p_nd->next = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "ref") == 0)
	      p_nd->ref = atol_64 (attr[i + 1]);
      }
    if (glob_way.first_nd == NULL)
	glob_way.first_nd = p_nd;
    if (glob_way.last_nd != NULL)
	glob_way.last_nd->next = p_nd;
    glob_way.last_nd = p_nd;
}

static void
start_xtag (struct aux_params *params, const char **attr)
{
    struct tag *p_tag = malloc (sizeof (struct tag));
    int i;
    int len;
    p_tag->k = NULL;
    p_tag->v = NULL;
    p_tag->next = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "k") == 0)
	    {
		len = strlen (attr[i + 1]);
		p_tag->k = malloc (len + 1);
		strcpy (p_tag->k, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "v") == 0)
	    {
		len = strlen (attr[i + 1]);
		p_tag->v = malloc (len + 1);
		strcpy (p_tag->v, attr[i + 1]);
	    }
      }
    if (params->current_tag == CURRENT_TAG_IS_NODE)
      {
	  if (glob_node.first == NULL)
	      glob_node.first = p_tag;
	  if (glob_node.last != NULL)
	      glob_node.last->next = p_tag;
	  glob_node.last = p_tag;
      }
    if (params->current_tag == CURRENT_TAG_IS_WAY)
      {
	  if (glob_way.first == NULL)
	      glob_way.first = p_tag;
	  if (glob_way.last != NULL)
	      glob_way.last->next = p_tag;
	  glob_way.last = p_tag;
      }
    if (params->current_tag == CURRENT_TAG_IS_RELATION)
      {
	  if (glob_relation.first == NULL)
	      glob_relation.first = p_tag;
	  if (glob_relation.last != NULL)
	      glob_relation.last->next = p_tag;
	  glob_relation.last = p_tag;
      }
}

static void
start_member (const char **attr)
{
    struct member *p_member = malloc (sizeof (struct member));
    int i;
    int len;
    p_member->ref = -1;
    p_member->is_node = 0;
    p_member->is_way = 0;
    p_member->found = 0;
    p_member->role = NULL;
    p_member->next = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "type") == 0)
	    {
		if (strcmp (attr[i + 1], "node") == 0)
		  {
		      p_member->is_node = 1;
		      p_member->is_way = 0;
		  }
		else if (strcmp (attr[i + 1], "way") == 0)
		  {
		      p_member->is_node = 0;
		      p_member->is_way = 1;
		  }
		else
		  {
		      p_member->is_node = 0;
		      p_member->is_way = 0;
		  }
	    }
	  if (strcmp (attr[i], "ref") == 0)
	      p_member->ref = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "role") == 0)
	    {
		len = strlen (attr[i + 1]);
		p_member->role = malloc (len + 1);
		strcpy (p_member->role, attr[i + 1]);
	    }
      }
    if (glob_relation.first_member == NULL)
	glob_relation.first_member = p_member;
    if (glob_relation.last_member != NULL)
	glob_relation.last_member->next = p_member;
    glob_relation.last_member = p_member;
}

static void
start_relation (struct aux_params *params, const char **attr)
{
    int i;
    glob_relation.id = -1;
    glob_relation.first_member = NULL;
    glob_relation.last_member = NULL;
    glob_relation.first = NULL;
    glob_relation.last = NULL;
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      glob_relation.id = atol_64 (attr[i + 1]);
      }
    params->current_tag = CURRENT_TAG_IS_RELATION;
}

static gaiaGeomCollPtr
build_multilinestring (sqlite3 * db_handle)
{
    gaiaGeomCollPtr geom;
    int lines = 0;
    int tbd;
    int block = 128;
    int base = 0;
    int how_many;
    int ind;
    int ret;
    int count;
    char sql[8192];
    sqlite3_stmt *stmt;
    sqlite3_int64 id;
    const unsigned char *blob = NULL;
    int blob_size;
    gaiaGeomCollPtr org_geom;
    struct member *p_member = glob_relation.first_member;
    while (p_member)
      {
	  lines++;
	  p_member = p_member->next;
      }
    if (!lines)
	return NULL;
    geom = gaiaAllocGeomColl ();
    geom->Srid = 4326;
    geom->DeclaredType = GAIA_MULTILINESTRING;
    tbd = lines;
    while (tbd > 0)
      {
	  /* 
	     / fetching ways
	     / requesting max 128 points at each time 
	   */
	  if (tbd < block)
	      how_many = tbd;
	  else
	      how_many = block;
	  strcpy (sql, "SELECT id, Geometry FROM osm_tmp_ways ");
	  strcat (sql, "WHERE id IN (");
	  for (ind = 0; ind < how_many; ind++)
	    {
		if (ind == 0)
		    strcat (sql, "?");
		else
		    strcat (sql, ",?");
	    }
	  strcat (sql, ")");
	  ret = sqlite3_prepare_v2 (db_handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (db_handle));
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  ind = 1;
	  count = 0;
	  p_member = glob_relation.first_member;
	  while (p_member)
	    {
		if (count < base)
		  {
		      count++;
		      p_member = p_member->next;
		      continue;
		  }
		if (count >= (base + how_many))
		    break;
		sqlite3_bind_int64 (stmt, ind, p_member->ref);
		ind++;
		count++;
		p_member = p_member->next;
	    }
	  while (1)
	    {
		/* scrolling the result set */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		  {
		      /* there are no more rows to fetch - we can stop looping */
		      break;
		  }
		if (ret == SQLITE_ROW)
		  {
		      /* ok, we've just fetched a valid row */
		      id = sqlite3_column_int64 (stmt, 0);
		      blob = sqlite3_column_blob (stmt, 1);
		      blob_size = sqlite3_column_bytes (stmt, 1);
		      org_geom = gaiaFromSpatiaLiteBlobWkb (blob, blob_size);

		      if (org_geom)
			{
			    p_member = glob_relation.first_member;
			    count = 0;
			    while (p_member)
			      {
				  if (p_member->ref == id)
				    {
					gaiaLinestringPtr ln1;
					p_member->found = 1;
					ln1 = org_geom->FirstLinestring;
					while (ln1)
					  {
					      int iv;
					      gaiaLinestringPtr ln2 =
						  gaiaAddLinestringToGeomColl
						  (geom, ln1->Points);
					      for (iv = 0; iv < ln2->Points;
						   iv++)
						{
						    double x;
						    double y;
						    gaiaGetPoint (ln1->Coords,
								  iv, &x, &y);
						    gaiaSetPoint (ln2->Coords,
								  iv, x, y);
						}
					      ln1 = ln1->Next;
					  }
				    }
				  count++;
				  p_member = p_member->next;
			      }
			    gaiaFreeGeomColl (org_geom);
			}
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (db_handle));
		      sqlite3_finalize (stmt);
		      gaiaFreeGeomColl (geom);
		  }
	    }
	  sqlite3_finalize (stmt);
	  tbd -= how_many;
	  base += how_many;
      }

/* final checkout */
    p_member = glob_relation.first_member;
    while (p_member)
      {
	  if (p_member->found == 0)
	    {
#if defined(_WIN32) || defined(__MINGW32__)
		/* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
		fprintf (stderr, "UNRESOLVED-WAY %I64d\n", p_member->ref);
#else
		fprintf (stderr, "UNRESOLVED-WAY %lld\n", p_member->ref);
#endif
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  p_member = p_member->next;
      }
    return geom;
}

static void
multiline_layer_insert (struct aux_params *params, const char *layer_name,
			const char *sub_type, const char *name)
{
    gaiaGeomCollPtr geom;
    unsigned char *blob = NULL;
    int blob_size;
    struct layers *layer;
    int i = 0;
    while (1)
      {
	  layer = &(base_layers[i++]);
	  if (layer->name == NULL)
	      return;
	  if (strcmp (layer->name, layer_name) == 0)
	    {
		if (layer->ok_linestring == 0)
		  {
		      layer->ok_linestring = 1;
		      create_linestring_table (params, layer);
		  }
		geom = build_multilinestring (params->db_handle);
		if (geom)
		  {
		      gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
		      gaiaFreeGeomColl (geom);
		  }
		if (layer->ins_linestring_stmt && blob)
		  {
		      int ret;
		      sqlite3_reset (layer->ins_linestring_stmt);
		      sqlite3_clear_bindings (layer->ins_linestring_stmt);
		      sqlite3_bind_int64 (layer->ins_linestring_stmt, 1,
					  glob_relation.id);
		      if (sub_type == NULL)
			  sqlite3_bind_null (layer->ins_linestring_stmt, 2);
		      else
			  sqlite3_bind_text (layer->ins_linestring_stmt, 2,
					     sub_type, strlen (sub_type),
					     SQLITE_STATIC);
		      if (name == NULL)
			  sqlite3_bind_null (layer->ins_linestring_stmt, 3);
		      else
			  sqlite3_bind_text (layer->ins_linestring_stmt, 3,
					     name, strlen (name),
					     SQLITE_STATIC);
		      sqlite3_bind_blob (layer->ins_linestring_stmt, 4, blob,
					 blob_size, free);
		      ret = sqlite3_step (layer->ins_linestring_stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  return;
		      fprintf (stderr,
			       "sqlite3_step() error: INS_MULTILINESTRING %s\n",
			       layer_name);
		      sqlite3_finalize (layer->ins_linestring_stmt);
		      layer->ins_linestring_stmt = NULL;
		  }
		return;
	    }
      }
}

static gaiaGeomCollPtr
build_multipolygon (sqlite3 * db_handle)
{
    gaiaGeomCollPtr geom;
    gaiaPolygonPtr pg2 = NULL;
    int rings = 0;
    int interiors = 0;
    int exteriors = 0;
    int ext_pts;
    int ib = 0;
    int tbd;
    int block = 128;
    int base = 0;
    int how_many;
    int ind;
    int ret;
    int count;
    char sql[8192];
    sqlite3_stmt *stmt;
    sqlite3_int64 id;

    const unsigned char *blob = NULL;
    int blob_size;
    gaiaGeomCollPtr org_geom;
    struct member *p_member = glob_relation.first_member;
    while (p_member)
      {
	  rings++;
	  if (p_member->role)
	    {
		if (strcmp (p_member->role, "inner") == 0)
		    interiors++;
		if (strcmp (p_member->role, "outer") == 0)
		    exteriors++;
	    }
	  p_member = p_member->next;
      }
    if (!rings)
	return NULL;
    if (exteriors != 1)
	return NULL;
    if (interiors + 1 != rings)
	return NULL;
    geom = gaiaAllocGeomColl ();
    geom->Srid = 4326;
    geom->DeclaredType = GAIA_MULTIPOLYGON;
    tbd = rings;
    while (tbd > 0)
      {
	  /* 
	     / fetching ways
	     / requesting max 128 points at each time 
	   */
	  if (tbd < block)
	      how_many = tbd;
	  else
	      how_many = block;
	  strcpy (sql, "SELECT id, area, Geometry FROM osm_tmp_ways ");
	  strcat (sql, "WHERE id IN (");
	  for (ind = 0; ind < how_many; ind++)
	    {
		if (ind == 0)
		    strcat (sql, "?");
		else
		    strcat (sql, ",?");
	    }
	  strcat (sql, ")");
	  ret = sqlite3_prepare_v2 (db_handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (db_handle));
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  ind = 1;
	  count = 0;
	  p_member = glob_relation.first_member;
	  while (p_member)
	    {
		if (count < base)
		  {
		      count++;
		      p_member = p_member->next;
		      continue;
		  }
		if (count >= (base + how_many))
		    break;
		sqlite3_bind_int64 (stmt, ind, p_member->ref);
		ind++;
		count++;
		p_member = p_member->next;
	    }
	  while (1)
	    {
		/* scrolling the result set */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		  {
		      /* there are no more rows to fetch - we can stop looping */
		      break;
		  }
		if (ret == SQLITE_ROW)
		  {
		      /* ok, we've just fetched a valid row */
		      id = sqlite3_column_int64 (stmt, 0);
		      blob = sqlite3_column_blob (stmt, 2);
		      blob_size = sqlite3_column_bytes (stmt, 2);
		      org_geom = gaiaFromSpatiaLiteBlobWkb (blob, blob_size);
		      if (org_geom)
			{
			    while (p_member)
			      {
				  if (p_member->ref == id)
				    {
					p_member->geom = org_geom;
					p_member->found = 1;
				    }
				  p_member = p_member->next;
			      }
			}
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (db_handle));
		      sqlite3_finalize (stmt);
		      gaiaFreeGeomColl (geom);
		  }
	    }
	  sqlite3_finalize (stmt);
	  tbd -= how_many;
	  base += how_many;
      }

/* final checkout */
    ext_pts = 0;
    p_member = glob_relation.first_member;
    while (p_member)
      {
	  if (p_member->found == 0)
	    {
#if defined(_WIN32) || defined(__MINGW32__)
		/* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
		fprintf (stderr, "UNRESOLVED-WAY %I64d\n", p_member->ref);
#else
		fprintf (stderr, "UNRESOLVED-WAY %lld\n", p_member->ref);
#endif
		gaiaFreeGeomColl (geom);
		return NULL;
	    }
	  if (strcmp (p_member->role, "outer") == 0)
	    {
		if (p_member->geom)
		  {
		      if (p_member->geom->FirstPolygon)
			  ext_pts =
			      p_member->geom->FirstPolygon->Exterior->Points;
		  }
	    }
	  p_member = p_member->next;
      }
    if (!ext_pts)
      {
#if defined(_WIN32) || defined(__MINGW32__)
	  /* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
	  fprintf (stderr, "ILLEGAL MULTIPOLYGON %I64d\n", p_member->ref);
#else
	  fprintf (stderr, "ILLEGAL MULTIPOLYGON %lld\n", p_member->ref);
#endif
	  gaiaFreeGeomColl (geom);
	  return NULL;
      }

    pg2 = gaiaAddPolygonToGeomColl (geom, ext_pts, interiors);
    p_member = glob_relation.first_member;
    while (p_member)
      {
	  gaiaPolygonPtr pg1;
	  pg1 = org_geom->FirstPolygon;
	  while (pg1)
	    {
		int iv;
		gaiaRingPtr rng1 = pg1->Exterior;
		gaiaRingPtr rng2;
		if (strcmp (p_member->role, "outer") == 0)
		    rng2 = pg2->Exterior;
		else
		    rng2 = pg2->Interiors + ib++;
		for (iv = 0; iv < rng2->Points; iv++)
		  {
		      double x;
		      double y;
		      gaiaGetPoint (rng1->Coords, iv, &x, &y);
		      gaiaSetPoint (rng2->Coords, iv, x, y);
		  }
		pg1 = pg1->Next;
	    }
	  p_member = p_member->next;
      }
    return geom;
}

static void
multipolygon_layer_insert (struct aux_params *params, const char *layer_name,
			   const char *sub_type, const char *name)
{
    gaiaGeomCollPtr geom;
    unsigned char *blob = NULL;
    int blob_size;
    struct layers *layer;
    int i = 0;
    while (1)
      {
	  layer = &(base_layers[i++]);
	  if (layer->name == NULL)
	      return;
	  if (strcmp (layer->name, layer_name) == 0)
	    {
		if (layer->ok_polygon == 0)
		  {
		      layer->ok_polygon = 1;
		      create_polygon_table (params, layer);
		  }
		geom = build_multipolygon (params->db_handle);
		if (geom)
		  {
		      gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
		      gaiaFreeGeomColl (geom);
		  }
		if (layer->ins_polygon_stmt)
		  {
		      int ret;
		      sqlite3_reset (layer->ins_polygon_stmt);
		      sqlite3_clear_bindings (layer->ins_polygon_stmt);
		      sqlite3_bind_int64 (layer->ins_polygon_stmt, 1,
					  glob_relation.id);
		      if (sub_type == NULL)
			  sqlite3_bind_null (layer->ins_polygon_stmt, 2);
		      else
			  sqlite3_bind_text (layer->ins_polygon_stmt, 2,
					     sub_type, strlen (sub_type),
					     SQLITE_STATIC);
		      if (name == NULL)
			  sqlite3_bind_null (layer->ins_polygon_stmt, 3);
		      else
			  sqlite3_bind_text (layer->ins_polygon_stmt, 3, name,
					     strlen (name), SQLITE_STATIC);
		      sqlite3_bind_blob (layer->ins_polygon_stmt, 4, blob,
					 blob_size, SQLITE_STATIC);
		      ret = sqlite3_step (layer->ins_polygon_stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  return;
		      fprintf (stderr,
			       "sqlite3_step() error: INS_MULTIPOLYGON %s\n",
			       layer_name);
		      sqlite3_finalize (layer->ins_polygon_stmt);
		      layer->ins_polygon_stmt = NULL;
		  }
		return;
	    }
      }
}

static void
multiline_generic_insert (struct aux_params *params, const char *name)
{
    gaiaGeomCollPtr geom;
    unsigned char *blob = NULL;
    int blob_size;
    geom = build_multilinestring (params->db_handle);
    if (geom)
      {
	  gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	  gaiaFreeGeomColl (geom);
      }
    if (params->ins_generic_linestring_stmt && blob)
      {
	  int ret;
	  sqlite3_reset (params->ins_generic_linestring_stmt);
	  sqlite3_clear_bindings (params->ins_generic_linestring_stmt);
	  sqlite3_bind_int64 (params->ins_generic_linestring_stmt, 1,
			      glob_relation.id);
	  if (name == NULL)
	      sqlite3_bind_null (params->ins_generic_linestring_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_generic_linestring_stmt, 2, name,
				 strlen (name), SQLITE_STATIC);
	  sqlite3_bind_blob (params->ins_generic_linestring_stmt, 3, blob,
			     blob_size, SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_generic_linestring_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr,
		   "sqlite3_step() error: INS_GENERIC_MULTILINESTRING\n");
	  sqlite3_finalize (params->ins_generic_linestring_stmt);
	  params->ins_generic_linestring_stmt = NULL;
      }
}

static void
multipolygon_generic_insert (struct aux_params *params, const char *name)
{
    gaiaGeomCollPtr geom;
    unsigned char *blob = NULL;
    int blob_size;
    geom = build_multipolygon (params->db_handle);
    if (geom)
      {
	  gaiaToSpatiaLiteBlobWkb (geom, &blob, &blob_size);
	  gaiaFreeGeomColl (geom);
      }
    if (params->ins_generic_polygon_stmt && blob)
      {
	  int ret;
	  sqlite3_reset (params->ins_generic_polygon_stmt);
	  sqlite3_clear_bindings (params->ins_generic_polygon_stmt);
	  sqlite3_bind_int64 (params->ins_generic_polygon_stmt, 1,
			      glob_relation.id);
	  if (name == NULL)
	      sqlite3_bind_null (params->ins_generic_polygon_stmt, 2);
	  else
	      sqlite3_bind_text (params->ins_generic_polygon_stmt, 2, name,
				 strlen (name), SQLITE_STATIC);
	  sqlite3_bind_blob (params->ins_generic_polygon_stmt, 3, blob,
			     blob_size, SQLITE_STATIC);
	  ret = sqlite3_step (params->ins_generic_polygon_stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      return;
	  fprintf (stderr, "sqlite3_step() error: INS_GENERIC_MULTIPOLYGON\n");
	  sqlite3_finalize (params->ins_generic_polygon_stmt);
	  params->ins_generic_polygon_stmt = NULL;
      }
}

static void
eval_relation (struct aux_params *params)
{
    struct tag *p_tag;
    const char *p;
    int i = 0;
    const char *layer_name = NULL;
    char *sub_type = NULL;
    char *name = NULL;
    int multipolygon = 0;
    if (glob_relation.first == NULL)
	return;
    while (1)
      {
	  p = base_layers[i++].name;
	  if (!p)
	      break;
	  p_tag = glob_relation.first;
	  while (p_tag)
	    {
		if (strcmp (p_tag->k, p) == 0)
		  {
		      layer_name = p;
		      sub_type = p_tag->v;
		  }
		if (strcmp (p_tag->k, "name") == 0)
		    name = p_tag->v;
		if (strcmp (p_tag->k, "type") == 0
		    && strcmp (p_tag->v, "multipolygon") == 0)
		    multipolygon = 1;
		p_tag = p_tag->next;
	    }
	  if (layer_name)
	      break;
      }
    if (layer_name)
      {
	  if (multipolygon)
	      multipolygon_layer_insert (params, layer_name, sub_type, name);
	  else
	      multiline_layer_insert (params, layer_name, sub_type, name);
	  return;
      }
    else if (name != NULL)
      {
	  if (multipolygon)
	      multipolygon_generic_insert (params, name);
	  else
	      multiline_generic_insert (params, name);
	  return;
      }
}

static void
end_relation (struct aux_params *params)
{
    struct tag *p_tag;
    struct tag *p_tag2;
    struct member *p_member;
    struct member *p_member2;
    eval_relation (params);
    p_tag = glob_relation.first;
    while (p_tag)
      {
	  p_tag2 = p_tag->next;
	  free_tag (p_tag);
	  p_tag = p_tag2;
      }
    p_member = glob_relation.first_member;
    while (p_member)
      {
	  p_member2 = p_member->next;
	  free_member (p_member);
	  p_member = p_member2;
      }
    glob_relation.id = -1;
    glob_relation.first_member = NULL;
    glob_relation.last_member = NULL;
    glob_relation.first = NULL;
    glob_relation.last = NULL;
    params->current_tag = CURRENT_TAG_UNKNOWN;
}

static void
start_tag (void *data, const char *el, const char **attr)
{
    struct aux_params *params = (struct aux_params *) data;
    if (strcmp (el, "node") == 0)
	start_node (params, attr);
    if (strcmp (el, "tag") == 0)
	start_xtag (params, attr);
    if (strcmp (el, "way") == 0)
	start_way (params, attr);
    if (strcmp (el, "nd") == 0)
	start_nd (attr);
    if (strcmp (el, "relation") == 0)
	start_relation (params, attr);
    if (strcmp (el, "member") == 0)
	start_member (attr);
}

static void
end_tag (void *data, const char *el)
{
    struct aux_params *params = (struct aux_params *) data;
    if (strcmp (el, "node") == 0)
	end_node (params);
    if (strcmp (el, "way") == 0)
	end_way (params);
    if (strcmp (el, "relation") == 0)
	end_relation (params);
}

static void
db_vacuum (sqlite3 * db_handle)
{
    int ret;
    char *sql_err = NULL;
/* VACUUMing the DB */
    printf ("\nVACUUMing the DB ... wait please ...\n");
    ret = sqlite3_exec (db_handle, "VACUUM", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "VACUUM error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    printf ("\tAll done: OSM map was succesfully loaded\n");
}

static void
spatialite_autocreate (sqlite3 * db)
{
/* attempting to perform self-initialization for a newly created DB */
    int ret;
    char sql[1024];
    char *err_msg = NULL;
    int count;
    int i;
    char **results;
    int rows;
    int columns;

/* checking if this DB is really empty */
    strcpy (sql, "SELECT Count(*) from sqlite_master");
    ret = sqlite3_get_table (db, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      count = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);

    if (count > 0)
	return;

/* all right, it's empty: proceding to initialize */
    strcpy (sql, "SELECT InitSpatialMetadata()");
    ret = sqlite3_exec (db, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "InitSpatialMetadata() error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
}

static void
open_db (const char *path, sqlite3 ** handle, int cache_size)
{
/* opening the DB */
    sqlite3 *db_handle;
    int ret;
    char sql[1024];
    char *err_msg = NULL;
    int spatialite_rs = 0;
    int spatialite_gc = 0;
    int rs_srid = 0;
    int auth_name = 0;
    int auth_srid = 0;
    int ref_sys_name = 0;
    int proj4text = 0;
    int f_table_name = 0;
    int f_geometry_column = 0;
    int coord_dimension = 0;
    int gc_srid = 0;
    int type = 0;
    int spatial_index_enabled = 0;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

    *handle = NULL;
    spatialite_init (0);
    printf ("SQLite version: %s\n", sqlite3_libversion ());
    printf ("SpatiaLite version: %s\n\n", spatialite_version ());

    ret =
	sqlite3_open_v2 (path, &db_handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", path,
		   sqlite3_errmsg (db_handle));
	  sqlite3_close (db_handle);
	  return;
      }
    spatialite_autocreate (db_handle);
    if (cache_size > 0)
      {
	  /* setting the CACHE-SIZE */
	  sprintf (sql, "PRAGMA cache_size=%d", cache_size);
	  sqlite3_exec (db_handle, sql, NULL, NULL, NULL);
      }

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "PRAGMA table_info(geometry_columns)");
    ret = sqlite3_get_table (db_handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "f_table_name") == 0)
		    f_table_name = 1;
		if (strcasecmp (name, "f_geometry_column") == 0)
		    f_geometry_column = 1;
		if (strcasecmp (name, "coord_dimension") == 0)
		    coord_dimension = 1;
		if (strcasecmp (name, "srid") == 0)
		    gc_srid = 1;
		if (strcasecmp (name, "type") == 0)
		    type = 1;
		if (strcasecmp (name, "spatial_index_enabled") == 0)
		    spatial_index_enabled = 1;
	    }
      }
    sqlite3_free_table (results);
    if (f_table_name && f_geometry_column && type && coord_dimension
	&& gc_srid && spatial_index_enabled)
	spatialite_gc = 1;

/* checking the SPATIAL_REF_SYS table */
    strcpy (sql, "PRAGMA table_info(spatial_ref_sys)");
    ret = sqlite3_get_table (db_handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "srid") == 0)
		    rs_srid = 1;
		if (strcasecmp (name, "auth_name") == 0)
		    auth_name = 1;
		if (strcasecmp (name, "auth_srid") == 0)
		    auth_srid = 1;
		if (strcasecmp (name, "ref_sys_name") == 0)
		    ref_sys_name = 1;
		if (strcasecmp (name, "proj4text") == 0)
		    proj4text = 1;
	    }
      }
    sqlite3_free_table (results);
    if (rs_srid && auth_name && auth_srid && ref_sys_name && proj4text)
	spatialite_rs = 1;
/* verifying the MetaData format */
    if (spatialite_gc && spatialite_rs)
	;
    else
	goto unknown;

/* creating the OSM temporary nodes */
    strcpy (sql, "CREATE TABLE osm_tmp_nodes (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "lat DOUBLE NOT NULL,\n");
    strcat (sql, "lon DOUBLE NOT NULL)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_tmp_nodes' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the OSM temporary ways */
    strcpy (sql, "CREATE TABLE osm_tmp_ways (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "area INTEGER NOT NULL)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_tmp_ways' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('osm_tmp_ways', 'Geometry', 4326, 'MULTILINESTRING', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'tmp_osm_ways' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the PT_GENERIC table */
    strcpy (sql, "CREATE TABLE pt_generic (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('pt_generic', 'Geometry', 4326, 'POINT', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the PT_ADDRESSES table */
    strcpy (sql, "CREATE TABLE pt_addresses (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "country TEXT,\n");
    strcat (sql, "city TEXT,\n");
    strcat (sql, "postcode TEXT,\n");
    strcat (sql, "street TEXT,\n");
    strcat (sql, "housename TEXT,\n");
    strcat (sql, "housenumber TEXT)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_addresses' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('pt_addresses', 'Geometry', 4326, 'POINT', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pt_addresses' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the LN_GENERIC table */
    strcpy (sql, "CREATE TABLE ln_generic (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'ln_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('ln_generic', 'Geometry', 4326, 'MULTILINESTRING', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'ln_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }
/* creating the PG_GENERIC table */
    strcpy (sql, "CREATE TABLE pg_generic (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "name TEXT)\n");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pg_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return;
      }
    strcpy (sql,
	    "SELECT AddGeometryColumn('pg_generic', 'Geometry', 4326, 'MULTIPOLYGON', 'XY')");
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'pg_generic' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (db_handle);
	  return;
      }

    *handle = db_handle;
    return;

  unknown:
    if (db_handle)
	sqlite3_close (db_handle);
    fprintf (stderr, "DB '%s'\n", path);
    fprintf (stderr, "doesn't seems to contain valid Spatial Metadata ...\n\n");
    fprintf (stderr, "Please, initialize Spatial Metadata\n\n");
    return;
}

static void
finalize_sql_stmts (struct aux_params *params)
{
    int ret;
    char *sql_err = NULL;
    struct layers layer;
    int i = 0;

    while (1)
      {
	  layer = base_layers[i++];
	  if (layer.name == NULL)
	      break;
	  if (layer.ins_point_stmt)
	      sqlite3_finalize (layer.ins_point_stmt);
	  if (layer.ins_linestring_stmt)
	      sqlite3_finalize (layer.ins_linestring_stmt);
	  if (layer.ins_polygon_stmt)
	      sqlite3_finalize (layer.ins_polygon_stmt);
	  layer.ins_point_stmt = NULL;
	  layer.ins_linestring_stmt = NULL;
	  layer.ins_polygon_stmt = NULL;
      }

    if (params->ins_tmp_nodes_stmt != NULL)
	sqlite3_finalize (params->ins_tmp_nodes_stmt);
    if (params->ins_tmp_ways_stmt != NULL)
	sqlite3_finalize (params->ins_tmp_ways_stmt);
    if (params->ins_generic_point_stmt != NULL)
	sqlite3_finalize (params->ins_generic_point_stmt);
    if (params->ins_addresses_stmt != NULL)
	sqlite3_finalize (params->ins_addresses_stmt);
    if (params->ins_generic_linestring_stmt != NULL)
	sqlite3_finalize (params->ins_generic_linestring_stmt);
    if (params->ins_generic_polygon_stmt != NULL)
	sqlite3_finalize (params->ins_generic_polygon_stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (params->db_handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
}

static void
create_sql_stmts (struct aux_params *params)
{
    sqlite3_stmt *ins_tmp_nodes_stmt;
    sqlite3_stmt *ins_tmp_ways_stmt;
    sqlite3_stmt *ins_generic_point_stmt;
    sqlite3_stmt *ins_addresses_stmt;
    sqlite3_stmt *ins_generic_linestring_stmt;
    sqlite3_stmt *ins_generic_polygon_stmt;
    char sql[1024];
    int ret;
    char *sql_err = NULL;

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (params->db_handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_tmp_nodes (id, lat, lon) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_tmp_nodes_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO osm_tmp_ways (id, area, geometry) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_tmp_ways_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO pt_generic (id, name, Geometry) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_generic_point_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO pt_addresses (id, country, city, postcode, street, housename, housenumber, Geometry) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_addresses_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO ln_generic (id, name, Geometry) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_generic_linestring_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }
    strcpy (sql, "INSERT INTO pg_generic (id, name, Geometry) ");
    strcat (sql, "VALUES (?, ?, ?)");
    ret =
	sqlite3_prepare_v2 (params->db_handle, sql, strlen (sql),
			    &ins_generic_polygon_stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql,
		   sqlite3_errmsg (params->db_handle));
	  finalize_sql_stmts (params);
	  return;
      }

    params->ins_tmp_nodes_stmt = ins_tmp_nodes_stmt;
    params->ins_tmp_ways_stmt = ins_tmp_ways_stmt;
    params->ins_generic_point_stmt = ins_generic_point_stmt;
    params->ins_addresses_stmt = ins_addresses_stmt;
    params->ins_generic_linestring_stmt = ins_generic_linestring_stmt;
    params->ins_generic_polygon_stmt = ins_generic_polygon_stmt;
}

static void
db_cleanup (sqlite3 * db_handle)
{
/* dropping temporary tables OSM_TMP_xxx */
    int ret;
    char *sql_err = NULL;

/* dropping OSM_TMP_NODES */
    ret =
	sqlite3_exec (db_handle, "DROP TABLE osm_tmp_nodes", NULL, NULL,
		      &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE 'osm_tmp_nodes' error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }

/* dropping OSM_TMP_WAYS */
    ret =
	sqlite3_exec (db_handle,
		      "DELETE FROM geometry_columns WHERE f_table_name = 'osm_tmp_ways'",
		      NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "Dropping Geometry from 'osm_tmp_ways' error: %s\n",
		   sql_err);
	  sqlite3_free (sql_err);
      }
    ret =
	sqlite3_exec (db_handle, "DROP TABLE osm_tmp_ways", NULL, NULL,
		      &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DROP TABLE 'osm_tmp_ways' error: %s\n", sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
do_spatial_index (sqlite3 * db_handle, const char *table, const char *geom)
{
/* creating some Spatial Index */
    char sql[1024];
    int ret;
    char *sql_err = NULL;

    sprintf (sql, "SELECT CreateSpatialIndex('%s', '%s')", table, geom);
    ret = sqlite3_exec (db_handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SpatialIndex %s'.'%s' error: %s\n", table, geom,
		   sql_err);
	  sqlite3_free (sql_err);
      }
}

static void
create_spatial_index (sqlite3 * db_handle)
{
/* attempting to create any Spatial Index */
    const char *table;
    const char *geom;
    char sql[1024];
    int i;
    char **results;
    int rows;
    int columns;
    int ret;

    strcpy (sql,
	    "SELECT f_table_name, f_geometry_column FROM geometry_columns");
    ret = sqlite3_get_table (db_handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		table = results[(i * columns) + 0];
		geom = results[(i * columns) + 1];
		do_spatial_index (db_handle, table, geom);
	    }
      }
    sqlite3_free_table (results);
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: spatialite_osm_map ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr,
	     "-h or --help                    print this help message\n");
    fprintf (stderr, "-o or --osm-path pathname       the OSM-XML file path\n");
    fprintf (stderr,
	     "-d or --db-path  pathname       the SpatiaLite DB path\n\n");
    fprintf (stderr, "you can specify the following options as well\n");
    fprintf (stderr,
	     "-cs or --cache-size    num      DB cache size (how many pages)\n");
    fprintf (stderr,
	     "-m or --in-memory               using IN-MEMORY database\n");
    fprintf (stderr,
	     "-n or --no-spatial-index        suppress R*Trees generation\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    sqlite3 *handle;
    int i;
    int next_arg = ARG_NONE;
    const char *osm_path = NULL;
    const char *db_path = NULL;
    int in_memory = 0;
    int cache_size = 0;
    int spatial_index = 1;
    int error = 0;
    char Buff[BUFFSIZE];
    int done = 0;
    int len;
    XML_Parser parser;
    struct aux_params params;
    FILE *xml_file;
    int last_line_no = 0;
    int curr_line_no = 0;

/* initializing the aux-struct */
    params.db_handle = NULL;
    params.ins_tmp_nodes_stmt = NULL;
    params.ins_tmp_ways_stmt = NULL;
    params.ins_generic_point_stmt = NULL;
    params.ins_addresses_stmt = NULL;
    params.ins_generic_linestring_stmt = NULL;
    params.ins_generic_polygon_stmt = NULL;
    params.current_tag = CURRENT_TAG_UNKNOWN;

    for (i = 1; i < argc; i++)
      {
	  /* parsing the invocation arguments */
	  if (next_arg != ARG_NONE)
	    {
		switch (next_arg)
		  {
		  case ARG_OSM_PATH:
		      osm_path = argv[i];
		      break;
		  case ARG_DB_PATH:
		      db_path = argv[i];
		      break;
		  case ARG_CACHE_SIZE:
		      cache_size = atoi (argv[i]);
		      break;
		  };
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "--help") == 0
	      || strcmp (argv[i], "-h") == 0)
	    {
		do_help ();
		return -1;
	    }
	  if (strcmp (argv[i], "-o") == 0)
	    {
		next_arg = ARG_OSM_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--osm-path") == 0)
	    {
		next_arg = ARG_OSM_PATH;
		continue;
	    }
	  if (strcmp (argv[i], "-d") == 0)
	    {
		next_arg = ARG_DB_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--db-path") == 0)
	    {
		next_arg = ARG_DB_PATH;
		continue;
	    }
	  if (strcasecmp (argv[i], "--cache-size") == 0
	      || strcmp (argv[i], "-cs") == 0)
	    {
		next_arg = ARG_CACHE_SIZE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-m") == 0)
	    {
		in_memory = 1;
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-in-memory") == 0)
	    {
		in_memory = 1;
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-n") == 0)
	    {
		spatial_index = 0;
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-no-spatial-index") == 0)
	    {
		spatial_index = 0;
		next_arg = ARG_NONE;
		continue;
	    }
	  fprintf (stderr, "unknown argument: %s\n", argv[i]);
	  error = 1;
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }

/* checking the arguments */
    if (!osm_path)
      {
	  fprintf (stderr,
		   "did you forget setting the --osm-path argument ?\n");
	  error = 1;
      }
    if (!db_path)
      {
	  fprintf (stderr, "did you forget setting the --db-path argument ?\n");
	  error = 1;
      }

    if (error)
      {
	  do_help ();
	  return -1;
      }

/* opening the DB */
    if (in_memory)
	cache_size = 0;
    open_db (db_path, &handle, cache_size);
    if (!handle)
	return -1;
    if (in_memory)
      {
	  /* loading the DB in-memory */
	  sqlite3 *mem_db_handle;
	  sqlite3_backup *backup;
	  int ret;
	  ret =
	      sqlite3_open_v2 (":memory:", &mem_db_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open 'MEMORY-DB': %s\n",
			 sqlite3_errmsg (mem_db_handle));
		sqlite3_close (mem_db_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (mem_db_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "cannot load 'MEMORY-DB'\n");
		sqlite3_close (handle);
		sqlite3_close (mem_db_handle);
		return -1;
	    }
	  while (1)
	    {
		ret = sqlite3_backup_step (backup, 1024);
		if (ret == SQLITE_DONE)
		    break;
	    }
	  ret = sqlite3_backup_finish (backup);
	  sqlite3_close (handle);
	  handle = mem_db_handle;
	  printf ("\nusing IN-MEMORY database\n");
      }
    params.db_handle = handle;

/* creating SQL prepared statements */
    create_sql_stmts (&params);

/* XML parsing */
    xml_file = fopen (osm_path, "rb");
    if (!xml_file)
      {
	  fprintf (stderr, "cannot open %s\n", osm_path);
	  sqlite3_close (handle);
	  return -1;
      }
    parser = XML_ParserCreate (NULL);
    if (!parser)
      {
	  fprintf (stderr, "Couldn't allocate memory for parser\n");
	  sqlite3_close (handle);
	  return -1;
      }
    XML_SetUserData (parser, &params);
    XML_SetElementHandler (parser, start_tag, end_tag);
    while (!done)
      {
	  len = fread (Buff, 1, BUFFSIZE, xml_file);
	  if (ferror (xml_file))
	    {
		fprintf (stderr, "XML Read error\n");
		sqlite3_close (handle);
		return -1;
	    }
	  done = feof (xml_file);
	  if (!XML_Parse (parser, Buff, len, done))
	    {
		fprintf (stderr, "Parse error at line %d:\n%s\n",
			 (int) XML_GetCurrentLineNumber (parser),
			 XML_ErrorString (XML_GetErrorCode (parser)));
		sqlite3_close (handle);
		return -1;
	    }
	  curr_line_no = (int) XML_GetCurrentLineNumber (parser);
	  if ((curr_line_no - last_line_no) > 1000)
	    {
		last_line_no = curr_line_no;
		printf ("Parsing XML line: %d\r", curr_line_no);
		fflush (stdout);
	    }
      }
    XML_ParserFree (parser);
    fclose (xml_file);
    printf ("                                                         \r");

/* finalizing SQL prepared statements */
    finalize_sql_stmts (&params);

/* dropping the OSM_TMP_xx tables */
    db_cleanup (handle);

    if (spatial_index)
      {
	  /* creating any Spatial Index */
	  create_spatial_index (handle);
      }

    if (in_memory)
      {
	  /* exporting the in-memory DB to filesystem */
	  sqlite3 *disk_db_handle;
	  sqlite3_backup *backup;
	  int ret;
	  printf ("\nexporting IN_MEMORY database ... wait please ...\n");
	  ret =
	      sqlite3_open_v2 (db_path, &disk_db_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open '%s': %s\n", db_path,
			 sqlite3_errmsg (disk_db_handle));
		sqlite3_close (disk_db_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (disk_db_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "Backup failure: 'MEMORY-DB' wasn't saved\n");
		sqlite3_close (handle);
		sqlite3_close (disk_db_handle);
		return -1;
	    }
	  while (1)
	    {
		ret = sqlite3_backup_step (backup, 1024);
		if (ret == SQLITE_DONE)
		    break;
	    }
	  ret = sqlite3_backup_finish (backup);
	  sqlite3_close (handle);
	  handle = disk_db_handle;
	  printf ("\tIN_MEMORY database succesfully exported\n");
      }

/* VACUUMing */
    db_vacuum (handle);
    sqlite3_close (handle);
    return 0;
}
