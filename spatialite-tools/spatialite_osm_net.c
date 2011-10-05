/* 
/ spatialite_osm_net
/
/ a tool loading OSM-XML roads into a SpatiaLite DB
/
/ version 1.0, 2009 August 1
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ Copyright (C) 2009  Alessandro Furieri
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
#define ARG_TABLE		3
#define ARG_CACHE_SIZE	4

#define MAX_TAG		16

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#endif /* not WIN32 */

#if defined(_WIN32)
#define atol_64		_atoi64
#else
#define atol_64		atoll
#endif

struct dup_node_id
{
    sqlite3_int64 id;
    struct dup_node_id *next;
};

struct dup_node
{
    double lon;
    double lat;
    int refcount;
    struct dup_node_id *first;
    struct dup_node_id *last;
    struct dup_node *next;
};

struct kv
{
    char *k;
    char *v;
    struct kv *next;
};

struct node_ref
{
    sqlite3_int64 id;
    sqlite3_int64 alias;
    double lat;
    double lon;
    int refcount;
    int ignore;
    struct node_ref *next;
};

struct arc
{
    sqlite3_int64 from;
    sqlite3_int64 to;
    gaiaDynamicLinePtr dyn;
    gaiaGeomCollPtr geom;
    double length;
    double cost;
    struct arc *next;
};

struct way
{
    sqlite3_int64 id;
    char *class;
    char *name;
    int oneway;
    int reverse;
    struct node_ref *first;
    struct node_ref *last;
    struct kv *first_kv;
    struct kv *last_kv;
    struct arc *first_arc;
    struct arc *last_arc;
};

struct check_tag
{
    char buf[MAX_TAG + 1];
    int pos;
    int max;
};

static void
init_tag (struct check_tag *tag, int max)
{
    tag->pos = 0;
    tag->max = max;
    memset (tag->buf, '\0', MAX_TAG + 1);
}

static void
update_tag (struct check_tag *tag, const char c)
{
/* updating the tag control struct */
    int i;
    if (tag->pos == (tag->max - 1))
      {
	  for (i = 0; i < tag->max; i++)
	    {
		/* rotating the buffer */
		*(tag->buf + i) = *(tag->buf + i + 1);
	    }
	  *(tag->buf + tag->pos) = c;
	  return;
      }
    *(tag->buf + tag->pos) = c;
    tag->pos++;
}

static int
parse_node_id (const char *buf, sqlite3_int64 * id)
{
/* parsing <node id="value" > */
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 4);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "id=\"", 4) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		*id = atol_64 (value);
		return 1;
	    }
      }
    return 0;
}

static int
parse_node_lat (const char *buf, double *lat)
{
/* parsing <node lat="value" >*/
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 5);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "lat=\"", 5) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		*lat = atof (value);
		return 1;
	    }
      }
    return 0;
}

static int
parse_node_lon (const char *buf, double *lon)
{
/* parsing <node lon="value" >*/
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 5);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "lon=\"", 5) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		*lon = atof (value);
		return 1;
	    }
      }
    return 0;
}

static int
parse_node_tag (const char *buf, sqlite3_int64 * id, double *lat, double *lon)
{
/* parsing a <node> tag */
    if (!parse_node_id (buf, id))
	return 0;
    if (!parse_node_lat (buf, lat))
	return 0;
    if (!parse_node_lon (buf, lon))
	return 0;
    return 1;
}

static int
insert_node (sqlite3 * handle, sqlite3_stmt * stmt, sqlite3_int64 id,
	     double lat, double lon)
{
/* inserting a temporary node into the DB */
    int ret;
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    sqlite3_bind_int64 (stmt, 2, id);
    sqlite3_bind_double (stmt, 3, lat);
    sqlite3_bind_double (stmt, 4, lon);
    sqlite3_bind_int (stmt, 5, 0);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	return 1;
    fprintf (stderr, "sqlite3_step() error: %s\n", sqlite3_errmsg (handle));
    sqlite3_finalize (stmt);
    return 0;
}

static int
parse_nodes (FILE * xml, sqlite3 * handle)
{
/* parsing <node> tags from XML file */
    sqlite3_stmt *stmt;
    int ret;
    char *sql_err = NULL;
    sqlite3_int64 id;
    double lat;
    double lon;
    int opened;
    int count = 0;
    int node = 0;
    int c;
    int last_c;
    char value[8192];
    char *p;
    struct check_tag tag;
    init_tag (&tag, 6);

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
/* preparing the SQL statement */
    strcpy (value,
	    "INSERT INTO osm_tmp_nodes (id, alias, lat, lon, refcount) ");
    strcat (value, "VALUES (?, ?, ?, ?, ?)");
    ret = sqlite3_prepare_v2 (handle, value, strlen (value), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", value,
		   sqlite3_errmsg (handle));
	  return -1;
      }

    while ((c = getc (xml)) != EOF)
      {
	  if (node)
	    {
		/* we are inside a <node> tag */
		*p++ = c;
		if (c == '<')
		    opened++;
		if (!opened && c == '>' && last_c == '/')
		  {
		      /* closing a <node /> tag */
		      *p = '\0';
		      if (!parse_node_tag (value, &id, &lat, &lon))
			{
			    fprintf (stderr, "ERROR: invalid <node>\n");
			    return -1;
			}
		      else
			{
			    if (!insert_node (handle, stmt, id, lat, lon))
				return -1;
			    count++;
			}
		      node = 0;
		      continue;
		  }
		last_c = c;
	    }
	  update_tag (&tag, c);
	  if (strncmp (tag.buf, "<node ", 6) == 0)
	    {
		/* opening a <node> tag */
		node = 1;
		last_c = '\0';
		opened = 0;
		strcpy (value, "<node ");
		p = value + 6;
		continue;
	    }
	  if (strncmp (tag.buf, "</node", 6) == 0)
	    {
		/* closing a <node> tag */
		*p = '\0';
		if (!parse_node_tag (value, &id, &lat, &lon))
		  {
		      fprintf (stderr, "ERROR: invalid <node>\n");
		      return -1;
		  }
		else
		  {
		      if (!insert_node (handle, stmt, id, lat, lon))
			  return -1;
		      count++;
		  }
		node = 0;
		continue;
	    }
      }
/* finalizing the INSERT INTO prepared statement */
    sqlite3_finalize (stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
    return count;
}

static int
disambiguate_nodes (sqlite3 * handle)
{
/* disambiguating duplicate NODEs */
    int ret;
    sqlite3_stmt *stmt;
    sqlite3_int64 id;
    int refcount;
    double lon;
    double lat;
    char sql[8192];
    int count = 0;
    struct dup_node *first = NULL;
    struct dup_node *last = NULL;
    struct dup_node *p;
    struct dup_node *pn;
    struct dup_node_id *pid;
    struct dup_node_id *pidn;
    char *sql_err = NULL;
/* creating an index */
    strcpy (sql, "CREATE INDEX latlon ON osm_tmp_nodes (lat, lon)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX latlon: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
/* identifying the duplicate nodes by coords */
    strcpy (sql, "SELECT lat, lon, Sum(refcount) FROM osm_tmp_nodes ");
    strcat (sql, "WHERE refcount > 0 GROUP BY lat, lon ");
    strcat (sql, "HAVING count(*) > 1");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  return -1;
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
		lat = sqlite3_column_double (stmt, 0);
		lon = sqlite3_column_double (stmt, 1);
		refcount = sqlite3_column_int (stmt, 2);
		p = malloc (sizeof (struct dup_node));
		p->lat = lat;
		p->lon = lon;
		p->refcount = refcount;
		p->first = NULL;
		p->last = NULL;
		p->next = NULL;
		if (!first)
		    first = p;
		if (last)
		    last->next = p;
		last = p;
	    }
	  else
	    {
		/* some unexpected error occurred */
		fprintf (stderr, "sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		sqlite3_finalize (stmt);
		return -1;
	    }
      }
    sqlite3_finalize (stmt);
/* retrieving the dup-nodes IDs */
    p = first;
    while (p)
      {
	  strcpy (sql, "SELECT id FROM osm_tmp_nodes ");
	  strcat (sql, "WHERE lat = ? AND lon = ?");
	  ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (handle));
		return -1;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_double (stmt, 1, p->lat);
	  sqlite3_bind_double (stmt, 2, p->lon);
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
		      pid = malloc (sizeof (struct dup_node_id));
		      pid->id = id;
		      pid->next = NULL;
		      if (p->first == NULL)
			  p->first = pid;
		      if (p->last != NULL)
			  p->last->next = pid;
		      p->last = pid;
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (handle));
		      sqlite3_finalize (stmt);
		      return -1;
		  }
	    }
	  sqlite3_finalize (stmt);
	  p = p->next;
      }

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
/* preparing the SQL statement */
    strcpy (sql, "UPDATE osm_tmp_nodes SET alias = ?, refcount = ? ");
    strcat (sql, "WHERE id = ?");
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", sql, sqlite3_errmsg (handle));
	  return -1;
      }

    p = first;
    while (p)
      {
	  pid = p->first;
	  while (pid)
	    {
		if (pid == p->first)
		  {
		      /* fixing the master node */
		      sqlite3_reset (stmt);
		      sqlite3_clear_bindings (stmt);
		      sqlite3_bind_int64 (stmt, 1, pid->id);
		      sqlite3_bind_int (stmt, 2, p->refcount);
		      sqlite3_bind_int64 (stmt, 3, pid->id);
		      ret = sqlite3_step (stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  count++;
		      else
			{
			    fprintf (stderr, "sqlite3_step() error: %s\n",
				     sqlite3_errmsg (handle));
			    sqlite3_finalize (stmt);
			    return -1;
			}
		  }
		else
		  {
		      /* disambiguating the node */
		      sqlite3_reset (stmt);
		      sqlite3_clear_bindings (stmt);
		      sqlite3_bind_int64 (stmt, 1, p->first->id);
		      sqlite3_bind_int (stmt, 2, p->refcount);
		      sqlite3_bind_int64 (stmt, 3, pid->id);
		      ret = sqlite3_step (stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  count++;
		      else
			{
			    fprintf (stderr, "sqlite3_step() error: %s\n",
				     sqlite3_errmsg (handle));
			    sqlite3_finalize (stmt);
			    return -1;
			}
		  }
		pid = pid->next;
	    }
	  p = p->next;
      }
/* finalizing the INSERT INTO prepared statement */
    sqlite3_finalize (stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }

/* freeing the dup-nodes list */
    p = first;
    while (p)
      {
	  pn = p->next;
	  pid = p->first;
	  while (pid)
	    {
		pidn = pid->next;
		free (pid);
		pid = pidn;
	    }
	  free (p);
	  p = pn;
      }
    return count;
}

static int
parse_way_id (const char *buf, sqlite3_int64 * id)
{
/* parsing <way id="value" > */
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 4);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "id=\"", 4) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		*id = atol_64 (value);
		return 1;
	    }
      }
    return 0;
}

static int
parse_nd_id (const char *buf, sqlite3_int64 * id)
{
/* parsing <nd ref="value" > */
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 5);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "ref=\"", 5) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		*id = atol_64 (value);
		return 1;
	    }
      }
    return 0;
}

static int
parse_tag_k (const char *buf, char *k)
{
/* parsing <tag k="value" > */
    char value[128];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 3);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "k=\"", 3) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		strcpy (k, value);
		return 1;
	    }
      }
    return 0;
}

static void
from_xml_string (const char *xml, char *clean)
{
/* cleans XML markers */
    char marker[16];
    char *pm;
    int is_marker = 0;
    const char *in = xml;
    char *out = clean;
    while (*in != '\0')
      {
	  if (is_marker)
	    {
		if (*in == ';')
		  {
		      /* closing an XML marker */
		      in++;
		      is_marker = 0;
		      *pm = '\0';
		      if (strcmp (marker, "&amp") == 0)
			  *out++ = '&';
		      if (strcmp (marker, "&lt") == 0)
			  *out++ = '<';
		      if (strcmp (marker, "&gt") == 0)
			  *out++ = '>';
		      if (strcmp (marker, "&quot") == 0)
			  *out++ = '"';
		      if (strcmp (marker, "&apos") == 0)
			  *out++ = '\'';
		  }
		else
		    *pm++ = *in++;
		continue;
	    }
	  if (*in == '&')
	    {
		/* found an XML marker */
		in++;
		is_marker = 1;
		pm = marker;
		*pm++ = '&';
		continue;
	    }
	  *out++ = *in++;
      }
    *out = '\0';
}

static int
parse_tag_v (const char *buf, char *v)
{
/* parsing <tag v="value" > */
    char value[8192];
    char *out = value;
    const char *p = buf;
    struct check_tag tag;
    init_tag (&tag, 3);
    while (*p != '\0')
      {
	  update_tag (&tag, *p);
	  p++;
	  if (strncmp (tag.buf, "v=\"", 3) == 0)
	    {
		while (*p != '\"')
		  {
		      *out++ = *p++;
		  }
		*out = '\0';
		from_xml_string (value, v);
		return 1;
	    }
      }
    return 0;
}

static int
parse_tag_kv (const char *buf, char *k, char *v)
{
/* parsing a <tag> tag */
    if (!parse_tag_k (buf, k))
	return 0;
    if (!parse_tag_v (buf, v))
	return 0;
    return 1;
}

static int
parse_way_tag (const char *buf, struct way *xway)
{
/* parsing a <way> tag */
    int nd = 0;
    int tg = 0;
    const char *in;
    char value[65536];
    char k[8192];
    char v[8192];
    char *p;
    char c;
    int opened;
    int last_c;
    sqlite3_int64 id;
    struct check_tag tag;
    if (!parse_way_id (buf, &(xway->id)))
	return 0;
/* parsing the <nd> list */
    in = buf;
    init_tag (&tag, 4);
    while (*in != '\0')
      {
	  c = *in++;
	  if (nd)
	    {
		/* we are inside a <nd> tag */
		*p++ = c;
		if (c == '<')
		    opened++;
		if (!opened && c == '>' && last_c == '/')
		  {
		      /* closing a <nd /> tag */
		      *p = '\0';
		      if (!parse_nd_id (value, &id))
			{
			    fprintf (stderr, "ERROR: invalid <nd>\n");
			    return 0;
			}
		      else
			{
			    struct node_ref *node =
				malloc (sizeof (struct node_ref));
			    node->id = id;
			    node->ignore = 1;
			    node->next = NULL;
			    if (xway->first == NULL)
				xway->first = node;
			    if (xway->last != NULL)
				xway->last->next = node;
			    xway->last = node;
			}
		      nd = 0;
		      continue;
		  }
		last_c = c;
	    }
	  update_tag (&tag, c);
	  if (strncmp (tag.buf, "<nd ", 4) == 0)
	    {
		/* opening a <nd> tag */
		nd = 1;
		last_c = '\0';
		opened = 0;
		strcpy (value, "<nd ");
		p = value + 4;
		continue;
	    }
      }
/* parsing the <tag> list */
    in = buf;
    init_tag (&tag, 5);
    while (*in != '\0')
      {
	  c = *in++;
	  if (tg)
	    {
		/* we are inside a <tag> tag */
		*p++ = c;
		if (c == '<')
		    opened++;
		if (!opened && c == '>' && last_c == '/')
		  {
		      /* closing a <tag /> tag */
		      *p = '\0';
		      if (!parse_tag_kv (value, k, v))
			{
			    fprintf (stderr, "ERROR: invalid <tag>\n");
			    return 0;
			}
		      else
			{
			    struct kv *key_value = malloc (sizeof (struct kv));
			    key_value->k = malloc (strlen (k) + 1);
			    strcpy (key_value->k, k);
			    key_value->v = malloc (strlen (v) + 1);
			    strcpy (key_value->v, v);
			    key_value->next = NULL;
			    if (xway->first_kv == NULL)
				xway->first_kv = key_value;
			    if (xway->last_kv != NULL)
				xway->last_kv->next = key_value;
			    xway->last_kv = key_value;
			}
		      tg = 0;
		      continue;
		  }
		last_c = c;
	    }
	  update_tag (&tag, c);
	  if (strncmp (tag.buf, "<tag ", 5) == 0)
	    {
		/* opening a <tag> tag */
		tg = 1;
		last_c = '\0';
		opened = 0;
		strcpy (value, "<tag ");
		p = value + 5;
		continue;
	    }
      }
    return 1;
}

static void
clean_way (struct way *xway)
{
/* memory clean up */
    struct node_ref *p;
    struct node_ref *pn;
    struct kv *pkv;
    struct kv *pkvn;
    struct arc *pa;
    struct arc *pan;
    p = xway->first;
    while (p)
      {
	  pn = p->next;
	  free (p);
	  p = pn;
      }
    xway->first = NULL;
    xway->last = NULL;
    pkv = xway->first_kv;
    while (pkv)
      {
	  pkvn = pkv->next;
	  free (pkv->k);
	  free (pkv->v);
	  free (pkv);
	  pkv = pkvn;
      }
    xway->first_kv = NULL;
    xway->last_kv = NULL;
    pa = xway->first_arc;
    while (pa)
      {
	  pan = pa->next;
	  if (pa->dyn)
	      gaiaFreeDynamicLine (pa->dyn);
	  if (pa->geom)
	      gaiaFreeGeomColl (pa->geom);
	  free (pa);
	  pa = pan;
      }
    xway->first_arc = NULL;
    xway->last_arc = NULL;
    if (xway->class)
	free (xway->class);
    xway->class = NULL;
    if (xway->name)
	free (xway->name);
    xway->name = NULL;
}

static void
set_way_name (struct way *xway)
{
/* trying to fetch the road name */
    struct kv *pkv;
    pkv = xway->first_kv;
    while (pkv)
      {
	  if (strcmp (pkv->k, "name") == 0)
	    {
		if (xway->name)
		    free (xway->name);
		xway->name = malloc (strlen (pkv->v) + 1);
		strcpy (xway->name, pkv->v);
		return;
	    }
	  if (strcmp (pkv->k, "ref") == 0)
	    {
		if (xway->name == NULL)
		  {
		      xway->name = malloc (strlen (pkv->v) + 1);
		      strcpy (xway->name, pkv->v);
		      return;
		  }
	    }
	  pkv = pkv->next;
      }
}

static int
is_valid_way (const char *key, const char *class)
{
/* checks if this one is an valid road to be included into the network */
    if (strcmp (key, "highway") == 0)
      {
	  if (strcmp (class, "pedestrian") == 0)
	      return 0;
	  if (strcmp (class, "track") == 0)
	      return 0;
	  if (strcmp (class, "services") == 0)
	      return 0;
	  if (strcmp (class, "bus_guideway") == 0)
	      return 0;
	  if (strcmp (class, "path") == 0)
	      return 0;
	  if (strcmp (class, "cycleway") == 0)
	      return 0;
	  if (strcmp (class, "footway") == 0)
	      return 0;
	  if (strcmp (class, "bridleway") == 0)
	      return 0;
	  if (strcmp (class, "byway") == 0)
	      return 0;
	  if (strcmp (class, "steps") == 0)
	      return 0;
	  return 1;
      }
    return 0;
}

static void
set_oneway (struct way *xway)
{
/* setting up the oneway markers */
    struct kv *pkv;
    xway->oneway = 0;
    xway->reverse = 0;
    pkv = xway->first_kv;
    while (pkv)
      {
	  if (strcmp (pkv->k, "oneway") == 0)
	    {
		if (strcmp (pkv->v, "yes") == 0 || strcmp (pkv->v, "yes") == 0
		    || strcmp (pkv->v, "1") == 0)
		  {
		      xway->oneway = 1;
		      xway->reverse = 0;
		  }
		if (strcmp (pkv->v, "-1") == 0)
		  {
		      xway->oneway = 1;
		      xway->reverse = 1;
		  }
	    }
	  pkv = pkv->next;
      }
}

static int
check_way (struct way *xway)
{
/* checks if <way> is interesting to build the road network */
    struct kv *pkv;
    pkv = xway->first_kv;
    while (pkv)
      {
	  if (is_valid_way (pkv->k, pkv->v))
	    {
		xway->class = malloc (strlen (pkv->v) + 1);
		strcpy (xway->class, pkv->v);
		set_way_name (xway);
		set_oneway (xway);
		return 1;
	    }
	  pkv = pkv->next;
      }
    return 0;
}

static sqlite3_int64
find_node (struct way *xway, double lon, double lat)
{
/* finding a node by coords */
    struct node_ref *p = xway->first;
    while (p)
      {
	  if (p->ignore == 0)
	    {
		if (p->lon == lon && p->lat == lat)
		    return p->alias;
	    }
	  p = p->next;
      }
    return 0;
}

static double
compute_time (const char *class, double length)
{
/* computing traval time */
    double speed = 30.0;	/* speed, in Km/h */
    double msec;
    if (strcmp (class, "motorway") == 0 || strcmp (class, "trunk") == 0)
	speed = 110;
    if (strcmp (class, "primary") == 0)
	speed = 90;
    if (strcmp (class, "secundary") == 0)
	speed = 70;
    if (strcmp (class, "tertiary") == 0)
	speed = 50;
    msec = speed * 1000.0 / 3600.0;	/* transforming speed in m/sec */
    return length / msec;
}

static int
build_geometry (sqlite3 * handle, struct way *xway)
{
/* building geometry representing an ARC */
    int ret;
    sqlite3_stmt *stmt;
    sqlite3_int64 id;
    sqlite3_int64 alias;
    int refcount;
    double lon;
    double lat;
    char sql[8192];
    int ind;
    int points = 0;
    int tbd;
    int count;
    int base = 0;
    int block = 128;
    int how_many;
    struct node_ref *p;
    struct node_ref *prev;
    struct arc *pa;
    gaiaLinestringPtr line;
    double a;
    double b;
    double rf;
    p = xway->first;
    while (p)
      {
	  points++;
	  p = p->next;
      }
    if (points < 2)
      {
	  /* discarding stupid degenerated lines - oh yes, there are lots of them !!! */
	  return -1;
      }
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
	  strcpy (sql,
		  "SELECT id, alias, lat, lon, refcount FROM osm_tmp_nodes ");
	  strcat (sql, "WHERE id IN (");
	  for (ind = 0; ind < how_many; ind++)
	    {
		if (ind == 0)
		    strcat (sql, "?");
		else
		    strcat (sql, ",?");
	    }
	  strcat (sql, ")");
	  ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (handle));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  ind = 1;
	  count = 0;
	  p = xway->first;
	  while (p)
	    {
		if (count < base)
		  {
		      count++;
		      p = p->next;
		      continue;
		  }
		if (count >= (base + how_many))
		    break;
		sqlite3_bind_int64 (stmt, ind, p->id);
		ind++;
		count++;
		p = p->next;
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
		      alias = sqlite3_column_int64 (stmt, 1);
		      lat = sqlite3_column_double (stmt, 2);
		      lon = sqlite3_column_double (stmt, 3);
		      refcount = sqlite3_column_int (stmt, 4);
		      p = xway->first;
		      while (p)
			{
			    if (p->id == id)
			      {
				  p->lat = lat;
				  p->lon = lon;
				  p->alias = alias;
				  p->refcount = refcount;
				  p->ignore = 0;
			      }
			    p = p->next;
			}
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (handle));
		      sqlite3_finalize (stmt);
		      return 0;
		  }
	    }
	  sqlite3_finalize (stmt);
	  tbd -= how_many;
	  base += how_many;
      }
    prev = NULL;
    p = xway->first;
    while (p)
      {
	  /* marking repeated points as invalid */
	  if (p->ignore)
	    {
		p = p->next;
		continue;
	    }
	  if (prev)
	    {
		if (p->lon == prev->lon && p->lat == prev->lat)
		    p->ignore = 1;
	    }
	  prev = p;
	  p = p->next;
      }
    points = 0;
    p = xway->first;
    while (p)
      {
	  if (p->ignore == 0)
	      points++;
	  p = p->next;
      }
    if (points < 2)
      {
	  /* discarding stupid degenerated lines - oh yes, there are lots of them !!! */
	  return -1;
      }
/* creating a new ARC */
    pa = malloc (sizeof (struct arc));
    pa->geom = NULL;
    pa->next = NULL;
    xway->first_arc = pa;
    xway->last_arc = pa;
    pa->dyn = gaiaAllocDynamicLine ();
    p = xway->first;
    while (p)
      {
	  /* setting up dynamic lines */
	  if (p->ignore)
	    {
		/* skipping invalid points */
		p = p->next;
		continue;
	    }
	  if (pa->dyn->First == NULL)
	      pa->from = p->alias;
	  gaiaAppendPointToDynamicLine (pa->dyn, p->lon, p->lat);
	  pa->to = p->alias;
	  if (p != xway->first && p != xway->last)
	    {
		if (p->refcount > 1)
		  {
		      /* found a graph NODE: creating a new ARC */
		      pa = malloc (sizeof (struct arc));
		      pa->geom = NULL;
		      pa->next = NULL;
		      xway->last_arc->next = pa;
		      xway->last_arc = pa;
		      pa->from = p->alias;
		      pa->to = p->alias;
		      pa->dyn = gaiaAllocDynamicLine ();
		      gaiaAppendPointToDynamicLine (pa->dyn, p->lon, p->lat);
		  }
	    }
	  p = p->next;
      }
    pa = xway->first_arc;
    while (pa)
      {
	  /* splitting self-closed arcs [rings] */
	  if (pa->dyn->First == NULL)
	    {
		/* skipping empty lines */
		pa = pa->next;
		continue;
	    }
	  if (pa->dyn->First == pa->dyn->Last)
	    {
		/* skipping one-point-only lines */
		pa = pa->next;
		continue;
	    }
	  if (pa->dyn->First->X == pa->dyn->Last->X
	      && pa->dyn->First->Y == pa->dyn->Last->Y)
	    {
		/* found a self-closure */
		gaiaDynamicLinePtr saved = pa->dyn;
		struct arc *pbis;
		gaiaPointPtr pt;
		int limit;
		sqlite3_int64 node_id;
		points = 0;
		pt = saved->First;
		while (pt)
		  {
		      /* counting how many points are there */
		      points++;
		      pt = pt->Next;
		  }
		limit = points / 2;
		/* appending a new arc */
		pbis = malloc (sizeof (struct arc));
		pbis->geom = NULL;
		pbis->next = NULL;
		xway->last_arc->next = pbis;
		xway->last_arc = pbis;
		pbis->to = pa->to;
		pbis->dyn = gaiaAllocDynamicLine ();
		pa->dyn = gaiaAllocDynamicLine ();
		ind = 0;
		pt = saved->First;
		while (pt)
		  {
		      /* appending points */
		      if (ind < limit)
			  gaiaAppendPointToDynamicLine (pa->dyn, pt->X, pt->Y);
		      else if (ind == limit)
			{
			    gaiaAppendPointToDynamicLine (pa->dyn, pt->X,
							  pt->Y);
			    gaiaAppendPointToDynamicLine (pbis->dyn, pt->X,
							  pt->Y);
			    node_id = find_node (xway, pt->X, pt->Y);
			}
		      else
			  gaiaAppendPointToDynamicLine (pbis->dyn, pt->X,
							pt->Y);
		      ind++;
		      pt = pt->Next;
		  }
		/* adjusting the node */
		pa->to = node_id;
		pbis->from = node_id;
		gaiaFreeDynamicLine (saved);
	    }
	  pa = pa->next;
      }
    gaiaEllipseParams ("WGS84", &a, &b, &rf);
    pa = xway->first_arc;
    while (pa)
      {
	  /* setting up geometries */
	  gaiaPointPtr pt;
	  if (pa->dyn->First == NULL)
	    {
		/* skipping empty lines */
		pa = pa->next;
		continue;
	    }
	  if (pa->dyn->First == pa->dyn->Last)
	    {
		/* skipping one-point-only lines */
		pa = pa->next;
		continue;
	    }
	  points = 0;
	  pt = pa->dyn->First;
	  while (pt)
	    {
		/* counting how many points are there */
		points++;
		pt = pt->Next;
	    }
	  pa->geom = gaiaAllocGeomColl ();
	  pa->geom->Srid = 4326;
	  line = gaiaAddLinestringToGeomColl (pa->geom, points);
	  ind = 0;
	  pt = pa->dyn->First;
	  while (pt)
	    {
		/* setting up linestring coords */
		gaiaSetPoint (line->Coords, ind, pt->X, pt->Y);
		ind++;
		pt = pt->Next;
	    }
	  gaiaFreeDynamicLine (pa->dyn);
	  pa->dyn = NULL;
	  pa->length =
	      gaiaGreatCircleTotalLength (a, b,
					  pa->geom->
					  FirstLinestring->DimensionModel,
					  pa->geom->FirstLinestring->Coords,
					  pa->geom->FirstLinestring->Points);
	  pa->cost = compute_time (xway->class, pa->length);
	  pa = pa->next;
      }
    return 1;
}

static int
insert_arc_bidir (sqlite3 * handle, sqlite3_stmt * stmt, struct way *xway,
		  struct arc *xarc)
{
/* inserting a bidirectional ARC [OSM road] into the DB */
    int ret;
    unsigned char *blob;
    int blob_size;
    const char *unknown = "unknown";
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, xway->id);
    sqlite3_bind_text (stmt, 2, xway->class, strlen (xway->class),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 3, xarc->from);
    sqlite3_bind_int64 (stmt, 4, xarc->to);
    if (xway->name)
	sqlite3_bind_text (stmt, 5, xway->name, strlen (xway->name),
			   SQLITE_STATIC);
    else
	sqlite3_bind_text (stmt, 5, unknown, strlen (unknown), SQLITE_STATIC);
    if (xway->oneway)
      {
	  /* oneway arc */
	  if (xway->reverse)
	    {
		/* reverse */
		sqlite3_bind_int (stmt, 6, 0);
		sqlite3_bind_int (stmt, 7, 1);
	    }
	  else
	    {
		/* conformant */
		sqlite3_bind_int (stmt, 6, 1);
		sqlite3_bind_int (stmt, 7, 0);
	    }
      }
    else
      {
	  /* bidirectional arc */
	  sqlite3_bind_int (stmt, 6, 1);
	  sqlite3_bind_int (stmt, 7, 1);
      }
    sqlite3_bind_double (stmt, 8, xarc->length);
    sqlite3_bind_double (stmt, 9, xarc->cost);
    gaiaToSpatiaLiteBlobWkb (xarc->geom, &blob, &blob_size);
    sqlite3_bind_blob (stmt, 10, blob, blob_size, free);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	return 1;
    fprintf (stderr, "sqlite3_step() error: %s\n", sqlite3_errmsg (handle));
    sqlite3_finalize (stmt);
    return 0;
}

static int
insert_arc_unidir (sqlite3 * handle, sqlite3_stmt * stmt, struct way *xway,
		   struct arc *xarc)
{
/* inserting an unidirectional ARC [OSM road] into the DB */
    int ret;
    unsigned char *blob;
    int blob_size;
    const char *unknown = "unknown";
    int straight = 0;
    int reverse = 0;
    int count = 0;
    if (xway->oneway)
      {
	  /* oneway arc */
	  if (xway->reverse)
	    {
		/* reverse */
		reverse = 1;
	    }
	  else
	    {
		/* conformant */
		straight = 1;
	    }
      }
    else
      {
	  /* bidirectional arc */
	  straight = 1;
	  reverse = 1;
      }
    if (straight)
      {
	  /* inserting to FromTo arc */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_int64 (stmt, 1, xway->id);
	  sqlite3_bind_text (stmt, 2, xway->class, strlen (xway->class),
			     SQLITE_STATIC);
	  sqlite3_bind_int64 (stmt, 3, xarc->from);
	  sqlite3_bind_int64 (stmt, 4, xarc->to);
	  if (xway->name)
	      sqlite3_bind_text (stmt, 5, xway->name, strlen (xway->name),
				 SQLITE_STATIC);
	  else
	      sqlite3_bind_text (stmt, 5, unknown, strlen (unknown),
				 SQLITE_STATIC);
	  sqlite3_bind_double (stmt, 6, xarc->length);
	  sqlite3_bind_double (stmt, 7, xarc->cost);
	  gaiaToSpatiaLiteBlobWkb (xarc->geom, &blob, &blob_size);
	  sqlite3_bind_blob (stmt, 8, blob, blob_size, free);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      count++;
	  else
	    {
		fprintf (stderr, "sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		sqlite3_finalize (stmt);
		return 0;
	    }
      }
    if (reverse)
      {
	  /* inserting to ToFrom arc */
	  gaiaLinestringPtr ln1;
	  gaiaLinestringPtr ln2;
	  int iv;
	  int iv2;
	  double x;
	  double y;
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_int64 (stmt, 1, xway->id);
	  sqlite3_bind_text (stmt, 2, xway->class, strlen (xway->class),
			     SQLITE_STATIC);
	  sqlite3_bind_int64 (stmt, 3, xarc->to);
	  sqlite3_bind_int64 (stmt, 4, xarc->from);
	  if (xway->name)
	      sqlite3_bind_text (stmt, 5, xway->name, strlen (xway->name),
				 SQLITE_STATIC);
	  else
	      sqlite3_bind_text (stmt, 5, unknown, strlen (unknown),
				 SQLITE_STATIC);
	  /* reversing the linestring */
	  ln1 = xarc->geom->FirstLinestring;
	  ln2 = gaiaAllocLinestring (ln1->Points);
	  xarc->geom->FirstLinestring = ln2;
	  iv2 = 0;
	  for (iv = ln1->Points - 1; iv >= 0; iv--)
	    {
		gaiaGetPoint (ln1->Coords, iv, &x, &y);
		gaiaSetPoint (ln2->Coords, iv2, x, y);
		iv2++;
	    }
	  gaiaFreeLinestring (ln1);
	  sqlite3_bind_double (stmt, 6, xarc->length);
	  sqlite3_bind_double (stmt, 7, xarc->cost);
	  gaiaToSpatiaLiteBlobWkb (xarc->geom, &blob, &blob_size);
	  sqlite3_bind_blob (stmt, 8, blob, blob_size, free);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      count++;
	  else
	    {
		fprintf (stderr, "sqlite3_step() error: %s\n",
			 sqlite3_errmsg (handle));
		sqlite3_finalize (stmt);
		return 0;
	    }
      }
    return count;
}

static int
parse_ways_pass_2 (FILE * xml, sqlite3 * handle, const char *table,
		   int double_arcs, char *value)
{
/* parsing <way> tags from XML file - Step II */
    sqlite3_stmt *stmt;
    int ret;
    char *sql_err = NULL;
    struct way xway;
    int way = 0;
    int count = 0;
    int c;
    char *p;
    struct check_tag tag;
    struct arc *xarc;
    init_tag (&tag, 5);
    xway.first = NULL;
    xway.last = NULL;
    xway.first_kv = NULL;
    xway.last_kv = NULL;
    xway.first_arc = NULL;
    xway.last_arc = NULL;
    xway.class = NULL;
    xway.name = NULL;

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }

/* preparing the SQL statement */
    if (double_arcs)
      {
	  /* unidirectional arcs */
	  sprintf (value, "INSERT OR IGNORE INTO \"%s\" ", table);
	  strcat (value, "(id, osm_id, class, node_from, node_to, name, ");
	  strcat (value, "length, cost, geometry) VALUES ");
	  strcat (value, "(NULL, ?, ?, ?, ?, ?, ?, ?, ?)");
      }
    else
      {
	  /* bidirectional arcs */
	  sprintf (value, "INSERT OR IGNORE INTO \"%s\" ", table);
	  strcat (value, "(id, osm_id, class, node_from, node_to, name, ");
	  strcat (value,
		  "oneway_fromto, oneway_tofrom, length, cost, geometry) ");
	  strcat (value, "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      }
    ret = sqlite3_prepare_v2 (handle, value, strlen (value), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", value,
		   sqlite3_errmsg (handle));
	  return -1;
      }

    while ((c = getc (xml)) != EOF)
      {
	  if (way)
	    {
		/* we are inside a <way> tag */
		*p++ = c;
	    }
	  update_tag (&tag, c);
	  if (strncmp (tag.buf, "<way ", 5) == 0)
	    {
		/* opening a <way> tag */
		way = 1;
		strcpy (value, "<way ");
		p = value + 5;
		continue;
	    }
	  if (strncmp (tag.buf, "</way", 5) == 0)
	    {
		/* closing a <way> tag */
		*p = '\0';
		if (!parse_way_tag (value, &xway))
		  {
		      clean_way (&xway);
		      fprintf (stderr, "ERROR: invalid <way>\n");
		      sqlite3_finalize (stmt);
		      return -1;
		  }
		else
		  {
		      if (check_way (&xway))
			{
			    ret = build_geometry (handle, &xway);
			    if (ret == 0)
			      {
				  fprintf (stderr,
					   "ERROR: invalid geometry <way>\n");
				  clean_way (&xway);
				  sqlite3_finalize (stmt);
				  return -1;
			      }
			    else if (ret < 0)
				;
			    else
			      {
				  xarc = xway.first_arc;
				  while (xarc)
				    {
					if (xarc->geom == NULL)
					  {
					      /* skipping empty geometries */
					      xarc = xarc->next;
					      continue;
					  }
					if (double_arcs)
					    ret =
						insert_arc_unidir (handle, stmt,
								   &xway, xarc);
					else
					    ret =
						insert_arc_bidir (handle, stmt,
								  &xway, xarc);
					if (!ret)
					  {
					      clean_way (&xway);
					      sqlite3_finalize (stmt);
					      return -1;
					  }
					count += ret;
					xarc = xarc->next;
				    }
			      }
			}
		      clean_way (&xway);
		  }
		way = 0;
		continue;
	    }
      }
/* finalizing the INSERT INTO prepared statement */
    sqlite3_finalize (stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
    return count;
}

static int
pre_check_way (struct way *xway)
{
/* checks if <way> is interesting to build the road network */
    struct kv *pkv;
    pkv = xway->first_kv;
    while (pkv)
      {
	  if (is_valid_way (pkv->k, pkv->v))
	      return 1;
	  pkv = pkv->next;
      }
    return 0;
}

static int
update_node (sqlite3 * handle, sqlite3_stmt * stmt, sqlite3_int64 id)
{
/* updating the node reference count */
    int ret;
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	return 1;
    fprintf (stderr, "sqlite3_step() error: %s\n", sqlite3_errmsg (handle));
    sqlite3_finalize (stmt);
    return 0;
}

static int
mark_nodes (sqlite3 * handle, sqlite3_stmt * stmt_upd, struct way *xway)
{
/* examining the node-reference list representing an ARC */
    int ret;
    sqlite3_stmt *stmt;
    sqlite3_int64 id;
    double lon;
    double lat;
    char sql[8192];
    int ind;
    int points = 0;
    int tbd;
    int count;
    int base = 0;
    int block = 128;
    int how_many;
    struct node_ref *p;
    p = xway->first;
    while (p)
      {
	  points++;
	  p = p->next;
      }
    tbd = points;
    while (tbd > 0)
      {
	  /* requesting max 128 points at each time */
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
	  ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "SQL error: %s\n%s\n", sql,
			 sqlite3_errmsg (handle));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  ind = 1;
	  count = 0;
	  p = xway->first;
	  while (p)
	    {
		if (count < base)
		  {
		      count++;
		      p = p->next;
		      continue;
		  }
		if (count >= (base + how_many))
		    break;
		sqlite3_bind_int64 (stmt, ind, p->id);
		ind++;
		count++;
		p = p->next;
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
		      id = sqlite3_column_int (stmt, 0);
		      lat = sqlite3_column_double (stmt, 1);
		      lon = sqlite3_column_double (stmt, 2);
		      p = xway->first;
		      while (p)
			{
			    if (p->id == id)
			      {
				  p->lat = lat;
				  p->lon = lon;
				  p->ignore = 0;
			      }
			    p = p->next;
			}
		  }
		else
		  {
		      /* some unexpected error occurred */
		      fprintf (stderr, "sqlite3_step() error: %s\n",
			       sqlite3_errmsg (handle));
		      sqlite3_finalize (stmt);
		      return 0;
		  }
	    }
	  sqlite3_finalize (stmt);
	  tbd -= how_many;
	  base += how_many;
      }
    p = xway->first;
    while (p)
      {
	  /* updating nodes ref-count */
	  if (p->ignore == 0)
	      update_node (handle, stmt_upd, p->id);
	  p = p->next;
      }
    return 1;
}

static int
parse_ways_pass_1 (FILE * xml, sqlite3 * handle, char *value)
{
/* parsing <way> tags from XML file - Pass I */
    sqlite3_stmt *stmt;
    int ret;
    char *sql_err = NULL;
    struct way xway;
    int way = 0;
    int count = 0;
    int c;
    char *p;
    struct check_tag tag;
    init_tag (&tag, 5);
    xway.first = NULL;
    xway.last = NULL;
    xway.first_kv = NULL;
    xway.last_kv = NULL;
    xway.first_arc = NULL;
    xway.last_arc = NULL;
    xway.class = NULL;
    xway.name = NULL;

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (handle, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
/* preparing the SQL statement */
    strcpy (value, "UPDATE osm_tmp_nodes SET refcount = refcount + 1 ");
    strcat (value, "WHERE id = ?");
    ret = sqlite3_prepare_v2 (handle, value, strlen (value), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SQL error: %s\n%s\n", value,
		   sqlite3_errmsg (handle));
	  return -1;
      }

    while ((c = getc (xml)) != EOF)
      {
	  if (way)
	    {
		/* we are inside a <way> tag */
		*p++ = c;
	    }
	  update_tag (&tag, c);
	  if (strncmp (tag.buf, "<way ", 5) == 0)
	    {
		/* opening a <way> tag */
		way = 1;
		strcpy (value, "<way ");
		p = value + 5;
		continue;
	    }
	  if (strncmp (tag.buf, "</way", 5) == 0)
	    {
		/* closing a <way> tag */
		*p = '\0';
		if (!parse_way_tag (value, &xway))
		  {
		      clean_way (&xway);
		      fprintf (stderr, "ERROR: invalid <way>\n");
		      return -1;
		  }
		else
		  {
		      if (pre_check_way (&xway))
			{
			    if (!mark_nodes (handle, stmt, &xway))
			      {
				  fprintf (stderr,
					   "ERROR: invalid node-reference list: <way>\n");
				  clean_way (&xway);
				  return -1;
			      }
			    count++;
			}
		      clean_way (&xway);
		  }
		way = 0;
		continue;
	    }
      }
/* finalizing the INSERT INTO prepared statement */
    sqlite3_finalize (stmt);

/* committing the still pending SQL Transaction */
    ret = sqlite3_exec (handle, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return -1;
      }
    return count;
}

static void
db_cleanup (sqlite3 * handle)
{
    int ret;
    char *sql_err = NULL;
/* dropping the OSM_TMP_NODES table */
    printf ("\nDropping temporary table 'osm_tmp_nodes' ... wait please ...\n");
    ret =
	sqlite3_exec (handle, "DROP TABLE osm_tmp_nodes", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "'DROP TABLE osm_tmp_nodes' error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    printf ("\tDropped table 'osm_tmp_nodes'\n");
/* dropping the 'from_to' index */
    printf ("\nDropping index 'from_to' ... wait please ...\n");
    ret = sqlite3_exec (handle, "DROP INDEX from_to", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "'DROP INDEX from_to' error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    printf ("\tDropped index 'from_to'\n");
}

static void
db_vacuum (sqlite3 * handle)
{
    int ret;
    char *sql_err = NULL;
/* VACUUMing the DB */
    printf ("\nVACUUMing the DB ... wait please ...\n");
    ret = sqlite3_exec (handle, "VACUUM", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "VACUUM error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return;
      }
    printf ("\tAll done: OSM graph was succesfully loaded\n");
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

static sqlite3 *
open_db (const char *path, const char *table, int double_arcs, int cache_size)
{
/* opening the DB */
    sqlite3 *handle;
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

    spatialite_init (0);
    printf ("SQLite version: %s\n", sqlite3_libversion ());
    printf ("SpatiaLite version: %s\n\n", spatialite_version ());

    ret =
	sqlite3_open_v2 (path, &handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open '%s': %s\n", path,
		   sqlite3_errmsg (handle));
	  sqlite3_close (handle);
	  return NULL;
      }
    spatialite_autocreate (handle);
    if (cache_size > 0)
      {
	  /* setting the CACHE-SIZE */
	  sprintf (sql, "PRAGMA cache_size=%d", cache_size);
	  sqlite3_exec (handle, sql, NULL, NULL, NULL);
      }

/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "PRAGMA table_info(geometry_columns)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
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
    if (f_table_name && f_geometry_column && type && coord_dimension && gc_srid
	&& spatial_index_enabled)
	spatialite_gc = 1;

/* checking the SPATIAL_REF_SYS table */
    strcpy (sql, "PRAGMA table_info(spatial_ref_sys)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
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

/* creating the OSM related tables */
    strcpy (sql, "CREATE TABLE osm_tmp_nodes (\n");
    strcat (sql, "id INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "alias INTEGER NOT NULL,\n");
    strcat (sql, "lat DOUBLE NOT NULL,\n");
    strcat (sql, "lon DOUBLE NOT NULL,\n");
    strcat (sql, "refcount INTEGER NOT NULL)\n");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE 'osm_tmp_nodes' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return NULL;
      }

    if (double_arcs)
      {
	  /* unidirectional arcs */
	  sprintf (sql, "CREATE TABLE \"%s\" (\n", table);
	  strcat (sql, "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n");
	  strcat (sql, "osm_id INTEGER NOT NULL,\n");
	  strcat (sql, "class TEXT NOT NULL,\n");
	  strcat (sql, "node_from INTEGER NOT NULL,\n");
	  strcat (sql, "node_to INTEGER NOT NULL,\n");
	  strcat (sql, "name TEXT NOT NULL,\n");
	  strcat (sql, "length DOUBLE NOT NULL,\n"),
	      strcat (sql, "cost DOUBLE NOT NULL)\n");
      }
    else
      {
	  /* bidirectional arcs */
	  sprintf (sql, "CREATE TABLE \"%s\" (\n", table);
	  strcat (sql, "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n");
	  strcat (sql, "osm_id INTEGER NOT NULL,\n");
	  strcat (sql, "class TEXT NOT NULL,\n");
	  strcat (sql, "node_from INTEGER NOT NULL,\n");
	  strcat (sql, "node_to INTEGER NOT NULL,\n");
	  strcat (sql, "name TEXT NOT NULL,\n");
	  strcat (sql, "oneway_fromto INTEGER NOT NULL,\n");
	  strcat (sql, "oneway_tofrom INTEGER NOT NULL,\n");
	  strcat (sql, "length DOUBLE NOT NULL,\n"),
	      strcat (sql, "cost DOUBLE NOT NULL)\n");
      }
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE TABLE '%s' error: %s\n", table, err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return NULL;
      }

    sprintf (sql, "SELECT AddGeometryColumn('%s', 'geometry', ", table);
    strcat (sql, " 4326, 'LINESTRING', 'XY')");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "AddGeometryColumn() error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return NULL;
      }

    sprintf (sql,
	     "CREATE UNIQUE INDEX from_to ON \"%s\" (node_from, node_to, length, cost)",
	     table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CREATE INDEX 'from_to' error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return NULL;
      }

    return handle;

  unknown:
    if (handle)
	sqlite3_close (handle);
    fprintf (stderr, "DB '%s'\n", path);
    fprintf (stderr, "doesn't seems to contain valid Spatial Metadata ...\n\n");
    fprintf (stderr, "Please, run the 'spatialite-init' SQL script \n");
    fprintf (stderr, "in order to initialize Spatial Metadata\n\n");
    return NULL;
}

static int
check_osm_xml (FILE * xml)
{
/* check if the file contains OSM XML */
    int xml_open = 0;
    int xml_close = 0;
    int osm_open = 0;
    int osm_close = 0;
    int ok_xml = 0;
    int ok_osm = 0;
    char tag[512];
    char *p;
    int c;
    int count = 0;
    while ((c = getc (xml)) != EOF)
      {
	  count++;
	  if (count > 512)
	      break;
	  if (!xml_open && c == '<')
	    {
		xml_open = 1;
		p = tag;
		*p = '\0';
		continue;
	    }
	  if (!osm_open && c == '<')
	    {
		osm_open = 1;
		p = tag;
		*p = '\0';
		continue;
	    }
	  if (!xml_close && c == '>')
	    {
		xml_close = 1;
		*p = '\0';
		if (strncmp (tag, "?xml", 4) == 0)
		    ok_xml = 1;
		continue;
	    }
	  if (!osm_close && c == '>')
	    {
		osm_close = 1;
		*p = '\0';
		if (strncmp (tag, "osm", 3) == 0)
		    ok_osm = 1;
		continue;
	    }
	  if (xml_open && !xml_close)
	      *p++ = c;
	  if (osm_open && !osm_close)
	      *p++ = c;
      }
    if (ok_xml && ok_osm)
	return 1;
    return 0;
}

static void
do_help ()
{
/* printing the argument list */
    fprintf (stderr, "\n\nusage: spatialite_osm_net ARGLIST\n");
    fprintf (stderr,
	     "==============================================================\n");
    fprintf (stderr,
	     "-h or --help                    print this help message\n");
    fprintf (stderr, "-o or --osm-path pathname       the OSM-XML file path\n");
    fprintf (stderr,
	     "-d or --db-path  pathname       the SpatiaLite DB path\n");
    fprintf (stderr,
	     "-T or --table    table_name     the db table to be feeded\n\n");
    fprintf (stderr, "you can specify the following options as well\n");
    fprintf (stderr,
	     "-cs or --cache-size    num      DB cache size (how many pages)\n");
    fprintf (stderr,
	     "-m or --in-memory               using IN-MEMORY database\n");
    fprintf (stderr, "-2 or --undirectional           double arcs\n\n");
}

int
main (int argc, char *argv[])
{
/* the MAIN function simply perform arguments checking */
    int i;
    int next_arg = ARG_NONE;
    const char *osm_path = NULL;
    const char *db_path = NULL;
    const char *table = NULL;
    int in_memory = 0;
    int cache_size = 0;
    int double_arcs = 0;
    int error = 0;
    sqlite3 *handle;
    FILE *xml;
    int nodes;
    int ways;
    char *big_buffer;
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
		  case ARG_TABLE:
		      table = argv[i];
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
	  if (strcmp (argv[i], "-T") == 0)
	    {
		next_arg = ARG_TABLE;
		continue;
	    }
	  if (strcasecmp (argv[i], "--table") == 0)
	    {
		next_arg = ARG_TABLE;
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
	  if (strcasecmp (argv[i], "-2") == 0)
	    {
		double_arcs = 1;
		next_arg = ARG_NONE;
		continue;
	    }
	  if (strcasecmp (argv[i], "-unidirectional") == 0)
	    {
		double_arcs = 1;
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
    if (!table)
      {
	  fprintf (stderr, "did you forget setting the --table argument ?\n");
	  error = 1;
      }
    if (error)
      {
	  do_help ();
	  return -1;
      }
/* opening the OSM-XML file */
    xml = fopen (osm_path, "rb");
    if (!xml)
      {
	  fprintf (stderr, "cannot open %s\n", osm_path);
	  return -1;
      }
/* checking if really is OSM XML */
    if (!check_osm_xml (xml))
      {
	  fprintf (stderr, "'%s' doesn't seems to contain OSM XML !!!\n",
		   osm_path);
	  return -1;
      }
/* repositioning XML from beginning */
    rewind (xml);
/* opening the DB */
    if (in_memory)
	cache_size = 0;
    handle = open_db (db_path, table, double_arcs, cache_size);
    if (!handle)
	return -1;
    if (in_memory)
      {
	  /* loading the DB in-memory */
	  sqlite3 *mem_handle;
	  sqlite3_backup *backup;
	  int ret;
	  ret =
	      sqlite3_open_v2 (":memory:", &mem_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open 'MEMORY-DB': %s\n",
			 sqlite3_errmsg (mem_handle));
		sqlite3_close (mem_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (mem_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "cannot load 'MEMORY-DB'\n");
		sqlite3_close (handle);
		sqlite3_close (mem_handle);
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
	  handle = mem_handle;
	  printf ("\nusing IN-MEMORY database\n");
      }
/* extracting <node> tags form XML file */
    printf ("\nLoading OSM nodes ... wait please ...\n");
    nodes = parse_nodes (xml, handle);
    if (nodes < 0)
      {
	  fclose (xml);
	  sqlite3_close (handle);
	  fprintf (stderr, "Sorry, I'm quitting ... UNRECOVERABLE ERROR\n");
	  return 1;
      }
    printf ("\tLoaded %d OSM nodes\n", nodes);
/* repositioning XML from beginning */
    rewind (xml);
/* extracting <way> tags form XML file - Pass I */
    printf ("\nVerifying OSM ways ... wait please ...\n");
    big_buffer = malloc (4 * 1024 * 1024);
    ways = parse_ways_pass_1 (xml, handle, big_buffer);
    free (big_buffer);
    if (ways < 0)
      {
	  fclose (xml);
	  sqlite3_close (handle);
	  fprintf (stderr, "Sorry, I'm quitting ... UNRECOVERABLE ERROR\n");
	  return 1;
      }
    printf ("\tVerified %d OSM ways\n", ways);
/* disambiguating nodes - oh yes, there lots of stupid duplicates using different IDs !!! */
    printf ("\nDisambiguating OSM nodes ... wait please ...\n");
    nodes = disambiguate_nodes (handle);
    if (nodes < 0)
      {
	  fclose (xml);
	  sqlite3_close (handle);
	  fprintf (stderr, "Sorry, I'm quitting ... UNRECOVERABLE ERROR\n");
	  return 1;
      }
    if (nodes == 0)
	printf ("\tNo duplicate OSM nodes found - fine ...\n");
    else
	printf ("\tFound %d duplicate OSM nodes - fixed !!!\n", nodes);
/* repositioning XML from beginning */
    rewind (xml);
/* extracting <way> tags form XML file - Pass II */
    printf ("\nLoading network ARCs ... wait please ...\n");
    big_buffer = malloc (4 * 1024 * 1024);
    ways = parse_ways_pass_2 (xml, handle, table, double_arcs, big_buffer);

    if (ways < 0)
      {
	  fclose (xml);
	  sqlite3_close (handle);
	  fprintf (stderr, "Sorry, I'm quitting ... UNRECOVERABLE ERROR\n");
	  return 1;
      }
    printf ("\tLoaded %d network ARCs\n", ways);
    fclose (xml);
/* dropping the OSM_TMP_NODES table */
    db_cleanup (handle);
    if (in_memory)
      {
	  /* exporting the in-memory DB to filesystem */
	  sqlite3 *disk_handle;
	  sqlite3_backup *backup;
	  int ret;
	  printf ("\nexporting IN_MEMORY database ... wait please ...\n");
	  ret =
	      sqlite3_open_v2 (db_path, &disk_handle,
			       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			       NULL);
	  if (ret != SQLITE_OK)
	    {
		fprintf (stderr, "cannot open '%s': %s\n", db_path,
			 sqlite3_errmsg (disk_handle));
		sqlite3_close (disk_handle);
		return -1;
	    }
	  backup = sqlite3_backup_init (disk_handle, "main", handle, "main");
	  if (!backup)
	    {
		fprintf (stderr, "Backup failure: 'MEMORY-DB' wasn't saved\n");
		sqlite3_close (handle);
		sqlite3_close (disk_handle);
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
	  handle = disk_handle;
	  printf ("\tIN_MEMORY database succesfully exported\n");
      }
/* VACUUMing */
    db_vacuum (handle);
    sqlite3_close (handle);
    return 0;
}
