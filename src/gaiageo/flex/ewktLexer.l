/* 
 EwktLexer.l -- EWKT parser - FLEX config
  
 version 2.4, 2011 May 14

 Author: Sandro Furieri a.furieri@lqt.it

 ------------------------------------------------------------------------------
 
 Version: MPL 1.1/GPL 2.0/LGPL 2.1
 
 The contents of this file are subject to the Mozilla Public License Version
 1.1 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at
 http://www.mozilla.org/MPL/
 
Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the
License.

The Original Code is the SpatiaLite library

The Initial Developer of the Original Code is Alessandro Furieri
 
Portions created by the Initial Developer are Copyright (C) 2011
the Initial Developer. All Rights Reserved.

Alternatively, the contents of this file may be used under the terms of
either the GNU General Public License Version 2 or later (the "GPL"), or
the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
in which case the provisions of the GPL or the LGPL are applicable instead
of those above. If you wish to allow use of your version of this file only
under the terms of either the GPL or the LGPL, and not to allow others to
use your version of this file under the terms of the MPL, indicate your
decision by deleting the provisions above and replace them with the notice
and other provisions required by the GPL or the LGPL. If you do not delete
the provisions above, a recipient may use your version of this file under
the terms of any one of the MPL, the GPL or the LGPL.
 
*/

%option prefix="Ewkt"

%{

/* For debugging purposes */
int ewkt_line = 1, ewkt_col = 1;

%}

/**
*  The main string-token matcher.
*  The lower case part is probably not needed.  We should really be converting 
*  The string to all uppercase/lowercase to make it case iNsEnSiTiVe.
*  What Flex will do is, For the input string, beginning from the front, Flex
*  will try to match with any of the defined tokens from below.  Flex will 
*  then match the string of longest length.  Suppose the string is: POINTM,
*  Flex would match both POINT and POINTM, but since POINTM is the longer
*  of the two tokens, FLEX will match POINTM.
*/
%%
-?[0-9]+|-?[0-9]+"."[0-9]*|"+"?[0-9]+|"+"?[0-9]+"."[0-9]*       	{ ewkt_col += (int) strlen(yytext);  EwktLval.dval = atof(yytext); return EWKT_NUM; }
","										{ EwktLval.dval = 0; return EWKT_COMMA; }
"("										{ EwktLval.dval = 0; return EWKT_OPEN_BRACKET; }
")"										{ EwktLval.dval = 0; return EWKT_CLOSE_BRACKET; }
(?i:"point")							{ EwktLval.dval = 0; return EWKT_POINT; }
(?i:"pointm")							{ EwktLval.dval = 0; return EWKT_POINT_M; }
(?i:"linestring")						{ EwktLval.dval = 0; return EWKT_LINESTRING; }
(?i:"linestringm")						{ EwktLval.dval = 0; return EWKT_LINESTRING_M; }
(?i:"polygon")							{ EwktLval.dval = 0; return EWKT_POLYGON; }
(?i:"polygonm")							{ EwktLval.dval = 0; return EWKT_POLYGON_M; }
(?i:"multipoint")						{ EwktLval.dval = 0; return EWKT_MULTIPOINT; }
(?i:"multipointm")						{ EwktLval.dval = 0; return EWKT_MULTIPOINT_M; }
(?i:"multilinestring")					{ EwktLval.dval = 0; return EWKT_MULTILINESTRING; }
(?i:"multilinestringm")					{ EwktLval.dval = 0; return EWKT_MULTILINESTRING_M; }
(?i:"multipolygon")						{ EwktLval.dval = 0; return EWKT_MULTIPOLYGON; }
(?i:"multipolygonm")					{ EwktLval.dval = 0; return EWKT_MULTIPOLYGON_M; }
(?i:"geometrycollection")				{ EwktLval.dval = 0; return EWKT_GEOMETRYCOLLECTION; }
(?i:"geometrycollectionm")				{ EwktLval.dval = 0; return EWKT_GEOMETRYCOLLECTION_M; }

[ \t]   { ewkt_col += (int) strlen(yytext); }               /* ignore but count white space */

\n      { ewkt_col = 0; ++ewkt_line; }

.       { ewkt_col += (int) strlen(yytext); return -1; }
%%
/**
 * reset the line and column count
 *
 *
 */
void ewkt_reset_lexer(void)
{

  ewkt_line = 1;
  ewkt_col  = 1;

}

/**
 * EwktError() is invoked when the lexer or the parser encounter
 * an error. The error message is passed via *s
 *
 *
 */
void EwktError(char *s)
{
  spatialite_e("error: %s at line: %d col: %d\n",s,ewkt_line,ewkt_col);

}

int yywrap(void)
{
  return 1;
}