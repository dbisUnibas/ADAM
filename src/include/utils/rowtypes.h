/*-------------------------------------------------------------------------
 *
 * rowtypes.h
 *	  We extracted some code from rowtypes.c to rowtypes.h to make it available
 *    in all postgresql code.
 *
 *
 * src/include/utils/rowtypes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROWTYPES_H
#define ROWTYPES_H

#include "postgres.h"

#include <ctype.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/rowtypes.h"

/*
 * structure to cache metadata needed for record I/O
 */
typedef struct ColumnIOData
{
	Oid			column_type;
	Oid			typiofunc;
	Oid			typioparam;
	bool		typisvarlena;
	FmgrInfo	proc;
} ColumnIOData;

typedef struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[1];	/* VARIABLE LENGTH ARRAY */
} RecordIOData;



/*
 * structure to cache metadata needed for record comparison
 */
typedef struct ColumnCompareData
{
	TypeCacheEntry *typentry;	/* has everything we need, actually */
} ColumnCompareData;

typedef struct RecordCompareData
{
	int			ncolumns;		/* allocated length of columns[] */
	Oid			record1_type;
	int32		record1_typmod;
	Oid			record2_type;
	int32		record2_typmod;
	ColumnCompareData columns[1];		/* VARIABLE LENGTH ARRAY */
} RecordCompareData;

Datum record_in(PG_FUNCTION_ARGS);
Datum record_out(PG_FUNCTION_ARGS);
Datum record_recv(PG_FUNCTION_ARGS);
Datum record_send(PG_FUNCTION_ARGS);
Datum record_eq(PG_FUNCTION_ARGS);
Datum record_ne(PG_FUNCTION_ARGS);
Datum record_lt(PG_FUNCTION_ARGS);
Datum record_gt(PG_FUNCTION_ARGS);
Datum record_le(PG_FUNCTION_ARGS);
Datum record_ge(PG_FUNCTION_ARGS);
Datum btrecordcmp(PG_FUNCTION_ARGS);

#endif  