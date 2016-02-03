/* 
 * ADAM - indexing functions
 * name: adam_index_hash
 * description: functions for hash index (i.e. VA-file)
 * 
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/include/utils/adam_index_va.h
 *
 * 
 * 
 *
 *
 * addendum: the structure of this code is based on the bloom filter code provided on
 * http://www.sigaev.ru/misc/bloom-0.3.tar.gz
 *
 */
#ifndef ADAM_INDEX_VA_H
#define ADAM_INDEX_VA_H

#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"

#define VA_MAGICK_NUMBER	(0xDBAC0DED)

#define EPSILON	0.001

/*
*  pg_am functions
*/
extern Datum vaBuild(PG_FUNCTION_ARGS);
extern Datum vaInsert(PG_FUNCTION_ARGS);
extern Datum vaGetOptions(PG_FUNCTION_ARGS);
extern Datum vaBeginScan(PG_FUNCTION_ARGS);
extern Datum vaReScan(PG_FUNCTION_ARGS);
extern Datum vaEndScan(PG_FUNCTION_ARGS);
extern Datum vaMarkPos(PG_FUNCTION_ARGS);
extern Datum vaRestorePos(PG_FUNCTION_ARGS);
extern Datum vaBuildEmpty(PG_FUNCTION_ARGS);
extern Datum vaGetBitmap(PG_FUNCTION_ARGS);
extern Datum vaBulkDelete(PG_FUNCTION_ARGS);
extern Datum vaVacuumCleanup(PG_FUNCTION_ARGS);
extern Datum vaCostEstimate(PG_FUNCTION_ARGS);
extern Datum vaCanReturn(PG_FUNCTION_ARGS);

extern void vaRedo(XLogRecPtr lsn, XLogRecord *record);
extern void vaDesc(StringInfo buf, uint8 xl_info, char *rec);

extern bool enable_vascan;

#endif   /* ADAM_INDEX_HASH_H */

