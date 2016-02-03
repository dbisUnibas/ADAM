/*
 * ADAM - indexing functions
 * name: adam_index_hash
 * description: functions for hash index (i.e. VA-file)
 * the code is based on descriptions from: Weber, R. (2000) : Similarity Search in High-Dimensional Vector Spaces.
 *
 * developed in the course of the MSc thesis at the University of Basel
 *
 * author: Ivan Giangreco
 * email: ivan.giangreco@unibas.ch
 *
 * src/backend/utils/adt/adam_index_va.c
 *
 *
 *
 *
 *
 * addendum: the structure of this code is based on the bloom filter code provided on
 * http://www.sigaev.ru/misc/bloom-0.3.tar.gz
 *
 */

#include "postgres.h"

#include "utils/adam_index_va.h"


#include "commands/adam_data_featurefunctioncmds.h"
#include "parser/adam_data_parse_featurefunction.h"
#include "utils/adam_data_feature.h"
#include "utils/adam_index_marks.h"
#include "utils/adam_retrieval_minkowski.h"
#include "utils/adam_utils_bitstring.h"
#include "utils/adam_utils_priorityqueue.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_proc.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "nodes/tidbitmap.h"
#include "optimizer/cost.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

#define MIN(X, Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X, Y) ((X) > (Y) ? (X) : (Y))

/*
 * VA File page functions
 */
#define getOpaque(page)				( (Opaque) PageGetSpecialPointer(page) )
#define getMaxOffset(page)			( getOpaque(page)->maxoff )
#define isMeta(page)				( getOpaque(page)->flags & VA_META)
#define isDeleted(page)				( getOpaque(page)->flags & VA_DELETED)
#define setDeleted(page)			( getOpaque(page)->flags |= VA_DELETED)
#define setNonDeleted(page)			( getOpaque(page)->flags &= ~VA_DELETED)
#define getData(page)				(  (Tuple*)PageGetContents(page) )

#define VA_META						(1<<0)
#define VA_DELETED					(2<<0)

#define VA_METAPAGE_BLKNO  			(0)
#define VA_HEAD_BLKNO  				(1)

/*
 * VA File options (i.e. stored) and state (i.e.computed for current query)
 */
typedef struct FileOptions {
	int32				vl_len_;					/* varlena header (do not touch directly!) */
	int32				indexMarks;
} FileOptions;

typedef struct StateOptions {
	FileOptions	   *opts;						/* stored in rd_amcache and defined at creation time */
	int32				sizeOfTuple;
	ArrayType		   *marks;
	int32				dimensions;
	int32				partitions;
} StateOptions;

/*
 * VA File storage
 */

typedef struct OpaqueData {
	OffsetNumber	maxoff;
	uint16			flags;
} OpaqueData;
typedef OpaqueData *Opaque;

typedef BlockNumber FreeBlockNumberArray[MAXALIGN_DOWN(
	BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(OpaqueData)) -
	/* header of MetaPageData struct */
	MAXALIGN(sizeof(uint16)* 2 + sizeof(uint32)+sizeof(FileOptions))
	) / sizeof(BlockNumber)];

typedef struct MetaPageData {
	uint32					magickNumber;
	uint32					nChanges;
	uint16					nStart;
	uint16					nEnd;
	FreeBlockNumberArray	notFullPage;
} MetaPageData;


typedef struct Tuple {
	ItemPointerData		heapPtr;
	BitStringElement	apx[1];
} Tuple;
#define TupleHDRSZ	offsetof(Tuple, sign)

#define MetaBlockN			(sizeof(FreeBlockNumberArray) / sizeof(BlockNumber))
#define GetMeta(p)		((MetaPageData *) PageGetContents(p))
#define GetFreePageSpace(state, page) \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
	- getMaxOffset(page) * (state)->sizeOfTuple \
	- MAXALIGN(sizeof(OpaqueData)))


/*
 * VA File scan
 */
typedef struct ScanOpaqueData{
	StateOptions	state;
} ScanOpaqueData;
typedef ScanOpaqueData *ScanOpaque;

typedef struct BuildState{
	StateOptions		blstate;
	MemoryContext	tmpCtx;
	Buffer			currentBuffer;
	Page			currentPage;
} BuildState;



/*
 * VA File management functions
 */
static Tuple* formTuple(StateOptions *state, ItemPointer iptr, Datum *values, bool *isnull);
static void buildCallback(Relation index, HeapTuple htup, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static bool addItemToBlock(Relation index, StateOptions *state, Tuple *itup, BlockNumber blkno);
static void initStateOptions(StateOptions *state, Relation index, ArrayType *marks);
static bool addItem(StateOptions *state, Page p, Tuple *t);
static Buffer newBuffer(Relation index);
static void initBuffer(Buffer b, uint16 f);
static void initPage(Page page, uint16 f, uint16 maxoff, Size pageSize);
static void initMetabuffer(Buffer b, Relation index);


/*
 * VA File specific functions for calculations
 */
static float8 get_bound(BitStringElement *vector, float8* bounds, int32 dimensions, int32 partitions, MinkowskiNorm norm);
static void set_bitstring(feature *f, ArrayType *marks, BitStringElement *result);

static float8* precompute_differences_ubound(Datum *f, ArrayType *marks, MinkowskiNorm norm);
static float8* precompute_differences_lbound(Datum *f, ArrayType *marks, MinkowskiNorm norm);
static float8* precompute_differences_lbound_lnorm(feature *f, ArrayType *marks, MinkowskiNorm norm);
static float8* precompute_differences_ubound_lnorm(feature *f, ArrayType *marks, MinkowskiNorm norm);


/*
 * enabler/disabler
 */
bool enable_vascan = true;

/*
 *  Prepare for an index scan.
 *
 *  Parameters:
 *   Relation indexRelation
 *   int nkeys - indicate the number of quals operators that will be used in the scan
 *   int norderbys - indicate the number of ordering operators that will be used in the scan
 *   (may be useful for space allocation purposes)
 *
 *  Returns:
 *   IndexScanDesc
 *
 *  Note that the actual values of the scan keys aren't provided yet.
 *  The result must be a palloc'd struct. For implementation reasons the index access method
 *  must create this struct by calling RelationGetIndexScan(). In most cases ambeginscan does
 *  little beyond making that call and perhaps acquiring locks; the interesting parts of
 *  index-scan startup are in amrescan.
 */
Datum
vaBeginScan(PG_FUNCTION_ARGS)
{
	Relation    rel = (Relation)PG_GETARG_POINTER(0);
	int         keysz = PG_GETARG_INT32(1);
	int			norderbys = PG_GETARG_INT32(2);
	IndexScanDesc scan;

	scan = RelationGetIndexScan(rel, keysz, norderbys);


	PG_RETURN_POINTER(scan);
}



/*
 * Start or restart an index scan, possibly with new scan keys.
 *
 * Parameters:
 *  IndexScanDesc scan
 *  ScanKey keys
 *  int nkeys
 *  ScanKey orderbys
 *  int norderbys
 *
 *  Returns:
 *   void
 *
 * To restart using previously-passed keys, NULL is passed for keys and/or orderbys.
 * Note that it is not allowed for the number of keys or order-by operators to be larger
 * than what was passed to ambeginscan.
 * In practice the restart feature is used when a new outer tuple is selected by a nested-loop
 * join and so a new key comparison value is needed, but the scan key structure remains the same.
 */
Datum
vaReScan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
	ScanKey     keys = (ScanKey)PG_GETARG_POINTER(1);
	//ScanKey     orderbys = (ScanKey) PG_GETARG_POINTER(3);

	ScanOpaque so = (ScanOpaque)scan->opaque;

	if (so == NULL) {
		/* if called from blbeginscan */
		so = (ScanOpaque)palloc(sizeof(ScanOpaqueData));
		initStateOptions(&so->state, scan->indexRelation, NULL);
		scan->opaque = so;
	}

	if (keys && scan->numberOfKeys > 0)	{
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));
	}

	PG_RETURN_VOID();
}



/*
 * End a scan and release resources.
 *
 * Parameters:
 *  IndexScanDesc scan
 *
 *  Returns:
 *   void
 *
 * The scan struct itself should not be freed, but any locks or pins taken internally by the access method must be released.
 */
Datum
vaEndScan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
	ScanOpaque so = (ScanOpaque)scan->opaque;

	if (so){
		pfree(so);
	}
	scan->opaque = NULL;

	PG_RETURN_VOID();
}



/*
 * Fetch all tuples in the given scan and add them to the caller-supplied TIDBitmap
 * (that is, OR the set of tuple IDs into whatever set is already in the bitmap).
 *
 * Parameters:
 *  IndexScanDesc scan
 *  TIDBitmap *tbm
 *
 *  Returns:
 *   int64
 *
 * The number of tuples fetched is returned (this might be just an approximate count,
 * for instance some AMs do not detect duplicates). While inserting tuple IDs into the
 * bitmap, amgetbitmap can indicate that rechecking of the scan conditions is required
 * for specific tuple IDs.
 *
 * The amgetbitmap function need only be provided if the access method supports "bitmap" index scans.
 * If it doesn't, the amgetbitmap field in its pg_am row must be set to zero.
 */
static Datum bitmapSingleSearch(IndexScanDesc scan, TIDBitmap *tbm);
static Datum bitmapMultiSearch(IndexScanDesc scan, TIDBitmap *tbm);

Datum
vaGetBitmap(PG_FUNCTION_ARGS)
{
	IndexScanDesc 			scan = (IndexScanDesc)PG_GETARG_POINTER(0);
	TIDBitmap  				*tbm = (TIDBitmap *)PG_GETARG_POINTER(1);

	AdamScanClause			*adamOptions = (AdamScanClause *)scan->adamScanClause;

	if (adamOptions->check_tid && adamOptions->nn_limit > 0 && !tbm_is_empty(tbm)){
		return bitmapMultiSearch(scan, tbm);
	}
	else {
		return bitmapSingleSearch(scan, tbm);
	}
}

/*
 * search function for WHERE clause given, e.g.
 *
 * WHERE im_f === '<...>'
 *
 * in this example no where clause was provided by the user, but the system
 * inserted the === operation to enforce the use of the VA index; in this case
 * the content of the TID list has not to be considered in search
 *
 * for efficiency this is a bit of a code copy of bitmapMultiSearch
 */
static Datum
bitmapSingleSearch(IndexScanDesc scan, TIDBitmap *tbm)
{
	AdamScanClause			*adamOptions = (AdamScanClause *)scan->adamScanClause;

	int64					ntids = 0;

	BlockNumber				blkno = VA_HEAD_BLKNO;
	BlockNumber				npages;

	BufferAccessStrategy	bas;

	ScanOpaque 			so = (ScanOpaque)scan->opaque;

	int						numResults = 0;
	MinkowskiNorm           norm = 0;

	Buffer					meta_buffer;
	MetaPageData		   *meta_data;

	float8				   *l_bounds;
	float8					l_bound;
	float8				   *u_bounds;
	float8					u_bound;

	feature				   *f;
	int						dimensions;

	FmgrInfo				numeric_cmp_fmgr;

	PriorityQueue		   *q = NULL;
	ScanKey					skey;

	fmgr_info(BTFLOAT8CMPOID, &numeric_cmp_fmgr);

	skey = scan->keyData;

	//if limit is not set, then this index function should not have been chosen!
	//error in cost function!
	numResults = adamOptions->nn_limit;
	norm = adamOptions->nn_minkowski;

	if (numResults > 0){
		q = createQueue(numResults, &numeric_cmp_fmgr);
	}
	else {
		//q is not created, thus we still do an index scan, but a very costly one (we add each tuple)!
	}

	if (norm < 0 || norm > 100){
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("VA indexing can only be used with Minkowski distances; the cost function estimator, however, did not take this into consideration"),
			errhint("Force the use of other indices or sequential scan.")));
	}


	PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno);

	if (scan->numberOfKeys > 0 && skey->sk_flags & SK_ISNULL){
		return (Datum)0;
	}

	//calculate lower bounds
	if (numResults > 0){
		l_bounds = precompute_differences_lbound(
			&skey->sk_argument,
			so->state.marks,
			norm);

		//calculate upper bounds
		u_bounds = precompute_differences_ubound(
			&skey->sk_argument,
			so->state.marks,
			norm);
	}

	f = (feature *)DatumGetPointer(skey->sk_argument);
	dimensions = MIN(so->state.dimensions, ArrayGetNItems(ARR_NDIM(&f->data), ARR_DIMS(&f->data)));

	bas = GetAccessStrategy(BAS_BULKREAD);

	if (!RELATION_IS_LOCAL(scan->indexRelation)){ LockRelation(scan->indexRelation, ShareLock); }
	npages = RelationGetNumberOfBlocks(scan->indexRelation);
	if (!RELATION_IS_LOCAL(scan->indexRelation)){ UnlockRelation(scan->indexRelation, ShareLock); }


	meta_buffer = ReadBuffer(scan->indexRelation, VA_METAPAGE_BLKNO);
	LockBuffer(meta_buffer, BUFFER_LOCK_SHARE);

	meta_data = GetMeta(BufferGetPage(meta_buffer));

	if (meta_data->magickNumber != VA_MAGICK_NUMBER){
		ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" contains corrupted content", RelationGetRelationName(scan->indexRelation)),
			errhint("Please REINDEX it.")));
	}

	// the user-defined threshold for changes in the VA files is 1000 changes
	// or if the number of changes exceeds 20% of the tuples (e.g. if only 10 tuples are indexed, adding 2 tuples
	// might change a lot already!)
	if (meta_data->nChanges > 1000 || meta_data->nChanges > (scan->indexRelation->rd_rel->reltuples * 0.2)){
		ereport(WARNING,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" has been updated too many times and should be re-created", RelationGetRelationName(scan->indexRelation)),
			errhint("Please REINDEX it.")));
	}

	UnlockReleaseBuffer(meta_buffer);

	for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++){
		Buffer 			buffer;
		Page			page;

		buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

		if (blkno + 1 < npages)
			PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno + 1);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (!isDeleted(page)){
			Tuple	 *itup = getData(page);
			Tuple   *itupEnd = (Tuple*)(((char*)itup) + so->state.sizeOfTuple * getMaxOffset(page));

			while (itup < itupEnd){
				//strategy as in (Weber, 2000, Program 5.6), implementation of VAF-NOA
				if (q){
					//calculate the lower bound
					l_bound = get_bound(itup->apx, l_bounds, dimensions,
						so->state.partitions, norm);

					if (insertIntoQueueCheck(q, Float8GetDatum(l_bound))){
						//calculate the upper bound
						u_bound = get_bound(itup->apx, u_bounds, dimensions,
							so->state.partitions, norm);

						if (insertIntoQueue(q, Float8GetDatum(l_bound), Float8GetDatum(u_bound))){
							//tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
							//ntids++;
						}
					}
				}

				//if q not created, i.e. we have no limit; our cost-function should have
				//caught this case, now we have a rather costly sequential search
				//(at least more expensive than just doing a sequential search)!
				if (!q){
					tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
					ntids++;
				}

				itup = (Tuple*)(((char*)itup) + so->state.sizeOfTuple);
			}
		}

		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

	if (q){
		for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++){
			Buffer 			buffer;
			Page			page;

			buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

			if (blkno + 1 < npages)
				PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno + 1);

			LockBuffer(buffer, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buffer);

			if (!isDeleted(page)){
				Tuple	 *itup = getData(page);
				Tuple   *itupEnd = (Tuple*)(((char*)itup) + so->state.sizeOfTuple * getMaxOffset(page));

				while (itup < itupEnd){
					//strategy as in (Weber, 2000, Program 5.6), implementation of VAF-NOA
					//calculate the lower bound
					l_bound = get_bound(itup->apx, l_bounds, dimensions,
						so->state.partitions, norm);

					if (insertIntoQueueCheck(q, Float8GetDatum(l_bound))){
						tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
						ntids++;
					}

					itup = (Tuple*)(((char*)itup) + so->state.sizeOfTuple);
				}
			}

			UnlockReleaseBuffer(buffer);
			CHECK_FOR_INTERRUPTS();
		}
	}

	FreeAccessStrategy(bas);

	if (q){
		pfree(q);
	}

	PG_RETURN_INT64(ntids);
}

/*
 * search function for WHERE clause given, e.g.
 *
 * WHERE image_name = 'test' OR im_f === '<...>'
 *
 * in this example the first where clause is provided by the user,
 * the second one is added by the system, to enforce the use of
 * the VA index for a distance search;
 * in the case of multiple WHERE clause elements, we first check all the
 * other indices and build a TID list (see nodeBitmapOr.c), then we use
 * this TID list in our VA index and only consider elements that are in the
 * TID list
 *
 * for efficiency this is a bit of a code copy of bitmapSingleSearch
 */
static Datum
bitmapMultiSearch(IndexScanDesc scan, TIDBitmap *tbm)
{
	AdamScanClause			*adamOptions = (AdamScanClause *)scan->adamScanClause;

	int64					ntids = 0;

	BlockNumber				blkno = VA_HEAD_BLKNO;
	BlockNumber				npages;

	BufferAccessStrategy	bas;

	ScanOpaque 			so = (ScanOpaque)scan->opaque;

	int						numResults = 0;
	MinkowskiNorm			norm = 0;

	Buffer					meta_buffer;
	MetaPageData		   *meta_data;

	float8				   *l_bounds;
	float8					l_bound;
	float8				   *u_bounds;
	float8					u_bound;

	feature				   *f;
	int						dimensions;

	FmgrInfo				numeric_cmp_fmgr;

	PriorityQueue		   *q = NULL;
	ScanKey					skey;

	fmgr_info(BTFLOAT8CMPOID, &numeric_cmp_fmgr);

	skey = scan->keyData;

	//if limit is not set, then this index function should not have been chosen!
	//error in cost function!
	//numResults = adamOptions->nn_limit;
	numResults = MAX(adamOptions->nn_limit, tbm_nentries(tbm));
	norm = adamOptions->nn_minkowski;

	if (numResults > 0){
		q = createQueue(numResults, &numeric_cmp_fmgr);
	}
	else {
		//q is not created, thus we still do an index scan, but a very costly one (we add each tuple)!
	}

	if (norm < 0 || norm > 100){
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("VA indexing can only be used with Minkowski distances; the cost function estimator, however, did not take this into consideration"),
			errhint("Force the use of other indices or sequential scan.")));
	}


	PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno);

	if (scan->numberOfKeys > 0 && skey->sk_flags & SK_ISNULL){
		return (Datum)0;
	}

	if (numResults > 0){
		//calculate lower bounds
		l_bounds = precompute_differences_lbound(
			&skey->sk_argument,
			so->state.marks,
			norm);

		//calculate upper bounds
		u_bounds = precompute_differences_ubound(
			&skey->sk_argument,
			so->state.marks,
			norm);
	}

	f = (feature *)DatumGetPointer(skey->sk_argument);
	dimensions = MIN(so->state.dimensions, ArrayGetNItems(ARR_NDIM(&f->data), ARR_DIMS(&f->data)));

	bas = GetAccessStrategy(BAS_BULKREAD);

	if (!RELATION_IS_LOCAL(scan->indexRelation)){ LockRelation(scan->indexRelation, ShareLock); }
	npages = RelationGetNumberOfBlocks(scan->indexRelation);
	if (!RELATION_IS_LOCAL(scan->indexRelation)){ UnlockRelation(scan->indexRelation, ShareLock); }


	meta_buffer = ReadBuffer(scan->indexRelation, VA_METAPAGE_BLKNO);
	LockBuffer(meta_buffer, BUFFER_LOCK_SHARE);

	meta_data = GetMeta(BufferGetPage(meta_buffer));

	if (meta_data->magickNumber != VA_MAGICK_NUMBER){
		ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" contains corrupted content", RelationGetRelationName(scan->indexRelation)),
			errhint("Please REINDEX it.")));
	}

	// the user-defined threshold for changes in the VA files is 1000 changes
	// or if the number of changes exceeds 20% of the tuples (e.g. if only 10 tuples are indexed, adding 2 tuples
	// might change a lot already!)
	if (meta_data->nChanges > 1000 || meta_data->nChanges > (scan->indexRelation->rd_rel->reltuples * 0.2)){
		ereport(WARNING,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" has been updated too many times and should be re-created", RelationGetRelationName(scan->indexRelation)),
			errhint("Please REINDEX it.")));
	}

	UnlockReleaseBuffer(meta_buffer);

	for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++){
		Buffer 			buffer;
		Page			page;

		buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

		if (blkno + 1 < npages)
			PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno + 1);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (!isDeleted(page)){
			Tuple	 *itup = getData(page);
			Tuple   *itupEnd = (Tuple*)(((char*)itup) + so->state.sizeOfTuple * getMaxOffset(page));

			while (itup < itupEnd){
				if (tbm_contains_tuple(tbm, &itup->heapPtr)){
					//strategy as in (Weber, 2000, Program 5.6), implementation of VAF-NOA
					if (q){
						//calculate the lower bound
						l_bound = get_bound(itup->apx, l_bounds, dimensions,
							so->state.partitions, norm);

						if (insertIntoQueueCheck(q, Float8GetDatum(l_bound))){

							//calculate the upper bound
							u_bound = get_bound(itup->apx, u_bounds, dimensions,
								so->state.partitions, norm);

							if (insertIntoQueue(q, Float8GetDatum(l_bound), Float8GetDatum(u_bound))){
								//tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
								//ntids++;
							}
						}
					}

					//if q not created, i.e. we have no limit; our cost-function should have
					//caught this case, now we have a very costly search
					//(at least more expensive than just doing a sequential search)!
					if (!q){
						tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
						ntids++;
					}
				}

				itup = (Tuple*)(((char*)itup) + so->state.sizeOfTuple);
			}
		}

		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

	if (q){
		for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++){
			Buffer 			buffer;
			Page			page;

			buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, blkno, RBM_NORMAL, bas);

			if (blkno + 1 < npages)
				PrefetchBuffer(scan->indexRelation, MAIN_FORKNUM, blkno + 1);

			LockBuffer(buffer, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buffer);

			if (!isDeleted(page)){
				Tuple	 *itup = getData(page);
				Tuple   *itupEnd = (Tuple*)(((char*)itup) + so->state.sizeOfTuple * getMaxOffset(page));

				while (itup < itupEnd){
					if (tbm_contains_tuple(tbm, &itup->heapPtr)){
						//strategy as in (Weber, 2000, Program 5.6), implementation of VAF-NOA

						//calculate the lower bound
						l_bound = get_bound(itup->apx, l_bounds, dimensions,
							so->state.partitions, norm);

						if (insertIntoQueueCheck(q, Float8GetDatum(l_bound))){
							tbm_add_tuples(tbm, &itup->heapPtr, 1, false);
							ntids++;
						}

					}

					itup = (Tuple*)(((char*)itup) + so->state.sizeOfTuple);
				}
			}

			UnlockReleaseBuffer(buffer);
			CHECK_FOR_INTERRUPTS();
		}
	}


	FreeAccessStrategy(bas);

	if (q){
		pfree(q);
	}

	PG_RETURN_INT64(ntids);
}



/*
 * Build a new index. The index relation has been physically created, but is empty.
 *
 * Parameters:
 *  Relation heapRelation
 *  Relation indexRelation
 *  IndexInfo *indexInfo
 *
 * Returns:
 *  IndexBuildResult *
 *
 * It must be filled in with whatever fixed data the access method requires, plus entries for
 * all tuples already existing in the table. Ordinarily the ambuild function will call
 * IndexBuildHeapScan() to scan the table for existing tuples and compute the keys that need to
 * be inserted into the index.
 * The function must return a palloc'd struct containing statistics about the new index.
 */
Datum
vaBuild(PG_FUNCTION_ARGS)
{
	Relation    heap = (Relation)PG_GETARG_POINTER(0);
	Relation    index = (Relation)PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double      reltuples;
	BuildState buildstate;
	Buffer		MetaBuffer;

	Datum	marks;

	ListCell	*index_field;

	if (RelationGetNumberOfBlocks(index) != 0){
		elog(ERROR, "index \"%s\" already contains data",
			RelationGetRelationName(index));
	}

	foreach(index_field, indexInfo->ii_Expressions){
		FieldSelect *field = (FieldSelect *)lfirst(index_field);

		if (field->resulttype != FEATURE){
			ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				errmsg("VA indexing is only supported for features data types"),
				errhint("Please use other indexing methods or change the data type to FEATURE.")));

		}
	}

	/* initialize the meta page */
	MetaBuffer = newBuffer(index);

	marks = calculateMarks(heap, indexInfo);
	UpdateIndexAddMarks(index->rd_id, marks);

	START_CRIT_SECTION();
	initMetabuffer(MetaBuffer, index);
	MarkBufferDirty(MetaBuffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(MetaBuffer);

	initStateOptions(&buildstate.blstate, index, DatumGetArrayTypeP(marks));

	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
		"VA build temporary context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

	buildstate.currentBuffer = InvalidBuffer;

	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
		buildCallback, (void *)&buildstate);

	/* close opened buffer */
	if (buildstate.currentBuffer != InvalidBuffer)
	{
		MarkBufferDirty(buildstate.currentBuffer);
		UnlockReleaseBuffer(buildstate.currentBuffer);
	}

	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = reltuples;

	PG_RETURN_POINTER(result);
}



/*
 * Build an empty index, and write it to the initialization fork (INIT_FORKNUM) of the given relation.
 *
 * Parameters:
 *  Relation indexRelation
 *
 * Returns:
 *  void
 *
 * This method is called only for unlogged tables; the empty index written to the initialization fork
 * will be copied over the main relation fork on each server restart.
 */
Datum
vaBuildEmpty(PG_FUNCTION_ARGS)
{
	elog(ERROR, "VA does not support empty indexes");
	PG_RETURN_VOID();
}



/*
 * Insert a new tuple into an existing index.
 *
 * Parameters:
 *  Relation indexRelation
 *  Datum *values
 *  bool *isnull
 *  ItemPointer heap_tid
 *  Relation heapRelation
 *  IndexUniqueCheck checkUnique
 *
 * Returns:
 *  bool
 *
 * The values and isnull arrays give the key values to be indexed, and heap_tid is the TID to be indexed.
 * If the access method supports unique indexes (its pg_am.amcanunique flag is true) then checkUnique
 * indicates the type of uniqueness check to perform. This varies depending on whether the unique constraint is
 * deferrable. Normally the access method only needs the heapRelation parameter when performing uniqueness
 * checking (since then it will have to look into the heap to verify tuple liveness).
 */
Datum
vaInsert(PG_FUNCTION_ARGS)
{
	Relation    index = (Relation)PG_GETARG_POINTER(0);
	Datum      *values = (Datum *)PG_GETARG_POINTER(1);
	bool       *isnull = (bool *)PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
	//Relation    heapRel = (Relation) PG_GETARG_POINTER(4);
	//IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);

	StateOptions   	 blstate;
	Tuple		*itup;
	MemoryContext 	oldCtx;
	MemoryContext 	insertCtx;
	MetaPageData	*metaData;
	Buffer				metaBuffer,
		buffer;
	BlockNumber			blkno = InvalidBlockNumber;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
		"VA insert temporary context",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	initStateOptions(&blstate, index, NULL);
	itup = formTuple(&blstate, ht_ctid, values, isnull);

	metaBuffer = ReadBuffer(index, VA_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
	metaData = GetMeta(BufferGetPage(metaBuffer));

	if (metaData->nEnd > metaData->nStart) {
		blkno = metaData->notFullPage[metaData->nStart];

		Assert(blkno != InvalidBlockNumber);


		if (addItemToBlock(index, &blstate, itup, blkno)){
			START_CRIT_SECTION();
			metaData->nChanges++;
			END_CRIT_SECTION();
			MarkBufferDirty(metaBuffer);
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			goto away;
		}

		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
	}
	else {
		/* no avaliable pages */
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
	}

	/* protect any changes on metapage with a help of CRIT_SECTION */
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();
	if (metaData->nEnd > metaData->nStart &&
		blkno == metaData->notFullPage[metaData->nStart])
		metaData->nStart++;
	END_CRIT_SECTION();

	while (metaData->nEnd > metaData->nStart){
		blkno = metaData->notFullPage[metaData->nStart];

		Assert(blkno != InvalidBlockNumber);
		if (addItemToBlock(index, &blstate, itup, blkno)) {
			LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();
			metaData->nChanges++;
			END_CRIT_SECTION();
			MarkBufferDirty(metaBuffer);
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			goto away;
		}

		START_CRIT_SECTION();
		metaData->nStart++;
		END_CRIT_SECTION();
	}

	/* no free pages */
	buffer = newBuffer(index);
	initBuffer(buffer, 0);
	addItem(&blstate, BufferGetPage(buffer), itup);

	START_CRIT_SECTION();
	metaData->nStart = 0;
	metaData->nEnd = 1;
	metaData->notFullPage[0] = BufferGetBlockNumber(buffer);
	END_CRIT_SECTION();

	MarkBufferDirty(metaBuffer);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

away:
	ReleaseBuffer(metaBuffer);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	PG_RETURN_BOOL(false);
}



/*
 * Mark current scan position. The access method need only support one remembered scan position per scan.
 *
 * Parameters:
 *  IndexScanDesc scan
 *
 * Returns:
 *  void
 */
Datum
vaMarkPos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "VA does not support mark/restore");
	PG_RETURN_VOID();
}



/*
 * Restore the scan to the most recently marked position.
 *
 * Parameters:
 *  IndexScanDesc scan
 *
 * Returns:
 *  void
 */
Datum
vaRestorePos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "VA does not support mark/restore");
	PG_RETURN_VOID();
}



/*
 * Check whether the index can support index-only scans by returning the indexed
 * column values for an index entry in the form of an IndexTuple. Return TRUE if so, else FALSE.
 *
 * Parameters:
 *  Relation indexRelation
 *
 * Returns:
 *  bool
 *
 * If the index AM can never support index-only scans (an example is hash, which stores only the hash
 * values not the original data), it is sufficient to set its amcanreturn field to zero in pg_am.
 */
Datum
vaCanReturn(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(false);
}



/*
 * Estimate the costs of an index scan
 *
 * Parameters:
 *  PlannerInfo *root
 *  IndexPath *path
 *  double loop_count
 *  Cost *indexStartupCost
 *  Cost *indexTotalCost
 *  Selectivity *indexSelectivity
 *  double *indexCorrelation
 *
 * Returns:
 *  void
 *
 * see http://www.postgresql.org/docs/9.2/static/index-cost-estimation.html
 */
static FileOptions* getRelopts(Oid idx);

Datum
vaCostEstimate(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *)PG_GETARG_POINTER(0);
	IndexPath  *path = (IndexPath *)PG_GETARG_POINTER(1);
	//double		loop_count = PG_GETARG_FLOAT8(2);
	Cost	   *indexStartupCost = (Cost *)PG_GETARG_POINTER(3);
	Cost	   *indexTotalCost = (Cost *)PG_GETARG_POINTER(4);
	Selectivity *indexSelectivity = (Selectivity *)PG_GETARG_POINTER(5);
	double	   *indexCorrelation = (double *)PG_GETARG_POINTER(6);

	//AdamQueryClause *adamOptions = (AdamQueryClause *) root->parse->adamQueryClause;

	FileOptions* relopts = getRelopts(path->indexinfo->indexoid);

	bool disableCost = false;

	vafilecostestimate(fcinfo);

	/* if the index is an index based on equifrequent mars we should prefer it over equidistant marks */
	if (relopts->indexMarks == VA_MARKS_EQUIFREQUENT){
		*indexTotalCost *= 0.99;
	}

	/* index is not useful if we want a large number of tuples */
	if (root->limit_tuples == 0 || (root->limit_tuples > 500 && root->limit_tuples / path->indexinfo->tuples > 0.1)){
		disableCost = true;
	}

	/* if offset used, then VA is not useful */
	if (root->parse->limitOffset){
		disableCost = true;
	}

	if (!enable_vascan){
		disableCost = true;
	}

	/* TODO: we should also check for high dimensionality, but that is not yet possible */

	/* do maximum costs if index is not useful */
	if (disableCost){
		*indexStartupCost = disable_cost + 1;
		*indexTotalCost = disable_cost + 1;
		*indexSelectivity = disable_cost + 1;
		*indexCorrelation = disable_cost + 1;
	}

	PG_RETURN_VOID();
}

/*
 *  checks the relopts to get the minkowski distance
 *  and use this information for cost calculation
 */
static FileOptions*
getRelopts(Oid idx)
{
	HeapTuple	index_tpl;
	FileOptions *rdopts;
	Relation rel;

	rel = heap_open(RelationRelationId, AccessShareLock);

	index_tpl = SearchSysCache1(RELOID, ObjectIdGetDatum(idx));
	if (!HeapTupleIsValid(index_tpl)){
		ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index contains corrupted content"),
			errhint("Please REINDEX it.")));
	}

	rdopts = (FileOptions *)extractRelOptions(index_tpl, RelationGetDescr(rel), VAOPTIONS);

	ReleaseSysCache(index_tpl);
	heap_close(rel, AccessShareLock);

	return rdopts;
}


/*
 * Delete tuple(s) from the index.
 *
 * Parameters:
 *  IndexVacuumInfo *info
 *  IndexBulkDeleteResult *stats
 *  IndexBulkDeleteCallback callback
 *  void *callback_state
 *
 * Returns:
 *  IndexBulkDeleteResult *
 *
 * This is a "bulk delete" operation that is intended to be implemented by scanning the
 * whole index and checking each entry to see if it should be deleted. The passed-in callback
 * function must be called, in the style callback(TID, callback_state) returns bool, to determine
 * whether any particular index entry, as identified by its referenced TID, is to be deleted.
 * Must return either NULL or a palloc'd struct containing statistics about the effects of
 * the deletion operation. It is OK to return NULL if no information needs to be passed on to
 * amvacuumcleanup.
 *
 * Because of limited maintenance_work_mem, ambulkdelete might need to be called more than once
 * when many tuples are to be deleted. The stats argument is the result of the previous call for
 * this index (it is NULL for the first call within a VACUUM operation). This allows the AM to
 * accumulate statistics across the whole operation. Typically, ambulkdelete will modify and
 * return the same struct if the passed stats is not null.
 */
Datum
vaBulkDelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo 		*info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
	IndexBulkDeleteResult 	*stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
	void       				*callback_state = (void *)PG_GETARG_POINTER(3);
	Relation    			index = info->index;
	BlockNumber             blkno,
		npages;
	FreeBlockNumberArray	notFullPage;
	int						countPage = 0;
	StateOptions				state;
	bool					needLock;
	Buffer					buffer;
	Page            		page;


	if (stats == NULL)
		stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

	initStateOptions(&state, index, NULL);

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelation(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelation(index, ExclusiveLock);

	for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++)
	{
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, info->strategy);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (!isDeleted(page)){
			Tuple	*itup = getData(page);
			Tuple	*itupEnd = (Tuple*)(((char*)itup) +
				state.sizeOfTuple * getMaxOffset(page));
			Tuple	*itupPtr = itup;

			while (itup < itupEnd){
				if (callback(&itup->heapPtr, callback_state)){
					stats->tuples_removed += 1;
					START_CRIT_SECTION();
					getOpaque(page)->maxoff--;
					END_CRIT_SECTION();
				}
				else {
					if (itupPtr != itup){
						START_CRIT_SECTION();
						memcpy(itupPtr, itup, state.sizeOfTuple);
						END_CRIT_SECTION();
					}
					stats->num_index_tuples++;
					itupPtr = (Tuple*)(((char*)itupPtr) + state.sizeOfTuple);
				}

				itup = (Tuple*)(((char*)itup) + state.sizeOfTuple);
			}

			if (itupPtr != itup){
				if (itupPtr == getData(page))	{
					START_CRIT_SECTION();
					setDeleted(page);
					END_CRIT_SECTION();
				}
				MarkBufferDirty(buffer);
			}

			if (!isDeleted(page) &&
				GetFreePageSpace(&state, page) > state.sizeOfTuple &&
				countPage < MetaBlockN){
				notFullPage[countPage++] = blkno;
			}
		}

		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

	if (countPage>0){
		MetaPageData	*metaData;

		buffer = ReadBuffer(index, VA_METAPAGE_BLKNO);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		metaData = GetMeta(page);
		START_CRIT_SECTION();
		memcpy(metaData->notFullPage, notFullPage, sizeof(FreeBlockNumberArray));
		metaData->nChanges += stats->tuples_removed;
		metaData->nStart = 0;
		metaData->nEnd = countPage;
		END_CRIT_SECTION();

		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);
	}

	PG_RETURN_POINTER(stats);
}



/*
 * Clean up after a VACUUM operation (zero or more ambulkdelete calls).
 *
 * Parameters:
 *  IndexVacuumInfo *info
 *  IndexBulkDeleteResult *stats
 *
 * Returns:
 *  IndexBulkDeleteResult *
 *
 * This does not have to do anything beyond returning index statistics, but it might
 * perform bulk cleanup such as reclaiming empty index pages. stats is whatever the
 * last ambulkdelete call returned, or NULL if ambulkdelete was not called because
 * no tuples needed to be deleted. If the result is not NULL it must be a palloc'd struct.
 * The statistics it contains will be used to update pg_class, and will be reported by
 * VACUUM if VERBOSE is given. It is OK to return NULL if the index was not changed at
 * all during the VACUUM operation, but otherwise correct stats should be returned.
 *
 * As of PostgreSQL 8.4, amvacuumcleanup will also be called at completion of an ANALYZE
 * operation. In this case stats is always NULL and any return value will be ignored. This
 * case can be distinguished by checking info->analyze_only. It is recommended that the
 * access method do nothing except post-insert cleanup in such a call, and that only in
 * an autovacuum worker process.
 */
Datum
vaVacuumCleanup(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
	Relation    index = info->index;
	bool        needLock;
	BlockNumber npages,
		blkno;
	BlockNumber totFreePages;
	BlockNumber lastBlock = VA_HEAD_BLKNO,
		lastFilledBlock = VA_HEAD_BLKNO;

	if (info->analyze_only)
		PG_RETURN_POINTER(stats);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelation(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelation(index, ExclusiveLock);

	totFreePages = 0;
	for (blkno = VA_HEAD_BLKNO; blkno < npages; blkno++){
		Buffer      buffer;
		Page        page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, info->strategy);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = (Page)BufferGetPage(buffer);

		if (isDeleted(page)) {
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else {
			lastFilledBlock = blkno;
			stats->num_index_tuples += getMaxOffset(page);
			stats->estimated_count += getMaxOffset(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	lastBlock = npages - 1;
	if (lastBlock > lastFilledBlock){
		RelationTruncate(index, lastFilledBlock + 1);
		stats->pages_removed = lastBlock - lastFilledBlock;
		totFreePages = totFreePages - stats->pages_removed;
	}

	IndexFreeSpaceMapVacuum(info->index);
	stats->pages_free = totFreePages;

	if (needLock) LockRelation(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock) UnlockRelation(index, ExclusiveLock);


	PG_RETURN_POINTER(stats);
}



/*
 * Parse and validate the reloptions array for an index.
 *
 * Parameters:
 *  ArrayType *reloptions
 *  bool validate
 *
 * Returns:
 *  bytea *
 *
 * This is called only when a non-null reloptions array exists for the index. reloptions is a text
 * array containing  entries of the form name=value. The function should construct a bytea value,
 * which will be copied into the rd_options field of the index's relcache entry. The data contents
 * of the bytea value are open for the access method to define; most of the standard access methods
 * use struct StdRdOptions. When validate is true, the function should report a suitable error
 * message if any of the options are unrecognized or have invalid values; when validate is false,
 * invalid entries should be silently ignored. (validate is false when loading options already
 * stored in pg_catalog; an invalid entry could only be found if the access method has changed
 * its rules for options, and in that case ignoring obsolete entries is appropriate.)
 * It is OK to return NULL if default behavior is wanted.
 */
Datum
vaGetOptions(PG_FUNCTION_ARGS)
{
	Datum       		reloptions = PG_GETARG_DATUM(0);
	bool        		validate = PG_GETARG_BOOL(1);
	relopt_value 		*options;

	int			numoptions = -1;
	FileOptions		*rdopts;
	relopt_parse_elt 	tab[1];

	/* we store the information about what kind of index it is in the relopts
	 only becuase of the cost calculation */
	tab[0].optname = "vamarks";
	tab[0].opttype = RELOPT_TYPE_INT;
	tab[0].offset = offsetof(FileOptions, indexMarks);

	options = parseRelOptions(reloptions, validate, RELOPT_KIND_VA, &numoptions);
	rdopts = allocateReloptStruct(sizeof(FileOptions), options, numoptions);
	fillRelOptions((void *)rdopts, sizeof(FileOptions), options, numoptions, validate, tab, 1);

	PG_RETURN_BYTEA_P(rdopts);
}


/*
 * initiates a VA state (i.e. a temporary, scan-related struct)
 */
static void
initStateOptions(StateOptions *state, Relation index, ArrayType *tmp_marks)
{
	int			*dims;

	//int32		featureSubtype = (**index->rd_att->attrs).atttypmod;


	if (!index->rd_amcache){
		Buffer				buffer;
		MetaPageData		*meta;
		FileOptions		*opts;

		opts = MemoryContextAlloc(index->rd_indexcxt, VARSIZE(index->rd_options));

		buffer = ReadBuffer(index, VA_METAPAGE_BLKNO);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		if (!isMeta(BufferGetPage(buffer)))
			ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" contains corrupted content", RelationGetRelationName(index)),
			errhint("Please REINDEX it.")));
		meta = GetMeta(BufferGetPage(buffer));

		if (meta->magickNumber != VA_MAGICK_NUMBER)
			ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" contains corrupted content", RelationGetRelationName(index)),
			errhint("Please REINDEX it.")));

		memcpy(opts, index->rd_options, VARSIZE(index->rd_options));
		UnlockReleaseBuffer(buffer);

		index->rd_amcache = (void*)opts;
	}


	state->opts = (FileOptions*)index->rd_amcache;

	if (tmp_marks){
		state->marks = tmp_marks;
	}
	else {
		state->marks = RelationGetMarks(index);
	}

	if (state->marks == NULL){
		ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("internal error at creation of marks"),
			errhint("VA index can only be created for commited data, but no marks could be found")));
	}

	dims = ARR_DIMS(state->marks);

	state->dimensions = dims[0];

	/* warn at low dimensionality? */

	state->partitions = dims[1];
	state->sizeOfTuple = sizeof(Tuple)+(state->dimensions) * sizeof(BitStringElement);
}

/*
 * forms a VA tuple to store in index
 */
static Tuple*
formTuple(StateOptions *state, ItemPointer iptr, Datum *values, bool *isnull)
{
	Tuple	*res = palloc0(state->sizeOfTuple);

	res->heapPtr = *iptr;

	if (!(*isnull)){
		feature		*f = (feature *)PG_DETOAST_DATUM(values[0]);
		set_bitstring(f, state->marks, res->apx);
	}

	return res;
}

/*
 * callback function after building index
 */
static void
buildCallback(Relation index, HeapTuple htup, Datum *values,
bool *isnull, bool tupleIsAlive, void *state)
{
	BuildState	*buildstate = (BuildState*)state;
	MemoryContext	oldCtx;
	Tuple		*itup;

	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	itup = formTuple(&buildstate->blstate, &htup->t_self, values, isnull);

	if (buildstate->currentBuffer == InvalidBuffer ||
		addItem(&buildstate->blstate, buildstate->currentPage, itup) == false) {
		if (buildstate->currentBuffer != InvalidBuffer){
			MarkBufferDirty(buildstate->currentBuffer);
			UnlockReleaseBuffer(buildstate->currentBuffer);
		}

		CHECK_FOR_INTERRUPTS();

		/* newBuffer returns locked page */
		buildstate->currentBuffer = newBuffer(index);
		initBuffer(buildstate->currentBuffer, 0);
		buildstate->currentPage = BufferGetPage(buildstate->currentBuffer);

		if (addItem(&buildstate->blstate, buildstate->currentPage, itup) == false)
			ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg("index \"%s\" contains corrupted content", RelationGetRelationName(index)),
			errhint("Please REINDEX it.")));
	}

	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}


/*
 * adds an item to storage block
 */
static bool
addItemToBlock(Relation index, StateOptions *state, Tuple *itup, BlockNumber blkno)
{
	Buffer		buffer;
	Page		page;

	buffer = ReadBuffer(index, blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	START_CRIT_SECTION();
	if (addItem(state, page, itup)){
		/* inserted */
		END_CRIT_SECTION();
		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);
		return true;
	}
	else 	{
		END_CRIT_SECTION();
		UnlockReleaseBuffer(buffer);
		return false;
	}
}

/*
 * adds an item to the VA file
 */
static bool
addItem(StateOptions *state, Page p, Tuple *t)
{
	Tuple		*pagePtr;
	Opaque	opaque;

	if (GetFreePageSpace(state, p) < state->sizeOfTuple)
		return false;

	opaque = getOpaque(p);
	pagePtr = getData(p);
	memcpy(((char*)pagePtr) + opaque->maxoff * state->sizeOfTuple,
		t, state->sizeOfTuple);
	opaque->maxoff++;

	return true;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling initBuffer
 */
static Buffer
newBuffer(Relation index)
{
	Buffer      buffer;
	bool        needLock;

	/* First, try to get a page from FSM */
	for (;;){
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);

		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer)){
			Page        page = BufferGetPage(buffer);

			if (PageIsNew(page))
				return buffer;  /* OK to use, if never initialized */

			if (isDeleted(page))
				return buffer;  /* OK to use */

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* Must extend the file */
	needLock = !RELATION_IS_LOCAL(index);
	if (needLock)
		LockRelation(index, ExclusiveLock);

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelation(index, ExclusiveLock);

	return buffer;
}

/*
 * initializes the buffer
 */
static void
initBuffer(Buffer b, uint16 f)
{
	initPage(BufferGetPage(b), f, 0, BufferGetPageSize(b));
}


/*
 * initializes the content of a page
 */
static void
initPage(Page page, uint16 f, uint16 maxoff, Size pageSize)
{
	Opaque opaque;

	PageInit(page, pageSize, sizeof(OpaqueData));

	opaque = getOpaque(page);
	memset(opaque, 0, sizeof(OpaqueData));
	opaque->maxoff = maxoff;
	opaque->flags = f;
}

/*
 * initializes the metapage buffer
 */
static void
initMetabuffer(Buffer b, Relation index)
{
	MetaPageData	*metadata;
	Page			 page = BufferGetPage(b);

	initPage(page, VA_META, 0, BufferGetPageSize(b));
	metadata = GetMeta(page);

	memset(metadata, 0, sizeof(MetaPageData));
	metadata->magickNumber = VA_MAGICK_NUMBER;
	metadata->nChanges = 0;
}



/*
 * Determines bounds on distances.
 * For the calculation the function trans_distfunc is used.
 *
 * (Weber, 2000, Section 5.5.4)
 */
static float8*
precompute_differences_lbound(Datum *query, ArrayType *marks, MinkowskiNorm norm)
{
	feature *f = (feature *)DatumGetPointer(*query);

	if (norm == MINKOWSKI_MAX_NORM){
		return precompute_differences_lbound_lnorm(f, marks, 1);
	}
	else {
		return precompute_differences_lbound_lnorm(f, marks, norm);
	}
}

static float8*
precompute_differences_ubound(Datum *query, ArrayType *marks, MinkowskiNorm norm)
{
	feature *f = (feature *)DatumGetPointer(*query);

	if (norm == MINKOWSKI_MAX_NORM){
		return precompute_differences_ubound_lnorm(f, marks, 1);
	}
	else {
		return precompute_differences_ubound_lnorm(f, marks, norm);
	}
}


static float8*
precompute_differences_lbound_lnorm(feature *f, ArrayType *marks_full, MinkowskiNorm norm)
{
	int typlen = sizeof(float8);
	bool typbyval = FLOAT8PASSBYVAL;
	char typalign = 'd';

	char	   *m_ptr;
	char	   *mp1_ptr;

	bool		f_lt_m;
	bool		f_gt_mp1;

	int			i,
		j = 0,
		i_max;

	ArrayIterator f_it = array_create_iterator(&f->data, 0);
	Datum		f_val;
	bool		f_isnull;
	float8		f_f8;

	ArrayIterator mfull_it = array_create_iterator(marks_full, 1);
	Datum		mfull_val;
	bool		mfull_isnull;
	float8		mfull_f8;

	ArrayIterator mfull2_it = array_create_iterator(marks_full, 1);
	Datum		mfull2_val;
	bool		mfull2_isnull;
	float8		mfull2_f8;

	ArrayType	*marks;

	Datum		num_norm;
	float8		*results;

	//too much space allocated here
	results = palloc(ArrayGetNItems(ARR_NDIM(&f->data), ARR_DIMS(&f->data)) *
		(ArrayGetNItems(ARR_NDIM(marks_full), ARR_DIMS(marks_full)) + 1) * sizeof(float8));

	while (array_iterate(mfull2_it, &mfull2_val, &mfull2_isnull)){
		mfull2_f8 = DatumGetFloat8(mfull2_val);
	}

	while (array_iterate(f_it, &f_val, &f_isnull) && array_iterate(mfull_it, &mfull_val, &mfull_isnull)){
		marks = DatumGetArrayTypeP(mfull_val);

		f_f8 = DatumGetFloat8(f_val);
		mfull_f8 = DatumGetFloat8(mfull_val);

		/* set mark pointer to start */
		m_ptr = ARR_DATA_PTR(marks);

		mp1_ptr = ARR_DATA_PTR(marks);
		mp1_ptr = att_addlength_pointer(mp1_ptr, typlen, mp1_ptr);
		mp1_ptr = (char *)att_align_nominal(mp1_ptr, typalign);

		i_max = ArrayGetNItems(ARR_NDIM(marks), ARR_DIMS(marks));

		for (i = 0; i < i_max - 1; i++){
			float8		m_f8;
			float8		mp1_f8;

			/* get mark */
			m_f8 = DatumGetFloat8(fetch_att(m_ptr, typbyval, typlen));
			mp1_f8 = DatumGetFloat8(fetch_att(mp1_ptr, typbyval, typlen));

			if (f_f8 < m_f8){
				results[j * i_max + i] = DatumGetFloat8(
					DirectFunctionCall2(dpow,
					Float8GetDatum(m_f8 - f_f8),
					Float8GetDatum(norm)));
			}
			else if (f_f8 > mp1_f8) {
				results[j * i_max + i] = DatumGetFloat8(
					DirectFunctionCall2(dpow,
					Float8GetDatum(f_f8 - mp1_f8),
					Float8GetDatum(norm)));
			}
			else {
				results[j * i_max + i] = DatumGetFloat8(DirectFunctionCall1(i4tod, Int32GetDatum(0)));
			}
			/* set mark pointer for next round */
			m_ptr = att_addlength_pointer(m_ptr, typlen, m_ptr);
			m_ptr = (char *)att_align_nominal(m_ptr, typalign);

			mp1_ptr = att_addlength_pointer(mp1_ptr, typlen, mp1_ptr);
			mp1_ptr = (char *)att_align_nominal(mp1_ptr, typalign);
		}
		results[j * i_max + i_max - 1] = results[j * i_max - 2];

		j++;
	}

	array_free_iterator(f_it);
	array_free_iterator(mfull_it);
	return results;
}

static float8*
precompute_differences_ubound_lnorm(feature *f, ArrayType *marks_full, MinkowskiNorm norm)
{
	int typlen = sizeof(float8);
	bool typbyval = FLOAT8PASSBYVAL;
	char typalign = 'd';

	char	   *m_ptr;
	char	   *mp1_ptr;

	bool		f_lt_b;

	int			i,
		j = 0,
		i_max;

	ArrayIterator f_it = array_create_iterator(&f->data, 0);
	Datum		f_val;
	bool		f_isnull;

	ArrayIterator mfull_it = array_create_iterator(marks_full, 1);
	Datum		mfull_val;
	bool		mfull_isnull;

	ArrayType	*marks;

	Datum		num_norm;
	Datum		num_two = DirectFunctionCall1(i4tod, Int32GetDatum(2));

	//too much space allocated here
	float8		*results = palloc(ArrayGetNItems(ARR_NDIM(&f->data), ARR_DIMS(&f->data)) *
		(ArrayGetNItems(ARR_NDIM(marks_full), ARR_DIMS(marks_full)) + 1) * sizeof(float8));

	num_norm = DirectFunctionCall1(i4tod, Int32GetDatum(norm));

	while (array_iterate(f_it, &f_val, &f_isnull) && array_iterate(mfull_it, &mfull_val, &mfull_isnull)){
		marks = DatumGetArrayTypeP(mfull_val);

		/* set mark pointer to start */
		m_ptr = ARR_DATA_PTR(marks);
		mp1_ptr = ARR_DATA_PTR(marks);

		mp1_ptr = att_addlength_pointer(mp1_ptr, typlen, mp1_ptr);
		mp1_ptr = (char *)att_align_nominal(mp1_ptr, typalign);

		i_max = ArrayGetNItems(ARR_NDIM(marks), ARR_DIMS(marks));

		for (i = 0; i < i_max - 1; i++){
			Datum		m_val;
			Datum		mp1_val;
			Datum		b_val;

			/* get mark */
			m_val = fetch_att(m_ptr, typbyval, typlen);
			mp1_val = fetch_att(mp1_ptr, typbyval, typlen);

			b_val = DirectFunctionCall2(float8div, DirectFunctionCall2(float8pl, m_val, mp1_val), num_two);

			f_lt_b = DatumGetBool(DirectFunctionCall2(float8le, f_val, b_val));

			if (f_lt_b){
				results[j * i_max + i] = DatumGetFloat8(
					DirectFunctionCall2(dpow,
					DirectFunctionCall2(float8mi, mp1_val, f_val),
					num_norm));
			}
			else {
				results[j * i_max + i] = DatumGetFloat8(
					DirectFunctionCall2(dpow,
					DirectFunctionCall2(float8mi, f_val, m_val),
					num_norm));
			}

			/* set mark pointer for next round */
			m_ptr = att_addlength_pointer(m_ptr, typlen, m_ptr);
			m_ptr = (char *)att_align_nominal(m_ptr, typalign);

			mp1_ptr = att_addlength_pointer(mp1_ptr, typlen, mp1_ptr);
			mp1_ptr = (char *)att_align_nominal(mp1_ptr, typalign);
		}
		results[j * i_max + i_max - 1] = results[j * i_max - 2];

		j++;
	}

	array_free_iterator(f_it);
	array_free_iterator(mfull_it);

	return results;
}




/*
 * Determines the single bound using the distances calculated in precompute_differences.
 *
 * (Weber, 2000, Section 5.5.4)
 */
static float8
get_bound(BitStringElement *apx, float8* differences, int32 dimensions, int32 partitions, MinkowskiNorm norm)
{
	int dim = 0;		/* dimensions */
	int apx_dim = 0;	/* approximation of dimension */

	float8 trans_result = 0;
	float8 difference;

	float8 result;

	MemoryContext old_ctx;
	MemoryContext ctx;

	//switch memory context
	ctx = AllocSetContextCreate(CurrentMemoryContext, "Marks build temporary context",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
	old_ctx = MemoryContextSwitchTo(ctx);

	if (norm < 100 && norm != MINKOWSKI_MAX_NORM){
		/* L_s norms, where s != Infinity */
		FmgrInfo in;

		fmgr_info(ARRAYINOID, &in);
		/* add up the difference for each dimension (Weber, 2000, Formula 5.5.3) */
		for (dim = 0; dim < dimensions; dim++){
			apx_dim = MIN(GET_WORD(apx, dim), partitions - 1);
			difference = differences[dim * partitions + apx_dim];
			trans_result = trans_result + difference;
		}

		/* because of monotonicity, root-function does not have to be used here */
		//return DirectFunctionCall1(numeric_sqrt, trans_result);
	}
	else if (norm == MINKOWSKI_MAX_NORM) {
		/* Maximum Norm, i.e. L_inf */

		/* get maximum (Weber, 2000, Formula 5.5.4) */
		for (dim = 0; dim < dimensions; dim++){
			apx_dim = MIN(GET_WORD(apx, dim), partitions - 1);
			difference = differences[dim * partitions + apx_dim];

			if (trans_result < difference){
				trans_result = difference;
			}
		}
	}

	MemoryContextSwitchTo(old_ctx);

	result = trans_result;

	MemoryContextDelete(ctx);

	return result;
}


/*
 * Sets the bits of a bit string correctly, given a feature and the marks.
 *
 * (Weber, 2000, Section 5.2.3)
 */
static void
set_bitstring(feature *f, ArrayType *marks, BitStringElement *result)
{
	ArrayIterator f_it;
	Datum		f_val;
	bool		f_isnull;

	ArrayIterator	mdim_it;
	Datum			mdim_val;
	bool			mdim_isnull;

	ArrayIterator	mmarks_it;
	Datum			mmarks_val;
	bool			mmarks_isnull;

	int			i = 0,
		j = 0;


	if (!f){
		/* throw true error here? */
		ereport(LOG, (errmsg("cannot establish bit string for empty feature")));
		return;
	}

	f_it = array_create_iterator(&f->data, 0);
	mdim_it = array_create_iterator(marks, 1);

	while (array_iterate(f_it, &f_val, &f_isnull) && array_iterate(mdim_it, &mdim_val, &mdim_isnull)){
		mmarks_it = array_create_iterator(DatumGetArrayTypeP(mdim_val), 0);

		j = 0;

		while (array_iterate(mmarks_it, &mmarks_val, &mmarks_isnull)){
			if (!f_isnull && !mmarks_isnull && !DatumGetBool(DirectFunctionCall2(float8lt, mmarks_val, f_val))){
				j--;
				goto setBits;
			}

			j++;
		}

	setBits:
		j = (j < 0) ? 0 : j;
		j = (j > 255) ? 255 : j;

		SET_BITS(result, i, j);

		i++;
	}

	array_free_iterator(f_it);
	array_free_iterator(mdim_it);
	array_free_iterator(mmarks_it);
}


/*
 *  description function for WAL/XLOG
 */
void
vaDesc(StringInfo buf, uint8 xl_info, char *rec)
{
}


/*
 *  redo function for WAL/XLOG
 */
void
vaRedo(XLogRecPtr lsn, XLogRecord *record)
{
	elog(PANIC, "va_redo: unimplemented");
}