#include "postgres.h"

#include <math.h>

#include "catalog/index.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "lib/pairingheap.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_TUPLES_DONE 0
#endif

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

#if PG_VERSION_NUM >= 120000
#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
#else
#define UpdateProgress(index, val) ((void)val)
#endif

/*
 * Create the metapage
 */
static void
CreateMetaPage(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswMetaPage metap;

	buf = HnswNewBuffer(index, forkNum);
	HnswInitRegisterPage(index, &buf, &page, &state);

	/* Set metapage data */
	metap = HnswPageGetMeta(page);
	metap->magicNumber = HNSW_MAGIC_NUMBER;
	metap->version = HNSW_VERSION;
	metap->dimensions = buildstate->dimensions;
	metap->m = buildstate->m;
	metap->efConstruction = buildstate->efConstruction;
	metap->entryBlkno = InvalidBlockNumber;
	metap->entryOffno = InvalidOffsetNumber;
	metap->entryLevel = -1;
	metap->insertPage = InvalidBlockNumber;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(HnswMetaPageData)) - (char *) page;

	HnswCommitBuffer(buf, state);
}

/*
 * Add a new page
 */
static void
HnswBuildAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	/* Add a new page */
	Buffer		newbuf = HnswNewBuffer(index, forkNum);

	/* Update previous page */
	HnswPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Commit */
	GenericXLogFinish(*state);
	UnlockReleaseBuffer(*buf);

	/* Can take a while, so ensure we can interrupt */
	/* Needs to be called when no buffer locks are held */
	LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
	CHECK_FOR_INTERRUPTS();
	LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

	/* Prepare new page */
	*buf = newbuf;
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*buf, *page);
}

/*
 * Create element pages
 */
static void
CreateElementPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	bool		useIndexTuple = buildstate->useIndexTuple;
	Size		etupAllocSize;
	Size		maxSize;
	HnswElementTuple etup;
	HnswNeighborTuple ntup;
	BlockNumber insertPage;
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ListCell   *lc;

	/* Calculate sizes */
	etupAllocSize = BLCKSZ;
	maxSize = HNSW_MAX_SIZE;

	/* Allocate once */
	etup = palloc0(etupAllocSize);
	ntup = palloc0(BLCKSZ);

	/* Prepare first page */
	buf = HnswNewBuffer(index, forkNum);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(buf, page);

	foreach(lc, buildstate->elements)
	{
		HnswElement element = lfirst(lc);
		Size		etupSize;
		Size		ntupSize;
		Size		combinedSize;

		/* Zero memory for each element */
		MemSet(etup, 0, etupAllocSize);

		/* Calculate sizes */
		etupSize = HNSW_ELEMENT_TUPLE_SIZE(useIndexTuple ? IndexTupleSize(element->itup) : VARSIZE_ANY(DatumGetPointer(element->value)));
		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(element->level, buildstate->m);
		combinedSize = etupSize + ntupSize + sizeof(ItemIdData);

		/* Initial size check */
		if (etupSize > etupAllocSize)
			elog(ERROR, "index tuple too large");

		HnswSetElementTuple(etup, element, useIndexTuple);

		/* Keep element and neighbors on the same page if possible */
		if (PageGetFreeSpace(page) < etupSize || (combinedSize <= maxSize && PageGetFreeSpace(page) < combinedSize))
			HnswBuildAppendPage(index, &buf, &page, &state, forkNum);

		/* Calculate offsets */
		element->blkno = BufferGetBlockNumber(buf);
		element->offno = OffsetNumberNext(PageGetMaxOffsetNumber(page));
		if (combinedSize <= maxSize)
		{
			element->neighborPage = element->blkno;
			element->neighborOffno = OffsetNumberNext(element->offno);
		}
		else
		{
			element->neighborPage = element->blkno + 1;
			element->neighborOffno = FirstOffsetNumber;
		}

		ItemPointerSet(&etup->neighbortid, element->neighborPage, element->neighborOffno);

		/* Add element */
		if (PageAddItem(page, (Item) etup, etupSize, InvalidOffsetNumber, false, false) != element->offno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Add new page if needed */
		if (PageGetFreeSpace(page) < ntupSize)
			HnswBuildAppendPage(index, &buf, &page, &state, forkNum);

		/* Add placeholder for neighbors */
		if (PageAddItem(page, (Item) ntup, ntupSize, InvalidOffsetNumber, false, false) != element->neighborOffno)
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));
	}

	insertPage = BufferGetBlockNumber(buf);

	/* Commit */
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	HnswUpdateMetaPage(index, HNSW_UPDATE_ENTRY_ALWAYS, buildstate->entryPoint, insertPage, forkNum);

	pfree(etup);
	pfree(ntup);
}

/*
 * Create neighbor pages
 */
static void
CreateNeighborPages(HnswBuildState * buildstate)
{
	Relation	index = buildstate->index;
	ForkNumber	forkNum = buildstate->forkNum;
	int			m = buildstate->m;
	ListCell   *lc;
	HnswNeighborTuple ntup;

	/* Allocate once */
	ntup = palloc0(BLCKSZ);

	foreach(lc, buildstate->elements)
	{
		HnswElement e = lfirst(lc);
		Buffer		buf;
		Page		page;
		GenericXLogState *state;
		Size		ntupSize = HNSW_NEIGHBOR_TUPLE_SIZE(e->level, m);

		/* Can take a while, so ensure we can interrupt */
		/* Needs to be called when no buffer locks are held */
		CHECK_FOR_INTERRUPTS();

		buf = ReadBufferExtended(index, forkNum, e->neighborPage, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, 0);

		HnswSetNeighborTuple(ntup, e, m);

		if (!PageIndexTupleOverwrite(page, e->neighborOffno, (Item) ntup, ntupSize))
			elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

		/* Commit */
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
	}

	pfree(ntup);
}

/*
 * Free elements
 */
static void
FreeElements(HnswBuildState * buildstate)
{
	ListCell   *lc;

	foreach(lc, buildstate->elements)
		HnswFreeElement(lfirst(lc));

	list_free(buildstate->elements);
}

/*
 * Flush pages
 */
static void
FlushPages(HnswBuildState * buildstate)
{
	CreateMetaPage(buildstate);
	CreateElementPages(buildstate);
	CreateNeighborPages(buildstate);

	buildstate->flushed = true;
	FreeElements(buildstate);
}

/*
 * Insert tuple
 */
static bool
InsertTuple(Relation index, Datum *values, bool *isnull, HnswElement element, HnswBuildState * buildstate, HnswElement * dup, MemoryContext outerCtx)
{
	FmgrInfo  **procinfos = buildstate->procinfos;
	Oid		   *collations = buildstate->collations;
	HnswElement entryPoint = buildstate->entryPoint;
	int			efConstruction = buildstate->efConstruction;
	int			m = buildstate->m;
	bool		inMemory = true;
	MemoryContext oldCtx;

	/* Detoast once for all calls */
	Datum		value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

	/* Normalize if needed */
	if (buildstate->normprocinfo != NULL)
	{
		if (!HnswNormValue(buildstate->normprocinfo, collations[0], &value, buildstate->normvec))
			return false;
	}

	/* Copy value to element so accessible outside of memory context */
	oldCtx = MemoryContextSwitchTo(outerCtx);
	HnswElementSetData(element, index, value, values, isnull);
	MemoryContextSwitchTo(oldCtx);

	/* Insert element in graph */
	HnswInsertElement(element, entryPoint, index, procinfos, collations, m, efConstruction, false, inMemory);

	/* Look for duplicate */
	*dup = HnswFindDuplicate(element, index);

	/* Update neighbors if needed */
	if (*dup == NULL)
	{
		for (int lc = element->level; lc >= 0; lc--)
		{
			int			lm = HnswGetLayerM(m, lc);
			HnswNeighborArray *neighbors = &element->neighbors[lc];

			for (int i = 0; i < neighbors->length; i++)
				HnswUpdateConnection(element, &neighbors->items[i], lm, lc, NULL, index, procinfos, collations, inMemory);
		}
	}

	/* Update entry point if needed */
	if (*dup == NULL && (entryPoint == NULL || element->level > entryPoint->level))
		buildstate->entryPoint = element;

	UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

	return *dup == NULL;
}

/*
 * Get the memory used by an element
 */
static long
HnswElementMemory(HnswElement e, int m)
{
	long		elementSize = sizeof(HnswElementData);

	elementSize += sizeof(HnswNeighborArray) * (e->level + 1);
	elementSize += sizeof(HnswCandidate) * (m * (e->level + 2));
	elementSize += sizeof(ItemPointerData);
	elementSize += IndexTupleSize(e->itup);
	return elementSize;
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	HnswBuildState *buildstate = (HnswBuildState *) state;
	MemoryContext oldCtx;
	HnswElement element;
	HnswElement dup = NULL;
	bool		inserted;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	/* Skip nulls */
	if (isnull[0])
		return;

	if (buildstate->memoryLeft <= 0)
	{
		if (!buildstate->flushed)
		{
			ereport(NOTICE,
					(errmsg("hnsw graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples", (int64) buildstate->indtuples),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up builds.")));

			FlushPages(buildstate);
		}

		oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

		if (HnswInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);

		return;
	}

	/* Allocate necessary memory outside of memory context */
	element = HnswInitElement(tid, buildstate->m, buildstate->ml, buildstate->maxLevel);

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	/* Insert tuple */
	inserted = InsertTuple(index, values, isnull, element, buildstate, &dup, oldCtx);

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);

	/* Add outside memory context */
	if (dup != NULL)
	{
		HnswAddHeapTid(dup, tid);
		buildstate->memoryLeft -= sizeof(ItemPointerData);
	}

	/* Add to buildstate or free */
	if (inserted)
	{
		buildstate->elements = lappend(buildstate->elements, element);
		buildstate->memoryLeft -= HnswElementMemory(element, buildstate->m);
	}
	else
		HnswFreeElement(element);
}

/*
 * Initialize the build state
 */
static void
InitBuildState(HnswBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo, ForkNumber forkNum)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	buildstate->m = HnswGetM(index);
	buildstate->efConstruction = HnswGetEfConstruction(index);
	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* TODO See if needed */
	if (IndexRelationGetNumberOfKeyAttributes(index) > 2)
		elog(ERROR, "index cannot have more than two columns");

	if (!OidIsValid(index_getprocid(index, 1, HNSW_DISTANCE_PROC)))
		elog(ERROR, "first column must be a vector");

	for (int i = 1; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
	{
		if (!OidIsValid(index_getprocid(index, i + 1, HNSW_ATTRIBUTE_DISTANCE_PROC)))
			elog(ERROR, "column %d cannot be a vector", i + 1);
	}

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > HNSW_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for hnsw index", HNSW_MAX_DIM);

	if (buildstate->efConstruction < 2 * buildstate->m)
		elog(ERROR, "ef_construction must be greater than or equal to 2 * m");

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfos = HnswInitProcinfos(index);
	buildstate->normprocinfo = HnswOptionalProcInfo(index, HNSW_NORM_PROC);
	buildstate->collations = index->rd_indcollation;

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->ml = HnswGetMl(buildstate->m);
	buildstate->maxLevel = HnswGetMaxLevel(buildstate->m);
	buildstate->memoryLeft = maintenance_work_mem * 1024L;
	buildstate->flushed = false;
	buildstate->useIndexTuple = IndexRelationGetNumberOfAttributes(index) > 1;

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Hnsw build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void
FreeBuildState(HnswBuildState * buildstate)
{
	pfree(buildstate->procinfos);
	pfree(buildstate->normvec);
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build graph
 */
static void
BuildGraph(HnswBuildState * buildstate, ForkNumber forkNum)
{
	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_HNSW_PHASE_LOAD);

#if PG_VERSION_NUM >= 120000
	buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, true, BuildCallback, (void *) buildstate, NULL);
#else
	buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
											   true, BuildCallback, (void *) buildstate, NULL);
#endif
}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   HnswBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo, forkNum);

	if (buildstate->heap != NULL)
		BuildGraph(buildstate, forkNum);

	if (!buildstate->flushed)
		FlushPages(buildstate);

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
hnswbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	HnswBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void
hnswbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	HnswBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
