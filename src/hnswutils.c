#include "postgres.h"

#include <math.h>

#include "hnsw.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "vector.h"

#if PG_VERSION_NUM < 130000
#define TYPSTORAGE_PLAIN 'p'
#endif

/*
 * Get the max number of connections in an upper layer for each element in the index
 */
int
HnswGetM(Relation index)
{
	HnswOptions *opts = (HnswOptions *) index->rd_options;

	if (opts)
		return opts->m;

	return HNSW_DEFAULT_M;
}

/*
 * Get the size of the dynamic candidate list in the index
 */
int
HnswGetEfConstruction(Relation index)
{
	HnswOptions *opts = (HnswOptions *) index->rd_options;

	if (opts)
		return opts->efConstruction;

	return HNSW_DEFAULT_EF_CONSTRUCTION;
}

/*
 * Get proc
 */
FmgrInfo *
HnswOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * Init procs
 */
FmgrInfo  **
HnswInitProcinfos(Relation index)
{
	int			keyAttributes = IndexRelationGetNumberOfKeyAttributes(index);
	FmgrInfo  **procinfos = palloc(keyAttributes * sizeof(FmgrInfo *));

	procinfos[0] = index_getprocinfo(index, 1, HNSW_DISTANCE_PROC);
	for (int i = 1; i < keyAttributes; i++)
		procinfos[i] = index_getprocinfo(index, i + 1, HNSW_ATTRIBUTE_DISTANCE_PROC);

	return procinfos;
}

/*
 * Divide by the norm
 *
 * Returns false if value should not be indexed
 *
 * The caller needs to free the pointer stored in value
 * if it's different than the original value
 */
bool
HnswNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result)
{
	double		norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	if (norm > 0)
	{
		Vector	   *v = DatumGetVector(*value);

		if (result == NULL)
			result = InitVector(v->dim);

		for (int i = 0; i < v->dim; i++)
			result->x[i] = v->x[i] / norm;

		*value = PointerGetDatum(result);

		return true;
	}

	return false;
}

/*
 * New buffer
 */
Buffer
HnswNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Init page
 */
void
HnswInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(HnswPageOpaqueData));
	HnswPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	HnswPageGetOpaque(page)->page_id = HNSW_PAGE_ID;
}

/*
 * Init and register page
 */
void
HnswInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	HnswInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
HnswCommitBuffer(Buffer buf, GenericXLogState *state)
{
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Allocate neighbors
 */
void
HnswInitNeighbors(HnswElement element, int m)
{
	int			level = element->level;

	element->neighbors = palloc(sizeof(HnswNeighborArray) * (level + 1));

	for (int lc = 0; lc <= level; lc++)
	{
		HnswNeighborArray *a;
		int			lm = HnswGetLayerM(m, lc);

		a = &element->neighbors[lc];
		a->length = 0;
		a->items = palloc(sizeof(HnswCandidate) * lm);
		a->closerSet = false;
	}
}

/*
 * Free neighbors
 */
static void
HnswFreeNeighbors(HnswElement element)
{
	for (int lc = 0; lc <= element->level; lc++)
		pfree(element->neighbors[lc].items);
	pfree(element->neighbors);
}

/*
 * Allocate an element
 */
HnswElement
HnswInitElement(ItemPointer heaptid, int m, double ml, int maxLevel)
{
	HnswElement element = palloc(sizeof(HnswElementData));

	int			level = (int) (-log(RandomDouble()) * ml);

	/* Cap level */
	if (level > maxLevel)
		level = maxLevel;

	element->heaptids = NIL;
	HnswAddHeapTid(element, heaptid);

	element->level = level;
	element->deleted = 0;
	element->itup = NULL;

	HnswInitNeighbors(element, m);

	return element;
}

/*
 * Free an element
 */
void
HnswFreeElement(HnswElement element)
{
	HnswFreeNeighbors(element);
	list_free_deep(element->heaptids);
	if (element->itup)
		pfree(element->itup);
	pfree(element);
}

/*
 * Add a heap TID to an element
 */
void
HnswAddHeapTid(HnswElement element, ItemPointer heaptid)
{
	ItemPointer copy = palloc(sizeof(ItemPointerData));

	ItemPointerCopy(heaptid, copy);
	element->heaptids = lappend(element->heaptids, copy);
}

/*
 * Allocate an element from block and offset numbers
 */
HnswElement
HnswInitElementFromBlock(BlockNumber blkno, OffsetNumber offno)
{
	HnswElement element = palloc(sizeof(HnswElementData));

	element->blkno = blkno;
	element->offno = offno;
	element->neighbors = NULL;
	element->value = PointerGetDatum(NULL);
	element->itup = NULL;
	return element;
}

/*
 * Get the metapage info
 */
void
HnswGetMetaPageInfo(Relation index, int *m, HnswElement * entryPoint)
{
	Buffer		buf;
	Page		page;
	HnswMetaPage metap;

	buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = HnswPageGetMeta(page);

	if (m != NULL)
		*m = metap->m;

	if (entryPoint != NULL)
	{
		if (BlockNumberIsValid(metap->entryBlkno))
			*entryPoint = HnswInitElementFromBlock(metap->entryBlkno, metap->entryOffno);
		else
			*entryPoint = NULL;
	}

	UnlockReleaseBuffer(buf);
}

/*
 * Get the entry point
 */
HnswElement
HnswGetEntryPoint(Relation index)
{
	HnswElement entryPoint;

	HnswGetMetaPageInfo(index, NULL, &entryPoint);

	return entryPoint;
}

/*
 * Update the metapage info
 */
static void
HnswUpdateMetaPageInfo(Page page, int updateEntry, HnswElement entryPoint, BlockNumber insertPage)
{
	HnswMetaPage metap = HnswPageGetMeta(page);

	if (updateEntry)
	{
		if (entryPoint == NULL)
		{
			metap->entryBlkno = InvalidBlockNumber;
			metap->entryOffno = InvalidOffsetNumber;
			metap->entryLevel = -1;
		}
		else if (entryPoint->level > metap->entryLevel || updateEntry == HNSW_UPDATE_ENTRY_ALWAYS)
		{
			metap->entryBlkno = entryPoint->blkno;
			metap->entryOffno = entryPoint->offno;
			metap->entryLevel = entryPoint->level;
		}
	}

	if (BlockNumberIsValid(insertPage))
		metap->insertPage = insertPage;
}

/*
 * Update the metapage
 */
void
HnswUpdateMetaPage(Relation index, int updateEntry, HnswElement entryPoint, BlockNumber insertPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	buf = ReadBufferExtended(index, forkNum, HNSW_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	HnswUpdateMetaPageInfo(page, updateEntry, entryPoint, insertPage);

	HnswCommitBuffer(buf, state);
}

/*
 * Set element tuple, except for neighbor info
 */
void
HnswSetElementTuple(HnswElementTuple etup, HnswElement element, bool useIndexTuple)
{
	etup->type = HNSW_ELEMENT_TUPLE_TYPE;
	etup->level = element->level;
	etup->deleted = 0;
	for (int i = 0; i < HNSW_HEAPTIDS; i++)
	{
		if (i < list_length(element->heaptids))
			etup->heaptids[i] = *((ItemPointer) list_nth(element->heaptids, i));
		else
			ItemPointerSetInvalid(&etup->heaptids[i]);
	}

	if (useIndexTuple)
		memcpy(&etup->data, element->itup, IndexTupleSize(element->itup));
	else
		memcpy(&etup->data, DatumGetPointer(element->value), VARSIZE_ANY(DatumGetPointer(element->value)));
}

/*
 * Set neighbor tuple
 */
void
HnswSetNeighborTuple(HnswNeighborTuple ntup, HnswElement e, int m)
{
	int			idx = 0;

	ntup->type = HNSW_NEIGHBOR_TUPLE_TYPE;

	for (int lc = e->level; lc >= 0; lc--)
	{
		HnswNeighborArray *neighbors = &e->neighbors[lc];
		int			lm = HnswGetLayerM(m, lc);

		for (int i = 0; i < lm; i++)
		{
			ItemPointer indextid = &ntup->indextids[idx++];

			if (i < neighbors->length)
			{
				HnswCandidate *hc = &neighbors->items[i];

				ItemPointerSet(indextid, hc->element->blkno, hc->element->offno);
			}
			else
				ItemPointerSetInvalid(indextid);
		}
	}

	ntup->count = idx;
}

/*
 * Load neighbors from page
 */
static void
LoadNeighborsFromPage(HnswElement element, Relation index, Page page, int m)
{
	HnswNeighborTuple ntup = (HnswNeighborTuple) PageGetItem(page, PageGetItemId(page, element->neighborOffno));
	int			neighborCount = (element->level + 2) * m;

	Assert(HnswIsNeighborTuple(ntup));

	HnswInitNeighbors(element, m);

	/* Ensure expected neighbors */
	if (ntup->count != neighborCount)
		return;

	for (int i = 0; i < neighborCount; i++)
	{
		HnswElement e;
		int			level;
		HnswCandidate *hc;
		ItemPointer indextid;
		HnswNeighborArray *neighbors;

		indextid = &ntup->indextids[i];

		if (!ItemPointerIsValid(indextid))
			continue;

		e = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid), ItemPointerGetOffsetNumber(indextid));

		/* Calculate level based on offset */
		level = element->level - i / m;
		if (level < 0)
			level = 0;

		neighbors = &element->neighbors[level];
		hc = &neighbors->items[neighbors->length++];
		hc->element = e;
	}
}

/*
 * Load neighbors
 */
void
HnswLoadNeighbors(HnswElement element, Relation index, int m)
{
	Buffer		buf;
	Page		page;

	buf = ReadBuffer(index, element->neighborPage);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	LoadNeighborsFromPage(element, index, page, m);

	UnlockReleaseBuffer(buf);
}

/*
 * Load an element from a tuple
 */
void
HnswLoadElementFromTuple(HnswElement element, HnswElementTuple etup, bool loadHeaptids, bool loadVec, Relation index)
{
	element->level = etup->level;
	element->deleted = etup->deleted;
	element->neighborPage = ItemPointerGetBlockNumber(&etup->neighbortid);
	element->neighborOffno = ItemPointerGetOffsetNumber(&etup->neighbortid);
	element->heaptids = NIL;

	if (loadHeaptids)
	{
		for (int i = 0; i < HNSW_HEAPTIDS; i++)
		{
			/* Can stop at first invalid */
			if (!ItemPointerIsValid(&etup->heaptids[i]))
				break;

			HnswAddHeapTid(element, &etup->heaptids[i]);
		}
	}

	if (loadVec)
	{
		if (IndexRelationGetNumberOfAttributes(index) > 1)
		{
			TupleDesc	tupdesc = RelationGetDescr(index);
			bool		unused;

			element->itup = CopyIndexTuple((IndexTuple) &etup->data);
			element->value = index_getattr(element->itup, 1, tupdesc, &unused);
		}
		else
		{
			Vector	   *vec = palloc(VARSIZE_ANY(&etup->data));

			memcpy(vec, &etup->data, VARSIZE_ANY(&etup->data));
			element->value = PointerGetDatum(vec);
		}
	}
}

/*
 * Get the tuple descriptor
 */
static TupleDesc
HnswTupleDesc(Relation index)
{
	TupleDesc	tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(index));

	/* Prevent compression */
	TupleDescAttr(tupdesc, 0)->attstorage = TYPSTORAGE_PLAIN;

	return tupdesc;
}

/*
 * Set element data
 */
void
HnswElementSetData(HnswElement element, Relation index, Datum value, Datum *values, bool *isnull)
{
	/* TODO Create once per index build */
	TupleDesc	tupdesc = HnswTupleDesc(index);
	bool		unused;
	Datum		tmp;

	tmp = values[0];
	values[0] = value;
	element->itup = index_form_tuple(tupdesc, values, isnull);
	values[0] = tmp;

	element->value = index_getattr(element->itup, 1, tupdesc, &unused);

	FreeTupleDesc(tupdesc);
}

/*
 * Get the attribute distance
 */
static inline double
AttributeDistance(double e)
{
	/* TODO Better bias */
	/* must be >> max(w * g) + 1 / log10(2) */
	double		bias = 4.32;

	return e > 0 ? bias - 1.0 / log10(e + 1) : 0;
}

/*
 * Get the distance
 */
static double
GetDistance(IndexTuple itup, Datum vec, Datum q, IndexTuple qtup, ScanKeyData *keyData, Relation index, FmgrInfo **procinfos, Oid *collations)
{
	double		g = DatumGetFloat8(FunctionCall2Coll(procinfos[0], collations[0], q, vec));

	if (IndexRelationGetNumberOfKeyAttributes(index) > 1)
	{
		double		w = 0.25;
		double		e = 0.0;
		TupleDesc	tupdesc = RelationGetDescr(index);

		if (keyData)
		{
			/* TODO need to pass length of key data */
			int			keyCount = 1;

			for (int i = 0; i < keyCount; i++)
			{
				ScanKey		key = &keyData[i];
				bool		isnull;
				Datum		value = index_getattr(itup, key->sk_attno, tupdesc, &isnull);
				bool		attnull = key->sk_flags & SK_ISNULL;

				if (isnull || attnull)
				{
					if (isnull != attnull)
						e += 1000;
				}
				else if (!DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation, value, key->sk_argument)))
				{
					double		ei = fabs(DatumGetFloat8(FunctionCall2Coll(procinfos[key->sk_attno - 1], collations[key->sk_attno - 1], value, key->sk_argument)));

					if (ei > 0)
						e += ei;
					else
						/* Distance is zero for inequality */
						e += 1000;
				}
			}

			return w * g + AttributeDistance(e);
		}
		else if (qtup)
		{
			int			keyCount = IndexRelationGetNumberOfKeyAttributes(index) - 1;

			for (int i = 0; i < keyCount; i++)
			{
				bool		isnull;
				bool		attnull;
				Datum		value = index_getattr(itup, i + 2, tupdesc, &isnull);
				Datum		value2 = index_getattr(qtup, i + 2, tupdesc, &attnull);

				if (isnull || attnull)
				{
					if (isnull != attnull)
						e += 1000;
				}
				else
					e += fabs(DatumGetFloat8(FunctionCall2Coll(procinfos[i + 1], collations[i + 1], value, value2)));
			}

			return w * g + AttributeDistance(e);
		}
	}

	return g;
}

/*
 * Load an element and optionally get its distance from q
 */
void
HnswLoadElement(HnswElement element, float *distance, Datum *q, IndexTuple qtup, ScanKeyData *keyData, Relation index, FmgrInfo **procinfos, Oid *collations, bool loadVec)
{
	Buffer		buf;
	Page		page;
	HnswElementTuple etup;

	/* Read vector */
	buf = ReadBuffer(index, element->blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	etup = (HnswElementTuple) PageGetItem(page, PageGetItemId(page, element->offno));

	Assert(HnswIsElementTuple(etup));

	/* Load element */
	HnswLoadElementFromTuple(element, etup, true, loadVec, index);

	/* Calculate distance */
	if (distance != NULL)
	{
		IndexTuple	itup = NULL;
		Datum		value;

		if (IndexRelationGetNumberOfAttributes(index) > 1)
		{
			TupleDesc	tupdesc = RelationGetDescr(index);
			bool		unused;

			itup = (IndexTuple) &etup->data;
			value = index_getattr(itup, 1, tupdesc, &unused);
		}
		else
			value = PointerGetDatum(&etup->data);

		*distance = GetDistance(itup, value, *q, qtup, keyData, index, procinfos, collations);
	}

	UnlockReleaseBuffer(buf);
}

/*
 * Get the distance for a candidate
 */
static float
GetCandidateDistance(HnswCandidate * hc, Datum q, IndexTuple qtup, ScanKeyData *keyData, Relation index, FmgrInfo **procinfos, Oid *collations)
{
	return GetDistance(hc->element->itup, hc->element->value, q, qtup, keyData, index, procinfos, collations);
}

/*
 * Create a candidate for the entry point
 */
HnswCandidate *
HnswEntryCandidate(HnswElement entryPoint, Datum q, IndexTuple qtup, ScanKeyData *keyData, Relation index, FmgrInfo **procinfos, Oid *collations, bool loadVec, bool inMemory)
{
	HnswCandidate *hc = palloc(sizeof(HnswCandidate));

	hc->element = entryPoint;
	if (inMemory)
		hc->distance = GetCandidateDistance(hc, q, qtup, keyData, index, procinfos, collations);
	else
		HnswLoadElement(hc->element, &hc->distance, &q, qtup, keyData, index, procinfos, collations, loadVec);
	return hc;
}

/*
 * Compare candidate distances
 */
static int
CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const HnswPairingHeapNode *) a)->inner->distance < ((const HnswPairingHeapNode *) b)->inner->distance)
		return 1;

	if (((const HnswPairingHeapNode *) a)->inner->distance > ((const HnswPairingHeapNode *) b)->inner->distance)
		return -1;

	return 0;
}

/*
 * Compare candidate distances
 */
static int
CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const HnswPairingHeapNode *) a)->inner->distance < ((const HnswPairingHeapNode *) b)->inner->distance)
		return -1;

	if (((const HnswPairingHeapNode *) a)->inner->distance > ((const HnswPairingHeapNode *) b)->inner->distance)
		return 1;

	return 0;
}

/*
 * Create a pairing heap node for a candidate
 */
static HnswPairingHeapNode *
CreatePairingHeapNode(HnswCandidate * c)
{
	HnswPairingHeapNode *node = palloc(sizeof(HnswPairingHeapNode));

	node->inner = c;
	return node;
}

/*
 * Add to visited
 */
static inline void
AddToVisited(HTAB *v, HnswCandidate * hc, bool inMemory, bool *found)
{
	if (inMemory)
		hash_search(v, &hc->element, HASH_ENTER, found);
	else
	{
		ItemPointerData indextid;

		ItemPointerSet(&indextid, hc->element->blkno, hc->element->offno);
		hash_search(v, &indextid, HASH_ENTER, found);
	}
}

/*
 * Algorithm 2 from paper
 */
List *
HnswSearchLayer(Datum q, IndexTuple qtup, ScanKeyData *keyData, List *ep, int ef, int lc, Relation index, FmgrInfo **procinfos, Oid *collations, int m, bool loadVec, HnswElement skipElement, bool inMemory)
{
	ListCell   *lc2;

	List	   *w = NIL;
	pairingheap *C = pairingheap_allocate(CompareNearestCandidates, NULL);
	pairingheap *W = pairingheap_allocate(CompareFurthestCandidates, NULL);
	int			wlen = 0;
	HASHCTL		hash_ctl;
	HTAB	   *v;

	/* Create hash table */
	if (inMemory)
	{
		hash_ctl.keysize = sizeof(HnswElement *);
		hash_ctl.entrysize = sizeof(HnswElement *);
	}
	else
	{
		hash_ctl.keysize = sizeof(ItemPointerData);
		hash_ctl.entrysize = sizeof(ItemPointerData);
	}

	hash_ctl.hcxt = CurrentMemoryContext;
	v = hash_create("hnsw visited", 256, &hash_ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Add entry points to v, C, and W */
	foreach(lc2, ep)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);

		AddToVisited(v, hc, inMemory, NULL);

		pairingheap_add(C, &(CreatePairingHeapNode(hc)->ph_node));
		pairingheap_add(W, &(CreatePairingHeapNode(hc)->ph_node));

		/*
		 * Do not count elements being deleted towards ef when vacuuming. It
		 * would be ideal to do this for inserts as well, but this could
		 * affect insert performance.
		 */
		if (skipElement == NULL || list_length(hc->element->heaptids) != 0)
			wlen++;
	}

	while (!pairingheap_is_empty(C))
	{
		HnswNeighborArray *neighborhood;
		HnswCandidate *c = ((HnswPairingHeapNode *) pairingheap_remove_first(C))->inner;
		HnswCandidate *f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;

		if (c->distance > f->distance)
			break;

		if (c->element->neighbors == NULL)
			HnswLoadNeighbors(c->element, index, m);

		/* Get the neighborhood at layer lc */
		neighborhood = &c->element->neighbors[lc];

		for (int i = 0; i < neighborhood->length; i++)
		{
			HnswCandidate *e = &neighborhood->items[i];
			bool		visited;

			AddToVisited(v, e, inMemory, &visited);

			if (!visited)
			{
				float		eDistance;

				f = ((HnswPairingHeapNode *) pairingheap_first(W))->inner;

				if (inMemory)
					eDistance = GetCandidateDistance(e, q, qtup, keyData, index, procinfos, collations);
				else
					HnswLoadElement(e->element, &eDistance, &q, qtup, keyData, index, procinfos, collations, loadVec);

				Assert(!e->element->deleted);

				/* Make robust to issues */
				if (e->element->level < lc)
					continue;

				if (eDistance < f->distance || wlen < ef)
				{
					/* Copy e */
					HnswCandidate *ec = palloc(sizeof(HnswCandidate));

					ec->element = e->element;
					ec->distance = eDistance;

					pairingheap_add(C, &(CreatePairingHeapNode(ec)->ph_node));
					pairingheap_add(W, &(CreatePairingHeapNode(ec)->ph_node));

					/*
					 * Do not count elements being deleted towards ef when
					 * vacuuming. It would be ideal to do this for inserts as
					 * well, but this could affect insert performance.
					 */
					if (skipElement == NULL || list_length(e->element->heaptids) != 0)
					{
						wlen++;

						/* No need to decrement wlen */
						if (wlen > ef)
							pairingheap_remove_first(W);
					}
				}
			}
		}
	}

	/* Add each element of W to w */
	while (!pairingheap_is_empty(W))
	{
		HnswCandidate *hc = ((HnswPairingHeapNode *) pairingheap_remove_first(W))->inner;

		w = lappend(w, hc);
	}

	return w;
}

/*
 * Compare candidate distances
 */
static int
#if PG_VERSION_NUM >= 130000
CompareCandidateDistances(const ListCell *a, const ListCell *b)
#else
CompareCandidateDistances(const void *a, const void *b)
#endif
{
	HnswCandidate *hca = lfirst((ListCell *) a);
	HnswCandidate *hcb = lfirst((ListCell *) b);

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	if (hca->element < hcb->element)
		return 1;

	if (hca->element > hcb->element)
		return -1;

	return 0;
}

/*
 * Calculate the distance between elements
 */
static float
HnswGetCachedDistance(HnswElement a, HnswElement b, int lc, Relation index, FmgrInfo **procinfos, Oid *collations)
{
	/* Look for cached distance */
	if (a->neighbors != NULL)
	{
		Assert(a->level >= lc);

		for (int i = 0; i < a->neighbors[lc].length; i++)
		{
			if (a->neighbors[lc].items[i].element == b)
				return a->neighbors[lc].items[i].distance;
		}
	}

	if (b->neighbors != NULL)
	{
		Assert(b->level >= lc);

		for (int i = 0; i < b->neighbors[lc].length; i++)
		{
			if (b->neighbors[lc].items[i].element == a)
				return b->neighbors[lc].items[i].distance;
		}
	}

	return GetDistance(a->itup, a->value, b->value, b->itup, NULL, index, procinfos, collations);
}

/*
 * Check if an element is closer to q than any element from R
 */
static bool
CheckElementCloser(HnswCandidate * e, List *r, int lc, Relation index, FmgrInfo **procinfos, Oid *collations)
{
	ListCell   *lc2;

	foreach(lc2, r)
	{
		HnswCandidate *ri = lfirst(lc2);
		float		distance = HnswGetCachedDistance(e->element, ri->element, lc, index, procinfos, collations);

		if (distance <= e->distance)
			return false;
	}

	return true;
}

/*
 * Algorithm 4 from paper
 */
static List *
SelectNeighbors(List *c, int m, int lc, Relation index, FmgrInfo **procinfos, Oid *collations, HnswElement e2, HnswCandidate * newCandidate, HnswCandidate * *pruned, bool sortCandidates)
{
	List	   *r = NIL;
	List	   *w = list_copy(c);
	pairingheap *wd;
	bool		mustCalculate = !e2->neighbors[lc].closerSet;
	List	   *added = NIL;
	bool		removedAny = false;

	if (list_length(w) <= m)
		return w;

	wd = pairingheap_allocate(CompareNearestCandidates, NULL);

	/* Ensure order of candidates is deterministic for closer caching */
	if (sortCandidates)
		list_sort(w, CompareCandidateDistances);

	while (list_length(w) > 0 && list_length(r) < m)
	{
		/* Assumes w is already ordered desc */
		HnswCandidate *e = llast(w);

		w = list_delete_last(w);

		/* Use previous state of r and wd to skip work when possible */
		if (mustCalculate)
			e->closer = CheckElementCloser(e, r, lc, index, procinfos, collations);
		else if (list_length(added) > 0)
		{
			/*
			 * If the current candidate was closer, we only need to compare it
			 * with the other candidates that we have added.
			 */
			if (e->closer)
			{
				e->closer = CheckElementCloser(e, added, lc, index, procinfos, collations);

				if (!e->closer)
					removedAny = true;
			}
			else
			{
				/*
				 * If we have removed any candidates from closer, a candidate
				 * that was not closer earlier might now be.
				 */
				if (removedAny)
				{
					e->closer = CheckElementCloser(e, r, lc, index, procinfos, collations);
					if (e->closer)
						added = lappend(added, e);
				}
			}
		}
		else if (e == newCandidate)
		{
			e->closer = CheckElementCloser(e, r, lc, index, procinfos, collations);
			if (e->closer)
				added = lappend(added, e);
		}

		if (e->closer)
			r = lappend(r, e);
		else
			pairingheap_add(wd, &(CreatePairingHeapNode(e)->ph_node));
	}

	/* Cached value can only be used in future if sorted deterministically */
	e2->neighbors[lc].closerSet = sortCandidates;

	/* Keep pruned connections */
	while (!pairingheap_is_empty(wd) && list_length(r) < m)
		r = lappend(r, ((HnswPairingHeapNode *) pairingheap_remove_first(wd))->inner);

	/* Return pruned for update connections */
	if (pruned != NULL)
	{
		if (!pairingheap_is_empty(wd))
			*pruned = ((HnswPairingHeapNode *) pairingheap_first(wd))->inner;
		else
			*pruned = linitial(w);
	}

	return r;
}

/*
 * Find duplicate element
 */
HnswElement
HnswFindDuplicate(HnswElement e, Relation index)
{
	HnswNeighborArray *neighbors = &e->neighbors[0];

	/* TODO Implement */
	if (IndexRelationGetNumberOfAttributes(index) > 1)
		return NULL;

	for (int i = 0; i < neighbors->length; i++)
	{
		HnswCandidate *neighbor = &neighbors->items[i];

		/* Exit early since ordered by distance */
		if (!datumIsEqual(e->value, neighbor->element->value, false, -1))
			break;

		/* Check for space */
		if (list_length(neighbor->element->heaptids) < HNSW_HEAPTIDS)
			return neighbor->element;
	}

	return NULL;
}

/*
 * Add connections
 */
static void
AddConnections(HnswElement element, List *neighbors, int m, int lc)
{
	ListCell   *lc2;
	HnswNeighborArray *a = &element->neighbors[lc];

	foreach(lc2, neighbors)
		a->items[a->length++] = *((HnswCandidate *) lfirst(lc2));
}

/*
 * Update connections
 */
void
HnswUpdateConnection(HnswElement element, HnswCandidate * hc, int m, int lc, int *updateIdx, Relation index, FmgrInfo **procinfos, Oid *collations, bool inMemory)
{
	HnswNeighborArray *currentNeighbors = &hc->element->neighbors[lc];

	HnswCandidate hc2;

	hc2.element = element;
	hc2.distance = hc->distance;

	if (currentNeighbors->length < m)
	{
		currentNeighbors->items[currentNeighbors->length++] = hc2;

		/* Track update */
		if (updateIdx != NULL)
			*updateIdx = -2;
	}
	else
	{
		/* Shrink connections */
		HnswCandidate *pruned = NULL;

		/* Load elements on insert */
		if (!inMemory)
		{
			Datum		q = hc->element->value;
			IndexTuple	qtup = hc->element->itup;
			ScanKeyData *keyData = NULL;

			for (int i = 0; i < currentNeighbors->length; i++)
			{
				HnswCandidate *hc3 = &currentNeighbors->items[i];

				if (DatumGetPointer(hc3->element->value) == NULL)
					HnswLoadElement(hc3->element, &hc3->distance, &q, qtup, keyData, index, procinfos, collations, true);
				else
					hc3->distance = GetCandidateDistance(hc3, q, qtup, keyData, index, procinfos, collations);

				/* Prune element if being deleted */
				if (list_length(hc3->element->heaptids) == 0)
				{
					pruned = &currentNeighbors->items[i];
					break;
				}
			}
		}

		if (pruned == NULL)
		{
			List	   *c = NIL;

			/* Add candidates */
			for (int i = 0; i < currentNeighbors->length; i++)
				c = lappend(c, &currentNeighbors->items[i]);
			c = lappend(c, &hc2);

			SelectNeighbors(c, m, lc, index, procinfos, collations, hc->element, &hc2, &pruned, true);

			/* Should not happen */
			if (pruned == NULL)
				return;
		}

		/* Find and replace the pruned element */
		for (int i = 0; i < currentNeighbors->length; i++)
		{
			if (currentNeighbors->items[i].element == pruned->element)
			{
				currentNeighbors->items[i] = hc2;

				/* Track update */
				if (updateIdx != NULL)
					*updateIdx = i;

				break;
			}
		}
	}
}

/*
 * Remove elements being deleted or skipped
 */
static List *
RemoveElements(List *w, HnswElement skipElement)
{
	ListCell   *lc2;
	List	   *w2 = NIL;

	foreach(lc2, w)
	{
		HnswCandidate *hc = (HnswCandidate *) lfirst(lc2);

		/* Skip self for vacuuming update */
		if (skipElement != NULL && hc->element->blkno == skipElement->blkno && hc->element->offno == skipElement->offno)
			continue;

		if (list_length(hc->element->heaptids) != 0)
			w2 = lappend(w2, hc);
	}

	return w2;
}

/*
 * Algorithm 1 from paper
 */
void
HnswInsertElement(HnswElement element, HnswElement entryPoint, Relation index, FmgrInfo **procinfos, Oid *collations, int m, int efConstruction, bool existing, bool inMemory)
{
	List	   *ep;
	List	   *w;
	int			level = element->level;
	int			entryLevel;
	Datum		q = element->value;
	IndexTuple	qtup = element->itup;
	ScanKeyData *keyData = NULL;
	HnswElement skipElement = existing ? element : NULL;

	/* No neighbors if no entry point */
	if (entryPoint == NULL)
		return;

	/* Get entry point and level */
	ep = list_make1(HnswEntryCandidate(entryPoint, q, qtup, keyData, index, procinfos, collations, true, inMemory));
	entryLevel = entryPoint->level;

	/* 1st phase: greedy search to insert level */
	for (int lc = entryLevel; lc >= level + 1; lc--)
	{
		w = HnswSearchLayer(q, qtup, keyData, ep, 1, lc, index, procinfos, collations, m, true, skipElement, inMemory);
		ep = w;
	}

	if (level > entryLevel)
		level = entryLevel;

	/* Add one for existing element */
	if (existing)
		efConstruction++;

	/* 2nd phase */
	for (int lc = level; lc >= 0; lc--)
	{
		int			lm = HnswGetLayerM(m, lc);
		List	   *neighbors;
		List	   *lw;

		w = HnswSearchLayer(q, qtup, keyData, ep, efConstruction, lc, index, procinfos, collations, m, true, skipElement, inMemory);

		/* Elements being deleted or skipped can help with search */
		/* but should be removed before selecting neighbors */
		if (!inMemory)
			lw = RemoveElements(w, skipElement);
		else
			lw = w;

		/*
		 * Candidates are sorted, but not deterministically. Could set
		 * sortCandidates to true for in-memory builds to enable closer
		 * caching, but there does not seem to be a difference in performance.
		 */
		neighbors = SelectNeighbors(lw, lm, lc, index, procinfos, collations, element, NULL, NULL, false);

		AddConnections(element, neighbors, lm, lc);

		ep = w;
	}
}
