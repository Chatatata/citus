/*-------------------------------------------------------------------------
 *
 * insert_select_executor.c
 *
 * Executor logic for INSERT..SELECT.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/transaction_management.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"


static void ExecuteSelectIntoRelation(Oid targetRelationId, List *insertTargetList,
									  Query *selectQuery, EState *executorState);
static void ExecuteQuery(Query *query, ParamListInfo params, DestReceiver *dest);


/*
 * CoordinatorInsertSelectExecScan executes an INSERT INTO distributed_table
 * SELECT .. query by setting up a DestReceiver that copies tuples into the
 * distributed table and then executing the SELECT query using that DestReceiver
 * as the tuple destination.
 */
TupleTableSlot *
CoordinatorInsertSelectExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		EState *executorState = scanState->customScanState.ss.ps.state;
		MultiPlan *multiPlan = scanState->multiPlan;
		Query *selectQuery = multiPlan->insertSelectQuery;
		List *insertTargetList = multiPlan->insertTargetList;
		Oid targetRelationId = multiPlan->targetRelationId;

		ereport(DEBUG1, (errmsg("Collecting INSERT ... SELECT results locally")));

		ExecuteSelectIntoRelation(targetRelationId, insertTargetList, selectQuery,
								  executorState);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * ExecuteSelectIntoRelation executes given SELECT query and inserts the
 * results into the target relation, which is assumed to be a distributed
 * table.
 */
static void
ExecuteSelectIntoRelation(Oid targetRelationId, List *insertTargetList,
						  Query *selectQuery, EState *executorState)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	ListCell *insertTargetCell = NULL;
	List *columnNameList = NIL;
	bool stopOnFailure = false;

	CitusCopyDestReceiver *copyDest = NULL;

	BeginOrContinueCoordinatedTransaction();

	/* build the list of column names for the COPY statement */
	foreach(insertTargetCell, insertTargetList)
	{
		TargetEntry *insertTargetEntry = (TargetEntry *) lfirst(insertTargetCell);

		columnNameList = lappend(columnNameList, insertTargetEntry->resname);
	}

	/* set up a DestReceiver that copies into the distributed table */
	copyDest = CreateCitusCopyDestReceiver(targetRelationId, columnNameList,
										   executorState, stopOnFailure);

	ExecuteQuery(selectQuery, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;
}


/*
 * ExecuteQuery plans and executes a query and sends results to the given
 * DestReceiver.
 */
static void
ExecuteQuery(Query *query, ParamListInfo params, DestReceiver *dest)
{
	PlannedStmt *queryPlan = NULL;
	Portal portal = NULL;
	int eflags = 0;
	long count = FETCH_ALL;

	/* create a new portal for executing the query */
	portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	/* plan the subquery, this may be another distributed query */
	queryPlan = pg_plan_query(query, 0, params);

	PortalDefineQuery(portal,
					  NULL,
					  "",
					  "SELECT",
					  list_make1(queryPlan),
					  NULL);

	PortalStart(portal, params, eflags, GetActiveSnapshot());
	PortalRun(portal, count, false, dest, dest, NULL);
	PortalDrop(portal, false);
}
