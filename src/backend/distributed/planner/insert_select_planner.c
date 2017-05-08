/*-------------------------------------------------------------------------
 *
 * insert_select_planner.c
 *
 * Planning logic for INSERT..SELECT.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/insert_select_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "utils/lsyscache.h"


struct DecrementCTELevelsContext
{
	List *topLevelCTEList;
	int topLevelsUp;
};


static bool DecrementCTELevelsWalker(Node *node,
									 struct DecrementCTELevelsContext *context);


/*
 * CreatteCoordinatorInsertSelectPlan creates a query plan for a SELECT into a
 * distributed table.
 */
MultiPlan *
CreateCoordinatorInsertSelectPlan(Query *parse)
{
	Query *insertSelectQuery = copyObject(parse);

	RangeTblRef *reference = linitial(insertSelectQuery->jointree->fromlist);
	RangeTblEntry *subqueryRte = rt_fetch(reference->rtindex,
										  insertSelectQuery->rtable);
	RangeTblEntry *insertRte = rt_fetch(insertSelectQuery->resultRelation,
										insertSelectQuery->rtable);
	Oid targetRelationId = insertRte->relid;

	Query *subquery = (Query *) subqueryRte->subquery;

	ListCell *selectTargetCell = NULL;
	ListCell *insertTargetCell = NULL;

	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->operation = CMD_INSERT;

	if (list_length(insertSelectQuery->returningList) > 0)
	{
		multiPlan->planningError =
			DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
						  "RETURNING is not supported in INSERT ... SELECT via coordinator",
						  NULL, NULL);

		return multiPlan;
	}

	if (insertSelectQuery->onConflict)
	{
		multiPlan->planningError =
			DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
						  "ON CONFLICT is not supported in INSERT ... SELECT via coordinator",
						  NULL, NULL);
	}

	if (PartitionMethod(targetRelationId) == DISTRIBUTE_BY_APPEND)
	{
		multiPlan->planningError =
			DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
						  "INSERT ... SELECT into an append-distributed table is not supported",
						  NULL, NULL);
	}

	if (list_length(insertSelectQuery->cteList) > 0)
	{
		struct DecrementCTELevelsContext context;
		List *combinedCTEList = NIL;

		/* prepend top-level CTEs in subquery */
		combinedCTEList = list_concat(combinedCTEList, insertSelectQuery->cteList);
		combinedCTEList = list_concat(combinedCTEList, subquery->cteList);
		subquery->cteList = combinedCTEList;

		context.topLevelCTEList = insertSelectQuery->cteList;
		context.topLevelsUp = 0;

		/* correct ctelevelsup for range table entries that refer to top-level CTEs */
		DecrementCTELevelsWalker((Node *) subquery, &context);
	}

	ReorderInsertSelectTargetLists(insertSelectQuery, insertRte, subqueryRte);

	/* add casts when the SELECT output does not directly match the table */
	forboth(insertTargetCell, insertSelectQuery->targetList,
			selectTargetCell, subquery->targetList)
	{
		TargetEntry *insertTargetEntry = (TargetEntry *) lfirst(insertTargetCell);
		TargetEntry *selectTargetEntry = (TargetEntry *) lfirst(selectTargetCell);

		Var *columnVar = NULL;
		Oid columnType = InvalidOid;
		int32 columnTypeMod = 0;
		Oid selectOutputType = InvalidOid;

		/* indirection is not supported, e.g. INSERT INTO table (composite_column.x) */
		if (!IsA(insertTargetEntry->expr, Var))
		{
			ereport(ERROR, (errmsg("can only handle regular columns in the target "
								   "list")));
		}

		columnVar = (Var *) insertTargetEntry->expr;
		columnType = get_atttype(targetRelationId, columnVar->varattno);
		columnTypeMod = get_atttypmod(targetRelationId, columnVar->varattno);
		selectOutputType = columnVar->vartype;

		/*
		 * If the type in the target list does not match the type of the column,
		 * we need to cast to the column type. PostgreSQL would do this
		 * automatically during the insert, but we're passing the the SELECT
		 * output directly to COPY.
		 */
		if (columnType != selectOutputType)
		{
			Expr *selectExpression = selectTargetEntry->expr;
			Expr *typeCast = (Expr *) coerce_to_target_type(NULL,
															(Node *) selectExpression,
															selectOutputType,
															columnType,
															columnTypeMod,
															COERCION_EXPLICIT,
															COERCE_IMPLICIT_CAST,
															-1);

			selectTargetEntry->expr = typeCast;
		}
	}

	multiPlan->insertSelectQuery = subquery;
	multiPlan->insertTargetList = insertSelectQuery->targetList;
	multiPlan->targetRelationId = targetRelationId;

	return multiPlan;
}


/*
 * DecrementCTELevelsWalker recursively lowers the ctelevelsup of all
 * range table entries that refer to the top level CTE list in the
 * given context. This needs to be done after moving the list of CTEs
 * down one level.
 */
static bool
DecrementCTELevelsWalker(Node *node, struct DecrementCTELevelsContext *context)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
		if (rangeTableEntry->ctename != NULL &&
			rangeTableEntry->ctelevelsup == context->topLevelsUp)
		{
			ListCell *cteCell = NULL;

			foreach(cteCell, context->topLevelCTEList)
			{
				CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
				if (strcmp(cte->ctename, rangeTableEntry->ctename) == 0)
				{
					rangeTableEntry->ctelevelsup--;
					break;
				}
			}
		}
	}
	else if (IsA(node, Query))
	{
		/*
		 * One level deeper, so a ctelevelsup referring to a top-level CTE will
		 * be one higher.
		 */
		context->topLevelsUp++;

		walkIsComplete = query_tree_walker((Query *) node, DecrementCTELevelsWalker,
										   context, QTW_EXAMINE_RTES);

		context->topLevelsUp--;
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, DecrementCTELevelsWalker,
												context);
	}

	return walkIsComplete;
}
