/*-------------------------------------------------------------------------
 *
 * insert_select_executor.h
 *
 * Declarations for public functions and types related to executing
 * INSERT..SELECT commands.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#pragma once


#include "executor/execdesc.h"


extern TupleTableSlot * CoordinatorInsertSelectExecScan(CustomScanState *node);
