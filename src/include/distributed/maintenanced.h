/*-------------------------------------------------------------------------
 *
 * maintenanced.h
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MAINTENANCED_H
#define MAINTENANCED_H

void InitializeMaintenanceDaemon(void);
void InitializeMaintenanceDaemonBackend(void);

void CitusMaintenanceDaemonMain(Datum main_arg);

#endif /* MAINTENANCED_H */
