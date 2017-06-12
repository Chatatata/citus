/*-------------------------------------------------------------------------
 *
 * maintenanced.c
 *
 * This file provides infrastructure for launching exactly one a background
 * worker for every database in which citus is used.  That background worker
 * can then perform work like deadlock detection and cleanup.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "miscadmin.h"

#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "tcop/tcopprot.h"

typedef struct MaintenanceDaemonControlData
{
	int trancheId;
	LWLockTranche lockTranche;
	LWLock lock;

	HTAB *dbHash;
} MaintenanceDaemonControlData;

typedef struct MaintenanceDaemonDBData
{
	Oid databaseOid;
	bool daemonStarted;
} MaintenanceDaemonDBData;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static MaintenanceDaemonControlData *MaintenanceDaemonControl = NULL;

static volatile sig_atomic_t got_SIGHUP = false;

static void MaintenanceDaemonSigHupHandler(SIGNAL_ARGS);
static size_t MaintenanceDaemonShmemSize(void);
static void MaintenanceDaemonShmemInit(void);

void
InitializeMaintenanceDaemon(void)
{
	RequestAddinShmemSpace(MaintenanceDaemonShmemSize());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = MaintenanceDaemonShmemInit;
}


void
InitializeMaintenanceDaemonBackend(void)
{
	MaintenanceDaemonDBData *dbData = NULL;
	bool found;

	LWLockAcquire(&MaintenanceDaemonControl->lock, LW_EXCLUSIVE);

	dbData = (MaintenanceDaemonDBData *) hash_search(MaintenanceDaemonControl->dbHash, &MyDatabaseId,
											HASH_ENTER_NULL, &found);

	if (dbData == NULL)
	{
		/* FIXME: better message, reference relevant guc in hint */
		ereport(ERROR, (errmsg("ran out of database slots")));
	}

	if (!found || !dbData->daemonStarted)
	{
		BackgroundWorker worker;
		BackgroundWorkerHandle *handle;
		int pid;
		Oid extensionOwner = CitusExtensionOwner();

		elog(WARNING, "got to start daemon: old entry: %d", dbData->daemonStarted);

		memset(&worker, 0, sizeof(worker));
		sprintf(worker.bgw_name, "Citus Maintenance Daemon: %u", MyDatabaseId);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 5;
		sprintf(worker.bgw_library_name, "citus");
		sprintf(worker.bgw_function_name, "CitusMaintenanceDaemonMain");
		worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);
		memcpy(worker.bgw_extra, &extensionOwner, sizeof(Oid));
		worker.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		{
			/* FIXME: reference gucs etc */
			ereport(ERROR, (errmsg("could not start maintenance background worker")));
		}

		dbData->daemonStarted = true;
		LWLockRelease(&MaintenanceDaemonControl->lock);

		WaitForBackgroundWorkerStartup(handle, &pid);
	}
	else
	{
		Assert(dbData->daemonStarted);
		LWLockRelease(&MaintenanceDaemonControl->lock);
	}
}


void
CitusMaintenanceDaemonMain(Datum main_arg)
{
	Oid databaseOid = DatumGetObjectId(main_arg);
	Oid extensionOwner = 0;

	memcpy(&extensionOwner, MyBgworkerEntry->bgw_extra, sizeof(Oid));

	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, MaintenanceDaemonSigHupHandler);

	BackgroundWorkerUnblockSignals();

	elog(LOG, "starting maintenance daemon on database %u user %u",
		 databaseOid, extensionOwner);

	BackgroundWorkerInitializeConnectionByOid(databaseOid, extensionOwner);

	/* Enter main loop */
	for (;;)
	{
		int			rc;
		int	latchFlags = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		int timeout = 5000;

		elog(LOG, "doing check cycle on database %u", MyDatabaseId);


		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch, latchFlags, timeout);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			/* interrupts will be checked at the beginning of the loop */
		}

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
}


static size_t
MaintenanceDaemonShmemSize(void)
{
	Size size = 0;
	Size hashSize = 0;

	size = add_size(size, sizeof(MaintenanceDaemonControlData));
	hashSize = hash_estimate_size(max_worker_processes, sizeof(MaintenanceDaemonDBData));
	size = add_size(size, hashSize);

	return size;
}


static void
MaintenanceDaemonShmemInit(void)
{
	bool alreadyInitialized = false;
	HASHCTL hashInfo;
	int hashFlags = 0;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	MaintenanceDaemonControl =
		(MaintenanceDaemonControlData *) ShmemInitStruct("Citus Maintenance Daemon",
														 MaintenanceDaemonShmemSize(),
														 &alreadyInitialized);
	if (!alreadyInitialized)
	{
		/* initialize lwlock  */
		LWLockTranche *tranche = &MaintenanceDaemonControl->lockTranche;

		/* start by zeroing out all the memory */
		memset(MaintenanceDaemonControl, 0, MaintenanceDaemonShmemSize());

		/* initialize lock */
		MaintenanceDaemonControl->trancheId = LWLockNewTrancheId();
		tranche->array_base = &MaintenanceDaemonControl->lock;
		tranche->array_stride = sizeof(LWLock);
		tranche->name = "Citus Maintenance Daemon";
		LWLockRegisterTranche(MaintenanceDaemonControl->trancheId, tranche);
		LWLockInitialize(&MaintenanceDaemonControl->lock,
						 MaintenanceDaemonControl->trancheId);

	}


	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(Oid);
	hashInfo.entrysize = sizeof(MaintenanceDaemonDBData);
	hashInfo.hash = tag_hash;
	hashFlags = (HASH_ELEM | HASH_FUNCTION);

	MaintenanceDaemonControl->dbHash =
		ShmemInitHash("Maintenance Database Hash",
					  max_worker_processes, max_worker_processes,
					  &hashInfo, hashFlags);

	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * MaintenanceDaemonSigHupHandler set a flag to re-read config file at next
 * convenient time.
 */
static void
MaintenanceDaemonSigHupHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	if (MyProc != NULL)
	{
		SetLatch(&MyProc->procLatch);
	}

	errno = save_errno;
}
