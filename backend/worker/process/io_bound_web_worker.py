import asyncio
import logging
from typing import cast
from uuid import UUID

from backend.db.session.factory import AsyncSessionFactory
from backend.lib.asset_manager.base import AssetManager
from backend.lib.asset_manager.factory import AssetManagerFactory
from backend.lib.job_manager.base import JobManager
from backend.lib.job_manager.types import JobQueue, JobType
from backend.lib.redis.client import RedisClient
from backend.worker.job_processor.factory import JobProcessorFactory
from backend.worker.job_processor.types import JobInputPayload, JobOutputPayload

from .base import WorkerProcess

MAX_JOB_TIMEOUT_SECS = 600  # 10 mins
POLL_SHUTDOWN_EVERY_SECS = 1


class IOBoundWebWorkerProcess(WorkerProcess):
    def _run_impl(self) -> None:
        redis = RedisClient()
        job_manager = JobManager(redis, JobQueue.MAIN_TASK_QUEUE)
        asset_manager = AssetManagerFactory().create()
        db_session_factory = AsyncSessionFactory()
        asyncio.run(self._main_loop(job_manager, asset_manager, db_session_factory))

    async def _mark_job_as_error(
        self,
        job_manager: JobManager,
        db_session_factory: AsyncSessionFactory,
        job_uuid: UUID,
        reason: str,
    ) -> None:
        try:
            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(
                    db_session,
                    job_uuid,
                    "error",
                    error_message=reason,
                )
        except Exception as inner:
            logging.warning(
                f"[{self.name}] Failed to mark job {job_uuid} as error: {inner}"
            )

    async def _main_loop(
        self,
        job_manager: JobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        logging.info(f"[{self.name}] Started worker process (PID={self.pid})")
        while True:
            # 1. Check for shutdown message
            if self.heartbeat_connection.poll(timeout=POLL_SHUTDOWN_EVERY_SECS):
                try:
                    msg = self.heartbeat_connection.recv()
                    if msg == "shutdown":
                        logging.info(f"[{self.name}] Received shutdown signal")
                        break
                except EOFError:
                    logging.warning(f"[{self.name}] Heartbeat pipe closed")
                    break

            try:
                job_uuid = await job_manager.poll(timeout=5)
                if job_uuid is None:
                    continue  # No job this cycle

                job_type, job_input_payload = None, None
                try:
                    async with db_session_factory.new_session() as db_session:
                        job_type, job_input_payload = await job_manager.claim(
                            db_session, job_uuid
                        )
                except Exception:
                    logging.exception(
                        f"[{self.name}] Job claim DB write failed for job: {job_uuid}"
                    )
                    await self._mark_job_as_error(
                        job_manager,
                        db_session_factory,
                        job_uuid,
                        "Failed to mark job as dequeued",
                    )

                if job_type is None or job_input_payload is None:
                    continue  # Was not successful in claiming the job

                try:
                    await asyncio.wait_for(
                        self._handle_task(
                            job_uuid,
                            job_type,
                            job_input_payload,
                            job_manager,
                            asset_manager,
                            db_session_factory,
                        ),
                        timeout=MAX_JOB_TIMEOUT_SECS,
                    )
                except asyncio.TimeoutError:
                    logging.warning(
                        f"[{self.name}] Job timed out after {MAX_JOB_TIMEOUT_SECS}s, "
                        f"job_id: {job_uuid} payload: {job_input_payload.model_dump_json()}"
                    )
                    await self._mark_job_as_error(
                        job_manager,
                        db_session_factory,
                        job_uuid,
                        f"Timeout after {MAX_JOB_TIMEOUT_SECS}s",
                    )
                except Exception as e:
                    logging.warning(
                        f"[{self.name}] Job failed: "
                        f"job_id: {job_uuid} payload: {job_input_payload.model_dump_json()}"
                    )
                    await self._mark_job_as_error(
                        job_manager,
                        db_session_factory,
                        job_uuid,
                        f"Job execution failed due to {str(e)}",
                    )
            except Exception as e:
                logging.exception(f"[{self.name}] Unexpected worker error: {e}")

        logging.info(f"[{self.name}] Exiting cleanly")

    async def _handle_task(
        self,
        job_uuid: UUID,
        job_type: JobType,
        job_input_payload: JobInputPayload,
        job_manager: JobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        try:
            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(db_session, job_uuid, "processing")

            job_processor = JobProcessorFactory.new_processor(
                job_uuid, job_type, asset_manager, db_session_factory
            )
            try:
                result = cast(
                    "JobOutputPayload", await job_processor.process(job_input_payload)
                )
            except Exception as e:
                logging.exception(
                    f"[{self.name}] Processor failed for job {job_uuid}: {e}"
                )
                raise e

            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(
                    db_session, job_uuid, "done", result_payload=result
                )

        except Exception as e:
            logging.warning(f"[{self.name}] Failed job {job_uuid}: {e}")
            raise e
