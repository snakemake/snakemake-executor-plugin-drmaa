from dataclasses import dataclass, field
import os
from pathlib import Path
import shlex
from typing import List, Generator, Optional
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    args: Optional[str] = field(
        default=None,
        metadata={
            "help": "Args that shall be passed to each DRMAA job submission. "
            "Can be used to "
            "specify options of the underlying cluster system, "
            "thereby using the job properties name, rulename, input, "
            "output, params, wildcards, log, "
            "threads and dependencies, e.g.: "
            "'-pe threaded {threads}'. Note that ARGS must be given "
            "in quotes."
        },
    )
    log_dir: Optional[Path] = field(
        default=None,
        metadata={
            "help": "Directory in which stdout and stderr files of DRMAA"
            " jobs will be written. The value may be given as a relative path,"
            " in which case Snakemake will use the current invocation directory"
            " as the origin. If given, this will override any given '-o' and/or"
            " '-e' native specification. If not given, all DRMAA stdout and"
            " stderr files are written to the current working directory."
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=False,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        try:
            import drmaa
            from drmaa.errors import AlreadyActiveSessionException
        except RuntimeError as e:
            raise WorkflowError(f"Error loading drmaa support:\n{e}")
        self.session = drmaa.Session()
        self.drmaa_args = self.workflow.executor_settings.args
        self.drmaa_log_dir = self.workflow.executor_settings.log_dir
        self.suspended_msg = set()
        try:
            self.session.initialize()
        except AlreadyActiveSessionException:
            pass

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        jobscript = self.get_jobscript(job)
        self.write_jobscript(job, jobscript)

        try:
            drmaa_args = job.format_wildcards(self.drmaa_args or "")
        except AttributeError as e:
            raise WorkflowError(str(e), rule=job.rule)

        import drmaa

        if self.drmaa_log_dir:
            os.makedirs(self.drmaa_log_dir, exist_ok=True)

        job_info = SubmittedJobInfo(job, aux={"jobscript": jobscript})

        try:
            jt = self.session.createJobTemplate()
            jt.remoteCommand = jobscript
            jt.nativeSpecification = drmaa_args
            if self.drmaa_log_dir:
                jt.outputPath = ":" + self.drmaa_log_dir
                jt.errorPath = ":" + self.drmaa_log_dir
            jt.jobName = os.path.basename(jobscript)

            job_info.external_jobid = self.session.runJob(jt)
        except (
            drmaa.DeniedByDrmException,
            drmaa.InternalException,
            drmaa.InvalidAttributeValueException,
        ) as e:
            self.report_job_error(
                job_info,
                msg=f"Error submitting job (DRMAA error {e})",
            )

        self.logger.info(
            f"Submitted DRMAA job {job.jobid} with external jobid "
            f"{job_info.external_jobid}."
        )
        self.report_job_submission(job_info)
        self.session.deleteJobTemplate(jt)

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(job).
        # For jobs that have errored, you have to call
        # self.report_job_error(job).
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        import drmaa

        for active_job in active_jobs:
            jobscript = active_job.aux["jobscript"]
            async with self.status_rate_limiter:
                try:
                    retval = self.session.jobStatus(active_job.external_jobid)
                except drmaa.ExitTimeoutException:
                    # job still active
                    yield active_job
                    continue
                except (drmaa.InternalException, Exception) as e:
                    self.logger.error(f"DRMAA Error: {e}")
                    os.remove(jobscript)
                    self.report_job_error(active_job)
                    continue
                if retval == drmaa.JobState.DONE:
                    os.remove(jobscript)
                    self.report_job_success(active_job)
                elif retval == drmaa.JobState.FAILED:
                    os.remove(jobscript)
                    self.report_job_error(active_job)
                else:
                    # still running
                    yield active_job

                    def handle_suspended(by):
                        if active_job.job.jobid not in self.suspended_msg:
                            self.logger.warning(
                                "Job {} (DRMAA id: {}) was suspended by {}.".format(
                                    active_job.job.jobid, active_job.jobid, by
                                )
                            )
                            self.suspended_msg.add(active_job.job.jobid)

                    if retval == drmaa.JobState.USER_SUSPENDED:
                        handle_suspended("user")
                    elif retval == drmaa.JobState.SYSTEM_SUSPENDED:
                        handle_suspended("system")
                    else:
                        try:
                            self.suspended_msg.remove(active_job.job.jobid)
                        except KeyError:
                            # there was nothing to remove
                            pass

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        from drmaa.const import JobControlAction
        from drmaa.errors import InvalidJobException, InternalException

        for job_info in active_jobs:
            try:
                self.session.control(
                    job_info.external_jobid, JobControlAction.TERMINATE
                )
            except (InvalidJobException, InternalException):
                # This is common - logging a warning would probably confuse the user.
                pass

    def shutdown(self):
        super().shutdown()
        self.session.exit()

    def get_job_exec_prefix(self, job: JobExecutorInterface):
        if self.workflow.storage_settings.assume_common_workdir:
            return f"cd {shlex.quote(self.workflow.workdir_init)}"
        else:
            return ""
