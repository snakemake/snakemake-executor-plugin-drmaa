from pathlib import Path
from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_cluster_generic import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase
from snakemake_interface_common.exceptions import WorkflowError


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    __test__ = True

    def get_executor(self) -> str:
        return "drmaa"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings()

    def get_default_remote_provider(self) -> Optional[str]:
        return None

    def get_default_remote_prefix(self) -> Optional[str]:
        return None