from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_drmaa import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsLocalStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "drmaa"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings()
