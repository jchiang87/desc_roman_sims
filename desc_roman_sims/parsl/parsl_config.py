import logging
import parsl
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import (HighThroughputExecutor, ThreadPoolExecutor,
                             WorkQueueExecutor)
from parsl.launchers import AprunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import LocalProvider

#COMPUTE_NODES = 3000
#THETA_QUEUE = "R.LSSTADSP_DESC"
#WALLTIME = "00:25:00"
#ACCOUNT = "LSSTADSP_DESC"

aprun_overrides = """-cc depth -j 1 -d 64"""

def work_queue_executor(label="work_queue",
                        worker_options="--memory=192000",  # Theta KNL max (MB)
                        port=9000,
                        provider=None):
    return WorkQueueExecutor(
        label="work_queue",
        port=port,
        shared_fs=True,
        autolabel=False,
        max_retries=1,
        worker_options=worker_options,
        provider=provider)


def local_provider(nodes_per_block=1):
    provider_options = dict(nodes_per_block=nodes_per_block,
                            init_blocks=0,
                            min_blocks=0,
                            max_blocks=1,
                            parallelism=0,
                            cmd_timeout=300)
    return LocalProvider(**provider_options)


local_wq = work_queue_executor(worker_options="--memory=10000",
                               provider=local_provider())


#theta_htx_executor = HighThroughputExecutor(
#    label='worker-nodes',
#    address=address_by_hostname(),
#    worker_debug=True,
#    poll_period=5000,
#    cores_per_worker=64,
#    heartbeat_period=300,
#    heartbeat_threshold=1200,
#    provider=LocalProvider(
#        nodes_per_block=8,
#        init_blocks=1,
#        min_blocks=1,
#        max_blocks=1,
#        launcher=AprunLauncher(overrides=aprun_overrides)
#    ),
#)

local_executor = ThreadPoolExecutor(max_threads=4, label="submit-node")

_EXECUTORS = {
#    'theta_htx': [theta_htx_executor, local_executor],
#    'theta_wq': [theta_wq_executor, local_executor],
    'local_wq': [local_wq, local_executor],
    'local': [local_executor]
}


def load_parsl_config(cluster='local_wq', hub_port=None):
    config = Config(
        executors=_EXECUTORS[cluster],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=hub_port,
            resource_monitoring_interval=3*60,
        )
    )
    return parsl.load(config)
