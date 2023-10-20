import logging
import parsl
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import WorkQueueExecutor, ThreadPoolExecutor
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import LocalProvider


__all__ = ["load_wq_config"]


local_executor = ThreadPoolExecutor(max_threads=4, label="submit-node")


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


def load_wq_config(memory=182000, port=9000, hub_port=None):
    provider = local_provider()
    worker_options = f"--memory={memory}"
    executors = [work_queue_executor(worker_options=worker_options,
                                     port=port,
                                     provider=provider),
                 local_executor]
    config = Config(
        executors=executors,
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=hub_port,
            resource_monitoring_interval=3*60,
        )
    )
    return parsl.load(config)
