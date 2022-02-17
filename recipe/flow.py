import os
import yaml
import datetime
from pathlib import Path
from prefect import Flow
from prefect.schedules import CronSchedule
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.run_configs.ecs import ECSRun
from prefect.storage.docker import Docker
from ooi_harvester.settings.main import harvest_settings

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
CONFIG_PATH = BASE.joinpath(harvest_settings.github.defaults.config_path_str)
RUN_OPTIONS = {
    'env': {
        'PREFECT__CLOUD__HEARTBEAT_MODE': 'thread',
    },
    'cpu': '2 vcpu',
    'memory': '16 GB',
    'labels': ['ecs-agent', 'ooi', 'prod'],
    'run_task_kwargs': {
        'cluster': 'prefectECSCluster',
        'launchType': 'FARGATE',
    },
}

project_name = "ooi-harvest"
data_org = "ooi-data"
config_json = yaml.safe_load(CONFIG_PATH.open())
flow_run_name = "-".join(
    [
        config_json['instrument'],
        config_json['stream']['method'],
        config_json['stream']['name'],
    ]
)
schedule = CronSchedule(config_json['workflow_config']['schedule'])
run_config = ECSRun(**RUN_OPTIONS)

parent_run_opts = dict(**RUN_OPTIONS)
parent_run_opts.update({'cpu': '0.5 vcpu', 'memory': '2 GB'})
parent_run_config = ECSRun(**parent_run_opts)

with Flow(
    flow_run_name, schedule=schedule, run_config=parent_run_config
) as parent_flow:
    flow_run = create_flow_run(
        flow_name="stream_harvest",
        run_name=flow_run_name,
        project_name=project_name,
        parameters={
            'config': config_json,
            'error_test': False,
            'export_da': True,
            'gh_write_da': True,
        },
        run_config=run_config,
    )
    wait_for_flow = wait_for_flow_run(flow_run, raise_final_state=True)  # noqa

now = datetime.datetime.utcnow()
image_registry = "cormorack"
image_name = "harvest"
image_tag = f"{flow_run_name}.{now:%Y%m%dT%H%M}"

parent_flow.storage = Docker(
    registry_url=image_registry,
    dockerfile=HERE.joinpath("Dockerfile"),
    image_name=image_name,
    prefect_directory="/home/jovyan/prefect",
    env_vars={'HARVEST_ENV': 'ooi-harvester'},
    python_dependencies=[
        'git+https://github.com/ooi-data/ooi-harvester.git@main'
    ],
    image_tag=image_tag,
    build_kwargs={
        'nocache': True,
        'buildargs': {'PYTHON_VERSION': os.environ.get('PYTHON_VERSION', 3.8)},
    },
)
