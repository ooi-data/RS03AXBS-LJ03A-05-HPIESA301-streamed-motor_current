from pathlib import Path
import os
import json
import datetime
import argparse
import subprocess
import yaml

from ooi_harvester.producer.models import StreamHarvest
from ooi_harvester.processor.pipeline import OOIStreamPipeline
from ooi_harvester.processor.state_handlers import process_status_update

from ooi_harvester.config import (
    CONFIG_PATH_STR,
    RESPONSE_PATH_STR,
    PROCESS_STATUS_PATH_STR,
)
from ooi_harvester.utils.github import (
    get_process_status_json,
    write_process_status_json,
)

HERE = Path(__file__).parent.absolute()
BASE = HERE.parent.absolute()
CONFIG_PATH = BASE.joinpath(CONFIG_PATH_STR)
RESPONSE_PATH = BASE.joinpath(RESPONSE_PATH_STR)
PROCESS_STATUS_PATH = BASE.joinpath(PROCESS_STATUS_PATH_STR)

IMAGE_REGISTRY = "cormorack"
IMAGE_NAME = "ooi-harvester"


def parse_args():
    parser = argparse.ArgumentParser(description='Register harvest pipeline')
    parser.add_argument(
        '--path',
        type=str,
        default="s3://ooi-data",
        help='Bucket url where data is stored. Default is s3://ooi-data',
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help="Testing flag. If activated, actual harvest is skipped.",
    )
    parser.add_argument(
        '--refresh',
        action='store_true',
        help="Refresh flag. Set to true to refresh data stream.",
    )
    parser.add_argument(
        '--prefect-project',
        type=str,
        default='tutorial',
        help="Prefect project name",
    )
    parser.add_argument(
        '--run-flow',
        action='store_true',
        help="Run flow flag. Actually run the flow.",
    )

    return parser.parse_args()


def main(test_run, refresh, data_bucket, project_name, run_flow):
    response = json.load(RESPONSE_PATH.open())
    config_json = yaml.load(CONFIG_PATH.open(), Loader=yaml.SafeLoader)
    stream_harvest = StreamHarvest(**config_json)

    # read from config file if flags are False
    if refresh:
        stream_harvest.harvest_options.refresh = refresh

    if test_run:
        stream_harvest.harvest_options.test = test_run

    # Get name and image tag
    name = response['stream']['table_name']
    now = datetime.datetime.utcnow()
    image_registry = IMAGE_REGISTRY
    image_name = IMAGE_NAME
    image_tag = f"{name}.{now:%Y%m%dT%H%M}"

    storage_options = dict(
        registry_url=image_registry,
        dockerfile=HERE.joinpath("Dockerfile"),
        image_name=image_name,
        prefect_directory="/home/jovyan/prefect",
        env_vars={'HARVEST_ENV': 'ooi-harvester'},
        python_dependencies=[
            'git+https://github.com/ooi-data/ooi-harvester.git@main'
        ],
        image_tag=image_tag,
    )
    run_options = {
        'env': {
            'GH_PAT': os.environ.get('GH_PAT', None),
            'AWS_KEY': os.environ.get('AWS_KEY', None),
            'AWS_SECRET': os.environ.get('AWS_SECRET', None),
            'OOI_USERNAME': os.environ.get('OOI_USERNAME', None),
            'OOI_TOKEN': os.environ.get('OOI_TOKEN', None),
            'PREFECT__CLOUD__HEARTBEAT_MODE': 'thread',
        },
        'cpu': '2 vcpu',
        'memory': '16 GB',
        'labels': ['ecs-agent', 'ooi', 'prod'],
        'task_role_arn': os.environ.get('TASK_ROLE_ARN', None),
        'execution_role_arn': os.environ.get('EXECUTION_ROLE_ARN', None),
        'run_task_kwargs': {
            'cluster': 'prefectECSCluster',
            'launchType': 'FARGATE',
        },
    }

    print("1) SETTING UP THE FLOW")
    pipeline = OOIStreamPipeline(
        response,
        storage_type='docker',
        stream_harvest=stream_harvest,
        run_config_type='ecs',
        storage_options=storage_options,
        run_config_options=run_options,
        task_state_handlers=[process_status_update],
        data_availability=True,
        da_config={'gh_write': True},
    )
    pipeline.flow.validate()
    print(pipeline)

    print("2) REGISTERING THE FLOW")
    pipeline.flow.register(project_name=project_name)

    if run_flow:
        print("3) RUNNING THE FLOW")
        subprocess.Popen(
            [
                "prefect",
                "run",
                "flow",
                f"--name={name}",
                f"--project={project_name}",
            ]
        )
        status_json = get_process_status_json(
            table_name=name,
            data_bucket=data_bucket,
            last_updated=datetime.datetime.utcnow().isoformat(),
            status="pending",
            data_start=response["stream"]["beginTime"],
            data_end=response["stream"]["endTime"],
        )
        print("4) WRITING FLOW STATUS")
        write_process_status_json(status_json)


if __name__ == "__main__":
    args = parse_args()
    main(
        test_run=args.test,
        refresh=args.refresh,
        data_bucket=args.path,
        project_name=args.prefect_project,
        run_flow=args.run_flow,
    )
