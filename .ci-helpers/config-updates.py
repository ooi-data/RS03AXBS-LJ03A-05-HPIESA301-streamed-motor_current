import argparse

import yaml

from github import Github
from flatten_dict import flatten, unflatten
from ooi_harvester.settings import harvest_settings
from ooi_harvester.config import (
    CONFIG_PATH_STR,
    PROCESS_STATUS_PATH_STR,
    REQUEST_STATUS_PATH_STR,
)
from gh_utils import print_rate_limiting_info

INDEX_URL = "https://ooi-data.s3.us-west-2.amazonaws.com/index.json"


def _str_to_bool(s):
    """Convert string to bool (in argparse context)."""
    if s.lower() not in ['true', 'false']:
        raise ValueError('Need bool; got %r' % s)
    return eval(s.title())


def _update_config(current_config_json, value={}):
    changes = False
    current_config = current_config_json.copy()
    flatten_config = flatten(current_config)
    for k, v in flatten(value).items():
        if k in flatten_config:
            flatten_config[k] = v
    updated_config = unflatten(flatten_config)
    if flatten(current_config_json) != flatten_config:
        changes = True
    return updated_config, changes


def _dispatch_workflow(repo, workflow='Data Request'):
    request_wf = next(wf for wf in repo.get_workflows() if wf.name == workflow)
    queued = request_wf.get_runs(status='queued').get_page(0)
    in_progress = request_wf.get_runs(status='in_progress').get_page(0)
    if len(queued) > 0 or len(in_progress) > 0:
        print(f"Skipping {workflow} run for {repo.name}, already in progress")
    else:
        print(f"Starting {workflow} for {repo.name}")
        request_wf.create_dispatch(harvest_settings.github.main_branch)


def config_update(repo, values, debug=True, force=False):
    try:
        print(repo.name)
        config = repo.get_contents(
            CONFIG_PATH_STR, ref=harvest_settings.github.main_branch
        )
        config_json = yaml.safe_load(config.decoded_content)
        updated_config, changes = _update_config(config_json, value=values)
        if debug:
            print(f"Debug mode, updated config: {updated_config}")
            if not changes:
                print("No changes found... skipping update.")
                if force:
                    print("Force flag found. No dispatching in debug mode.")
        else:
            if not changes:
                print("No changes found... skipping update.")
                if force:
                    _dispatch_workflow(repo)
            else:
                process_status = repo.get_contents(
                    PROCESS_STATUS_PATH_STR,
                    ref=harvest_settings.github.main_branch,
                )
                request_status = repo.get_contents(
                    REQUEST_STATUS_PATH_STR,
                    ref=harvest_settings.github.main_branch,
                )
                request_status_json = yaml.safe_load(
                    request_status.decoded_content
                )
                process_status_json = yaml.safe_load(
                    process_status.decoded_content
                )
                print(
                    "Request:",
                    request_status_json['last_request'],
                    request_status_json['status'],
                )
                print(
                    "Process:",
                    process_status_json['last_updated'],
                    process_status_json['status'],
                )
                print("Updating config values...")
                config_yaml = yaml.safe_dump(updated_config)
                repo.update_file(
                    CONFIG_PATH_STR,
                    message="Update config values",
                    content=config_yaml,
                    sha=config.sha,
                    branch=harvest_settings.github.main_branch,
                )
                print("Done.")
        print()
    except Exception as e:
        print(repo.name)
        print(f'File not found: {e}')
        print(f"https://github.com/ooi-data/{repo.name}")
        print()


def parse_args():
    parser = argparse.ArgumentParser(description='Perform Config Updates')
    parser.add_argument(
        '--refresh',
        type=_str_to_bool,
        nargs="?",
        const=True,
        default=True,
        help='Harvest options refresh',
    )
    parser.add_argument(
        '--goldcopy',
        type=_str_to_bool,
        nargs="?",
        const=True,
        default=True,
        help='Harvest options goldcopy',
    )
    parser.add_argument(
        '--test',
        type=_str_to_bool,
        nargs="?",
        const=True,
        default=False,
        help='Harvest options test',
    )
    parser.add_argument(
        '--debug',
        type=_str_to_bool,
        nargs="?",
        const=True,
        default=True,
        help='Not commit the changes',
    )
    parser.add_argument(
        '--repo',
        nargs="?",
        type=str,
        const=None,
        help='Specific repo to update config on',
    )
    parser.add_argument(
        '--force',
        type=_str_to_bool,
        nargs="?",
        const=False,
        default=False,
        help='Force dispatch data request even if there is no changes.',
    )
    parser.add_argument(
        '--from-index',
        type=_str_to_bool,
        nargs="?",
        const=False,
        default=False,
        help='Only update from ooi-data index',
    )

    return parser.parse_args()


def main():
    args = parse_args()
    values = {
        'harvest_options': {
            'refresh': args.refresh,
            'goldcopy': args.goldcopy,
            'test': args.test,
        }
    }
    gh = Github(harvest_settings.github.pat)
    print_rate_limiting_info(gh, 'GH_PAT')
    data_org = gh.get_organization(harvest_settings.github.data_org)
    if args.repo:
        try:
            repo = data_org.get_repo(args.repo)
            config_update(repo, values, debug=args.debug, force=args.force)
        except Exception:
            raise ValueError(f"{args.repo} repository does not exist.")
    elif args.from_index is True:
        import requests
        import itertools as it

        resp = requests.get(INDEX_URL)
        data_index = resp.json()

        sorted_streams = sorted(
            it.chain.from_iterable(
                map(lambda i: i['streams'], data_index['instruments'])
            ),
            key=lambda s: s['bytes_size'],
        )
        for stream in sorted_streams:
            try:
                repo = data_org.get_repo(stream['id'])
                config_update(repo, values, debug=args.debug, force=args.force)
            except Exception:
                print(f"{stream['id']} repository does not exist.")
                pass
    else:
        for repo in data_org.get_repos():
            if repo.name != 'stream_template':
                config_update(repo, values, debug=args.debug, force=args.force)


if __name__ == "__main__":
    main()
