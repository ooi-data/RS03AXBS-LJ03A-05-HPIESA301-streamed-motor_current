from github import Github
from ooi_harvester.settings import harvest_settings
from gh_utils import print_rate_limiting_info


def main(dispatch=True):
    gh = Github(harvest_settings.github.pat)
    print_rate_limiting_info(gh, 'GH_PAT')
    data_org = gh.get_organization(harvest_settings.github.data_org)
    for repo in data_org.get_repos():
        if repo.name != 'stream_template':
            try:
                repo.get_contents('config.yaml')
                update_template_wf = next(
                    wf
                    for wf in repo.get_workflows()
                    if wf.name == 'Update from template'
                )
                queued = update_template_wf.get_runs(status='queued').get_page(
                    0
                )
                in_progress = update_template_wf.get_runs(
                    status='in_progress'
                ).get_page(0)
                if len(queued) > 0 or len(in_progress) > 0:
                    print("Skipping workflow run, already in progress")
                else:
                    print(f"Updating template for {repo.name}")
                    if dispatch:
                        update_template_wf.create_dispatch(
                            harvest_settings.github.main_branch
                        )
            except Exception:
                pass


if __name__ == "__main__":
    main()
