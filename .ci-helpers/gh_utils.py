from datetime import datetime


def print_rate_limiting_info(gh, user):
    # Compute some info about our GitHub API Rate Limit.
    # Note that it doesn't count against our limit to
    # get this info. So, we should be doing this regularly
    # to better know when it is going to run out. Also,
    # this will help us better understand where we are
    # spending it and how to better optimize it.

    # Get GitHub API Rate Limit usage and total
    gh_api_remaining = gh.get_rate_limit().core.remaining
    gh_api_total = gh.get_rate_limit().core.limit

    # Compute time until GitHub API Rate Limit reset
    gh_api_reset_time = gh.get_rate_limit().core.reset
    gh_api_reset_time -= datetime.utcnow()

    print("")
    print("GitHub API Rate Limit Info:")
    print("---------------------------")
    print("token: ", user)
    print(
        "Currently remaining {remaining} out of {total}.".format(
            remaining=gh_api_remaining, total=gh_api_total
        )
    )
    print("Will reset in {time}.".format(time=gh_api_reset_time))
    print("")
    return gh_api_remaining
