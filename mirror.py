import sys
import uuid
import string
import logging
import tomllib

from shutil import rmtree
from functools import cache
from itertools import groupby
from dataclasses import dataclass
from urllib.parse import urlparse
from subprocess import run, CalledProcessError, STDOUT


logging.getLogger().setLevel(logging.INFO)


@dataclass
class MirrorJob:
    name: str
    from_repo: str
    from_branch: str
    to_repo: str
    to_branch: str


def load_config(filename: str = "mirror-config.toml") -> dict[str, MirrorJob]:
    with open("mirror-config.toml", "rb") as f:
        data = tomllib.load(f)

    mirror_jobs: dict[str, MirrorJob] = {}
    for name, job_data in data['mirror'].items():
        # Convert from-repo -> from_repo, etc for the dataclass:
        args = {k.replace("-", "_"): v for k, v in job_data.items()}
        mirror_jobs[name] = MirrorJob(name=name, **args)

    return mirror_jobs


@cache
def get_remote_heads(url: str) -> dict[str, str] | None:
    try:
        cmd = ["git", "ls-remote", "--heads", url]
        p = run(cmd, capture_output=True, timeout=60, check=True)
        heads = {}
        for line in p.stdout.decode().splitlines():
            hash, head = line.split("\t")
            heads[head.strip().removeprefix("refs/heads/")] = hash.strip()

        logging.debug("Git heads for %s: %s", url, str(heads))
        return heads
    except CalledProcessError:
        logging.exception("Failed to fetch heads for: %s", url)
        return None


def should_sync_job(job: MirrorJob) -> bool:
    from_heads = get_remote_heads(job.from_repo)
    to_heads = get_remote_heads(job.to_repo)

    if from_heads is None or to_heads is None:
        logging.error("Could not fetch git heads, skipping job: %s", job.name)
        return False

    from_hash = from_heads.get(job.from_branch)
    to_hash = to_heads.get(job.to_branch)
    logging.info(
        "Comparing: %s -> %s @ %s vs %s -> %s @ %s",
        job.from_repo, job.from_branch, from_hash,
        job.to_repo, job.to_branch, to_hash
    )

    if from_hash is None:  # to_hash may be None if the target branch does not exist
        logging.error("Source branch %s does not exist, skipping job: %s", job.from_branch, job.name)
        return False

    if from_hash == to_hash:
        logging.info("Source and target branches are in sync for job: %s", job.name)
        return False

    return True


def sync_repos(job: MirrorJob, repo_dir: str):
    logging.info("[+] Processing job: %s", job.name)

    heads = get_remote_heads(job.to_repo)
    if heads is None:
        raise ValueError("wat")

    git_cmd = ("git", "-C", repo_dir)
    xtras = {"check": True, "stderr": STDOUT}

    # Set up and download missing objects from the source server:
    logging.info("Downloading the source branch from %s -> %s...", job.from_repo, job.from_branch)
    run([*git_cmd, "remote", "add", job.name, job.from_repo], timeout=30, **xtras)
    run([*git_cmd, "fetch", job.name, job.from_branch], timeout=1800, **xtras)

    # Make the target branch point to the mirrored object and force push it:
    logging.info("Pushing to the target branch to %s -> %s...", job.to_repo, job.to_branch)
    run([*git_cmd, "push", "--force", "origin", f"refs/remotes/{job.name}/{job.from_branch}:refs/heads/{job.to_branch}"], timeout=1800, **xtras)


def generate_repo_path(url: str) -> str:
    path = urlparse(url).path.strip().strip('/').lower()
    safe_chars = [ch for ch in path if ch in string.ascii_lowercase + string.digits]
    return "".join(safe_chars) + "-" + str(uuid.uuid4())


def main():
    global_failure = False
    by_repo_url = lambda x: x.to_repo
    jobs = [job for job in load_config().values() if should_sync_job(job)]

    logging.info("Will sync %d job(s): %s", len(jobs), ", ".join(j.name for j in jobs))

    # Group jobs by the target repo to clone the target repo only once.
    for repo_url, jobs in groupby(sorted(jobs, key=by_repo_url), key=by_repo_url):
        logging.info("[+] Processing jobs for repo: %s", repo_url)
        repo_path = generate_repo_path(repo_url)
        group_failure = False

        # Clone the target repository first to minimize load on the source server:
        logging.info("Cloning and configuring target repo %s to %s...", repo_url, repo_path)
        run(["git", "clone", "--no-checkout", repo_url, repo_path], timeout=1800, check=True)
        run(["git", "-C", repo_path, "config", "http.postBuffer", "157286400"], timeout=30, check=True)

        for job in jobs:
            try:
                sync_repos(job, repo_path)
            except:
                logging.exception("Failed to process job: %s", job.name)
                global_failure = True
                group_failure = True

        if not group_failure:
            # Leave the dir for debugging purposes:
            try:
                rmtree(repo_path)
            except:
                logging.exception("Could not clean up the job repo: %s", repo_path)
                # Don't set failures here, if the sync was successful, it's all good!

    return 1 if global_failure else 0


if __name__ == "__main__":
    sys.exit(main())
