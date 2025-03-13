import sys
import logging
import tomllib

from shutil import rmtree
from functools import cache
from dataclasses import dataclass
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


def sync_repos(job: MirrorJob, cleanup: bool = True):
    heads = get_remote_heads(job.to_repo)
    if heads is None:
        raise ValueError("wat")

    target_dir = ("-C", job.name)
    xtras = {"check": True, "stderr": STDOUT}

    if job.to_branch in heads:
        # Clone the target repository first to minimize load on the source server:
        run(["git", "clone", "--no-checkout", "--branch", job.to_branch, job.to_repo, job.name], timeout=1800, **xtras)
    else:
        # Clone the target repo first, but create the new branch manually:
        logging.info("Target branch %s not found in repo %s - initializing", job.to_branch, job.to_repo)
        run(["git", "clone", "--no-checkout", job.to_repo, job.name], timeout=1800, **xtras)
        run(["git", *target_dir, "switch", "-c", job.to_branch], timeout=60, **xtras)

    run(["git", *target_dir, "config", "http.postBuffer", "157286400"], timeout=30, **xtras)

    # Set up and download missing objects from the source server:
    logging.info("Downloading the source branch from %s -> %s...", job.from_repo, job.from_branch)
    run(["git", *target_dir, "remote", "add", "mirrorsrc", job.from_repo], timeout=30, **xtras)
    run(["git", *target_dir, "fetch", "mirrorsrc", job.from_branch], timeout=1800, **xtras)

    # Make the target branch point to the mirrored object and force push it:
    logging.info("Pushing to the target branch to %s -> %s...", job.to_repo, job.to_branch)
    run(["git", *target_dir, "branch", "-f", job.to_branch, f"mirrorsrc/{job.from_branch}"], timeout=60, **xtras)
    run(["git", *target_dir, "push", "--set-upstream", "--force", "origin", job.to_branch], timeout=1800, **xtras)

    # Clean up after the mirror:
    if cleanup:
        rmtree(job.name)


def main():
    whoopsie = False
    jobs = load_config()

    for name, job in jobs.items():
        logging.info("[+] Processing job: %s", job.name)
        if not should_sync_job(job):
            continue

        try:
            sync_repos(job)
        except:
            logging.exception("Failed to process job: %s", job.name)
            whoopsie = True

    return 1 if whoopsie else 0


if __name__ == "__main__":
    sys.exit(main())
