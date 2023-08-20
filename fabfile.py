import logging
import os
from datetime import datetime

import coloredlogs
import requests
from dotenv import load_dotenv
from fabric import Connection, task

load_dotenv()

DOCKER_HOME = "/home/findr"
GITHUB_REPO = 'andrewshvv/findr'
DOCKER_BUILD_BRANCH = 'build'
DOCKER_HUB_REPO_IMAGE = "andrewsamokhvalov/findr"

# Create a custom logger
log = logging.getLogger(__name__)

# Set the level of this logger
log.setLevel(logging.INFO)

field_styles = {
    'asctime': {'color': 'white'},
    'name': {'color': 'white'}
}

# Redefining colors for levels
level_styles = {
    'debug': {'color': 'magenta'},
    'info': {'color': 'white'},
    'warning': {'color': 'yellow'},
    'error': {'color': 'red'},
    'critical': {'color': 'red', 'bold': True}
}

# colorize the logger
coloredlogs.install(
    level='DEBUG',
    logger=log,
    fmt='%(asctime)s - %(levelname)s - %(funcName)s() - %(message)s',
    field_styles=field_styles,
    level_styles=level_styles
)


def connect() -> Connection:
    assert os.getenv("DIGITAL_OCEAN_IP") is not None

    # initialise the connection
    return Connection(
        host=os.getenv("DIGITAL_OCEAN_IP"),
        user="root",
    )


@task
def deploy(ctx, transfer=False):
    # initialise the connection
    conn = connect()

    is_built, commit_hash = is_image_built()
    if not is_built:
        log.warning(f"Seems like image for latest commit hasn't been created yet, wait... hash: {commit_hash}")
        return

    log.info(f"Built for last commit is ready, latest commit hash: {commit_hash}")

    # running_id = get_running_image_id(conn)
    last_digest = get_last_docker_digest()
    # if running_id == last_digest: <==== id != digest
    #     log.info(f"Last available image already running: {last_digest}")
    #     return

    log.info(f"Found image {last_digest}, proceeding...")

    if transfer:
        # backup databases
        backup(conn)

        # put docker compose and other files
        transfer_files(conn)

    # load image and run new container
    docker_pull(conn)
    start_daemon(conn)

    # show logs
    logs(conn)


@task
def backup(conn: Connection):
    # Timestamp for the backup
    timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    # Just in case
    conn.run(f'mkdir -p {DOCKER_HOME}')

    # Add code here for backing up files
    with conn.cd(DOCKER_HOME):
        if conn.run('test -e ./telegram.session', warn=True).failed:
            log.warning('Can\'t backup telegram.session, file not found')
        else:
            conn.run(f"mv ./telegram.session ./telegram.session.bak.{timestamp}")
            log.info(f'Created backup telegram.session.bak.{timestamp}')

        if conn.run('test -e ./db.sqlite', warn=True).failed:
            log.warning('Can\'t backup db.sqlite, file not found')
        else:
            conn.run(f"mv ./db.sqlite ./db.sqlite.bak.{timestamp}")
            log.info(f'Created backup db.sqlite.bak.{timestamp}')

        if conn.run('test -e ./chroma', warn=True).failed:
            log.warning('Can\'t backup chroma directory, directory not found')
        else:
            conn.run(f"mv ./chroma ./chroma.bak.{timestamp}")
            log.info(f'Created backup chroma.bak.{timestamp}')


@task
def transfer_files(conn: Connection):
    # put your docker-compose.yml and other files
    conn.run(f'mkdir -p {DOCKER_HOME}')
    conn.put("./docker-compose.yml", os.path.join(DOCKER_HOME, 'docker-compose.yml'))
    log.info('Transferred docker-compose.yml ...')

    conn.put("./telegram.session", os.path.join(DOCKER_HOME, 'telegram.session'))
    log.info('Transferred telegram.session ...')

    conn.put("./db.sqlite", os.path.join(DOCKER_HOME, 'db.sqlite'))
    log.info('Transferred db.sqlite ...')

    conn.put("./.env", os.path.join(DOCKER_HOME, '.env'))
    log.info('Transferred .env ...')

    upload_directory(conn, './chroma', os.path.join(DOCKER_HOME, 'chroma'))
    log.info('Transferred chroma...')


@task
def retrieve_db(ctx):
    conn = connect()

    local_db_path = "./db.sqlite"
    if os.path.exists(local_db_path):
        # Timestamp for the backup
        timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
        os.system(f"mv {local_db_path} {local_db_path}.bak.{timestamp}")
        log.info(f'Created backup {local_db_path}.bak.{timestamp}')

    conn.get(os.path.join(DOCKER_HOME, 'db.sqlite'), local_db_path)
    log.info('Retrieved db.sqlite ...')

    download_directory(conn, './chroma', os.path.join(DOCKER_HOME, 'chroma'))
    log.info('Retrieved chroma ...')


@task
def docker_pull(conn: Connection):
    # pull the latest images and run
    with conn.cd(DOCKER_HOME):
        conn.run(f"docker pull --platform='linux/amd64' {DOCKER_HUB_REPO_IMAGE}:latest")


@task
def start_daemon(conn: Connection):
    # pull the latest images and run
    with conn.cd(DOCKER_HOME):
        conn.run(f"docker pull {DOCKER_HUB_REPO_IMAGE}:latest")
        conn.run("docker compose up -d findr")


@task
def stop_daemon(ctx):
    conn = connect()
    with conn.cd(DOCKER_HOME):
        conn.run("docker compose down")


@task
def logs(ctx, conn=None, service="findr"):
    if not conn:
        conn = connect()
    with conn.cd(DOCKER_HOME):
        conn.run(f"docker compose logs --follow --tail 50 {service}")


@task
def cleanup(ctx):
    conn = connect()
    conn.run("docker system prune -f")


def is_image_built(commit_hash=None):
    assert os.getenv("GITHUB_TOKEN") is not None

    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

    if not commit_hash:
        # Get the latest commit hash in the build branch
        response = requests.get(
            f'https://api.github.com/repos/{GITHUB_REPO}/commits/{DOCKER_BUILD_BRANCH}',
            headers={'Authorization': f'token {GITHUB_TOKEN}'}
        )
        response.raise_for_status()
        commit_hash = response.json()['sha']

    # Check if the image was built for the last commit
    response = requests.get(
        f'https://api.github.com/repos/{GITHUB_REPO}/actions/runs',
        headers={'Authorization': f'token {GITHUB_TOKEN}'}
    )
    response.raise_for_status()
    for run in response.json()['workflow_runs']:
        if run['head_sha'] == commit_hash:
            if run["status"] != "completed":
                return False, commit_hash
            return True, commit_hash

    return False, commit_hash


def get_running_image_id(conn):
    result = conn.run('docker ps --format "{{.Image}}"')
    if result.stdout.strip():
        digest = conn.run(f'docker inspect --format="{{{{.Id}}}}" {result.stdout.strip()}')
        return digest.stdout
    else:
        return None


def upload_directory(conn: Connection, local_dir: str, remote_dir: str):
    # Create a tar.gz file of your directory
    os.system(f"tar -czf /tmp/tmp.tar.gz -C {local_dir} .")

    # Then use put() to upload the tar.gz file to the remote server
    conn.put("/tmp/tmp.tar.gz", "/tmp/tmp.tar.gz")

    # On the remote server, extract the tar.gz file to the destination directory
    conn.run(f"mkdir -p {remote_dir} && tar -xzf /tmp/tmp.tar.gz -C {remote_dir}")

    # Finally, clean up the temporary tar.gz file both locally and remotely
    os.remove("/tmp/tmp.tar.gz")
    conn.run("rm /tmp/tmp.tar.gz")


def download_directory(conn: Connection, local_dir: str, remote_dir: str):
    try:
        # Create a tar.gz file of remote directory in the /tmp folder
        conn.run(f"tar -czf /tmp/tmp.tar.gz -C {remote_dir} .")

        # download the tar.gz file from remote server to local server's /tmp directory
        conn.get("/tmp/tmp.tar.gz", "/tmp/tmp.tar.gz")

        if os.path.isdir(local_dir):
            # Timestamp for the backup
            timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            os.system(f"mv {local_dir} {local_dir}.bak.{timestamp}")
            log.info(f'Created backup {local_dir}.bak.{timestamp}')

        # On the local server, extract the tar.gz file in the local directory
        os.system(f"mkdir -p {local_dir} && tar -xzf /tmp/tmp.tar.gz -C {local_dir}")

    finally:
        # Finally, clean up the temporary tar.gz file both locally and remotely
        if os.path.exists(f"/tmp/tmp.tar.gz"):
            os.remove("/tmp/tmp.tar.gz")
        conn.run("rm /tmp/tmp.tar.gz")


def get_last_docker_digest():
    assert os.getenv("DOCKER_USERNAME") is not None
    assert os.getenv("DOCKER_PASSWORD") is not None

    DOCKER_USERNAME = os.getenv("DOCKER_USERNAME")
    DOCKER_PASSWORD = os.getenv("DOCKER_PASSWORD")

    # Get the image digest from Docker Hub
    response = requests.get(
        f'https://hub.docker.com/v2/repositories/{DOCKER_HUB_REPO_IMAGE}/tags/latest',
        auth=(DOCKER_USERNAME, DOCKER_PASSWORD)
    )
    response.raise_for_status()
    image_digest = response.json()['images'][0]['digest']
    return image_digest
