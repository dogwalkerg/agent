import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import hashlib
from github import Auth, Github

REQUEST_TIMEOUT = 30
DOWNLOAD_DEADLINE = 180
# Gitee may take longer than a fixed socket timeout while accepting large ZIPs.
# The workflow-level timeout remains the outer safety limit.
UPLOAD_TIMEOUT = None
MAX_RETRIES = 3
GITEE_REPOSITORIES = (
    ("hashgopeed", "agent", "GITEE_TOKEN"),
    ("naibahq", "agent", "GITEE_TOKEN2"),
)


def get_github_latest_release():
    github_token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
    g = (Github(auth=Auth.Token(github_token), timeout=REQUEST_TIMEOUT)
         if github_token else Github(timeout=REQUEST_TIMEOUT))
    repo = g.get_repo("nezhahq/agent")
    release = repo.get_latest_release()
    if release:
        print(f"Latest release tag is: {release.tag_name}", flush=True)
        print(f"Latest release info is: {release.body}", flush=True)
        files = []
        download_headers = {'Accept': 'application/octet-stream'}
        if github_token:
            download_headers['Authorization'] = f'Bearer {github_token}'
        assets = [(asset.browser_download_url, asset.name)
                  for asset in release.get_assets()]

        def download_asset(item):
            url, name = item
            print(f"Downloading {name}", flush=True)
            for attempt in range(1, MAX_RETRIES + 1):
                response = None
                try:
                    started_at = time.monotonic()
                    downloaded_bytes = 0
                    response = requests.get(
                        url, headers=download_headers, allow_redirects=True,
                        stream=True, timeout=(10, 120))
                    response.raise_for_status()
                    with open(name, 'wb') as output_file:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                output_file.write(chunk)
                                downloaded_bytes += len(chunk)
                                if time.monotonic() - started_at > DOWNLOAD_DEADLINE:
                                    raise requests.exceptions.Timeout(
                                        f"download exceeded {DOWNLOAD_DEADLINE}s")
                    print(f"Downloaded {name} ({downloaded_bytes // (1024 * 1024)} MiB)", flush=True)
                    return name
                except (OSError, requests.exceptions.RequestException) as err:
                    status = 'no response' if response is None else str(response.status_code)
                    print(
                        f"Download {name} failed (HTTP {status}, attempt "
                        f"{attempt}/{MAX_RETRIES}): {err}", flush=True)
                    if attempt == MAX_RETRIES:
                        raise RuntimeError(
                            f"Failed to download {name} after {MAX_RETRIES} attempts") from err
                    time.sleep(2 ** (attempt - 1))

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(download_asset, item) for item in assets]
            for future in as_completed(futures):
                name = future.result()
                file_abs_path = get_abs_path(name)
                if not os.path.isfile(file_abs_path):
                    raise RuntimeError(f"Downloaded asset is missing: {file_abs_path}")
                files.append(file_abs_path)
        files.sort()
        print('Checking file integrities')
        verify_checksum(get_abs_path("checksums.txt"))
        for owner, repo, token_name in GITEE_REPOSITORIES:
            print(f"Syncing release {release.tag_name} to Gitee {owner}/{repo}")
            sync_to_gitee(
                release.tag_name, release.body, files, owner, repo, token_name)
    else:
        print("No releases found.")


def delete_gitee_releases(latest_id, client, uri, token):
    get_data = {
        'access_token': token
    }

    release_info = []
    release_response = client.get(uri, json=get_data)
    if release_response.status_code == 200:
        release_info = release_response.json()
    else:
        print(
            f"Request failed with status code {release_response.status_code}")

    release_ids = []
    for block in release_info:
        if 'id' in block:
            release_ids.append(block['id'])

    print(f'Current release ids: {release_ids}')
    if latest_id in release_ids:
        release_ids.remove(latest_id)

    for id in release_ids:
        release_uri = f"{uri}/{id}"
        delete_data = {
            'access_token': token
        }
        delete_response = client.delete(release_uri, json=delete_data)
        if delete_response.status_code == 204:
            print(f'Successfully deleted release #{id}.')
        else:
            raise ValueError(
                f"Request failed with status code {delete_response.status_code}")


def sync_to_gitee(
        tag: str, body: str, files: slice, owner: str, repo: str,
        token_name: str):
    release_id = ""
    release_api_uri = f"https://gitee.com/api/v5/repos/{owner}/{repo}/releases"
    api_client = requests.Session()
    api_client.headers.update({
        'Accept': 'application/json'
    })

    access_token = os.environ.get(token_name, '').strip()
    if not access_token:
        raise RuntimeError(f'{token_name} is missing or empty')

    # Replace an existing release with the same tag so both mirrors stay identical.
    existing_response = api_client.get(
        release_api_uri,
        params={'access_token': access_token, 'page': 1, 'per_page': 100},
        timeout=REQUEST_TIMEOUT)
    if existing_response.status_code == 200:
        for existing_release in existing_response.json():
            if existing_release.get('tag_name') == tag:
                existing_id = existing_release.get('id')
                if existing_id:
                    delete_response = api_client.delete(
                        f"{release_api_uri}/{existing_id}",
                        params={'access_token': access_token},
                        timeout=REQUEST_TIMEOUT)
                    if delete_response.status_code not in (204, 404):
                        raise RuntimeError(
                            f"Failed to replace {owner}/{repo} release {tag}: "
                            f"HTTP {delete_response.status_code} {delete_response.text[:500]}")
                    print(f"Replaced existing {owner}/{repo} release {tag}")
                break
    elif existing_response.status_code not in (401, 403, 404):
        raise RuntimeError(
            f"Unable to inspect {owner}/{repo} releases: HTTP "
            f"{existing_response.status_code} {existing_response.text[:500]}")

    release_data = {
        'access_token': access_token,
        'tag_name': tag,
        'name': tag,
        'body': body,
        'prerelease': False,
        'target_commitish': 'main'
    }
    release_api_response = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            release_api_response = api_client.post(
                release_api_uri, json=release_data, timeout=REQUEST_TIMEOUT)
            if release_api_response.status_code == 201:
                break
            print(
                f"Create release returned HTTP {release_api_response.status_code} "
                f"(attempt {attempt}/{MAX_RETRIES}): {release_api_response.text[:500]}")
        except requests.exceptions.RequestException as err:
            print(f"Create release failed (attempt {attempt}/{MAX_RETRIES}): {err}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** (attempt - 1))

    if release_api_response is None or release_api_response.status_code != 201:
        status = 'no response' if release_api_response is None else str(release_api_response.status_code)
        raise RuntimeError(f"Gitee release creation failed after {MAX_RETRIES} attempts (HTTP {status})")

    release_info = release_api_response.json()
    release_id = release_info.get('id')
    if not release_id:
        raise RuntimeError('Gitee release response did not contain an id')

    print(f"Gitee release id: {release_id}")
    asset_api_uri = f"{release_api_uri}/{release_id}/attach_files"

    for file_path in files:
        success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with open(file_path, 'rb') as upload_file:
                    asset_api_response = api_client.post(
                        asset_api_uri,
                        params={'access_token': access_token},
                        files={'file': upload_file},
                        timeout=UPLOAD_TIMEOUT)
                if asset_api_response.status_code == 201:
                    asset_info = asset_api_response.json()
                    asset_name = asset_info.get('name', os.path.basename(file_path))
                    print(f"Successfully uploaded {asset_name}!")
                    success = True
                    break
                print(
                    f"Upload {os.path.basename(file_path)} returned HTTP "
                    f"{asset_api_response.status_code} (attempt {attempt}/{MAX_RETRIES}): "
                    f"{asset_api_response.text[:500]}")
            except (OSError, requests.exceptions.RequestException) as err:
                print(
                    f"Upload {os.path.basename(file_path)} failed "
                    f"(attempt {attempt}/{MAX_RETRIES}): {err}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** (attempt - 1))

        if not success:
            raise RuntimeError(
                f"Failed to upload {os.path.basename(file_path)} after "
                f"{MAX_RETRIES} attempts")

    # 仅保留最新 Release 以防超出 Gitee 仓库配额
    try:
        delete_gitee_releases(release_id, api_client,
                              release_api_uri, access_token)
    except ValueError as e:
        print(e)

    api_client.close()
    print(f"Sync is completed for {owner}/{repo}!")


def get_abs_path(path: str):
    wd = os.getcwd()
    return os.path.join(wd, path)


def compute_sha256(file: str):
    sha256_hash = hashlib.sha256()
    buf_size = 65536
    with open(file, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha256_hash.update(data)
    return sha256_hash.hexdigest()


def verify_checksum(checksum_file: str):
    with open(checksum_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        checksum, file = line.strip().split()
        abs_path = get_abs_path(file)
        computed_hash = compute_sha256(abs_path)

        if checksum == computed_hash:
            print(f"{file}: OK")
        else:
            print(f"{file}: FAIL (expected {checksum}, got {computed_hash})")
            raise RuntimeError(f"Checksum verification failed for {file}")


get_github_latest_release()
