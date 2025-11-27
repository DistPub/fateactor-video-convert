import subprocess
from argparse import ArgumentParser
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth


def get_job():
    res = requests.get(f'{options.service}/xrpc/app.bsky.video.pickJob',
                       auth=HTTPBasicAuth(options.fateactor_username, options.fateactor_password))
    res.raise_for_status()
    data = res.json()
    if data['status'] == 0:
        return None
    return data['job']

def update_job(job, status):
    res = requests.get(f'{options.service}/xrpc/app.bsky.video.completeJob',
                       params={'job': job, 'status':status},
                       auth=HTTPBasicAuth(options.fateactor_username, options.fateactor_password))
    res.raise_for_status()

def main():
    start = datetime.now()
    while True:
        # if action running too long, we break
        now = datetime.now()
        if now - start > timedelta(hours=5):
            break

        # if no job found, we break
        job = get_job()
        if job is None:
            break

        did = job["author"]
        cid = job['cid']
        job_id = job['id']
        download_url = f'https://{job["pds"]}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}'
        video_file = '/tmp/video.mp4'
        cmd = f'curl -m 120 -L -o {video_file} "{download_url}"'

        if options.dev:
            print(cmd)

        if not options.skip_download:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print(f'download result: {result.stderr} {result.stdout}')

        work_dir = f'/tmp/{did}/{cid}'
        subprocess.run(f'mkdir -p {work_dir}', shell=True, capture_output=True, text=True)

        interval_seconds = options.interval
        max_seconds = options.max_seconds
        max_seg = str(max_seconds // interval_seconds)
        cmd = (f'ffmpeg -hide_banner -loglevel error -y -i {video_file} '
               f'-vf scale=-2:720 -c:v libx264 -b:v 1200k -preset faster '
               f'-profile:v high -level 3.1 -c:a aac -b:a 128k -ar 44100 '
               f'-f hls -hls_time {interval_seconds} '
               f'-hls_segment_filename "{work_dir}/video%0{len(max_seg)}d.ts" '
               f'-hls_playlist_type vod "{work_dir}/playlist.m3u8"')

        if options.dev:
            print(cmd)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            print(f'ffmpeg error: {result.stderr} {result.stdout}')
            update_job(job_id, 'error')
            continue

        cmd = (f'ffmpeg -hide_banner -loglevel error -y -ss 0.5 -i "{video_file}" '
               f'-vframes 1 -q:v 5 "{work_dir}/thumbnail.jpg"')

        if options.dev:
            print(cmd)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            print(f'ffmpeg error: {result.stderr} {result.stdout}')
            update_job(job_id, 'error')
            continue

        if options.dev:
            # reset status
            update_job(job_id, 'pending')
            return

        result = subprocess.run(
            f'aws s3 sync "{work_dir}" \
              "s3://${options.r2_bucket}/{did}/{cid}/" \
              --endpoint-url="${options.r2_endpoint}"'
        , shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            print(f's3 error: {result.stderr} {result.stdout}')
            update_job(job_id, 'error')
        else:
            update_job(job_id, 'done')

        subprocess.run(f'rm -rf {video_file} {work_dir}', shell=True, capture_output=True, text=True)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--skip-download', action='store_true')
    parser.add_argument('--fateactor-username', required=True)
    parser.add_argument('--fateactor-password', required=True)
    parser.add_argument('--r2-bucket', required=True)
    parser.add_argument('--r2-endpoint', required=True)
    parser.add_argument('--service', default='https://fateactor.hukoubook.com')
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--max-seconds', type=int, default=180)
    options = parser.parse_args()
    main()