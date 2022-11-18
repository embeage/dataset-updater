from urllib.parse import urljoin
import time
import logging
import asyncio
import aiohttp
import backoff
from bs4 import BeautifulSoup

TIMEOUT = 60*60*5
SEMAPHORE_COUNTER = 100
API_URL = 'https://api.svt.se'
HEADERS = {
    'user-agent': ("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) "
                   "Gecko/20100101 Firefox/47.0")
    }
DASH_PRIORITY = (
    'dash-full', 'dash', 'dash-avc', 'dashhbbtv',
    'dash-hbbtv', 'dash-hbbtv-avc', 'dash-hb-avc', 'dash-lb-full',
    'dash-lb', 'dash-lb-avc', 'dash-hbbtv-avc-51', 'dash-hb-avc-51',
    'dash-avc-51', 'dash-hevc', 'dash-hbbtv-hevc', 'dash-hb-hevc',
    'dash-lb-hevc', 'dash-hevc-51', 'dash-hbbtv-hevc-51', 'dash-hb-hevc-51'
    )

def fatal_code(ex):
    return 400 <= ex.status < 500

def retry(function):
    function = backoff.on_exception(
        backoff.expo, aiohttp.ClientResponseError,
        max_time=60, giveup=fatal_code, logger=None)(function)
    function = backoff.on_exception(
        backoff.expo, aiohttp.ClientConnectionError,
        max_time=60, logger=None)(function)
    function = backoff.on_exception(
        backoff.expo, asyncio.TimeoutError,
        max_time=300, logger=None)(function)
    return function

def manifest_url(video_refs):
    for dash_format in DASH_PRIORITY:
        for ref in video_refs:
            if dash_format == ref['format']:
                return ref['url'] + '&excludeCodecs=hvc&excludeCodecs=ac-3'
    raise ValueError("DASH format not found")

def n_segments(segment_timeline):
    segments = 0
    for segment in segment_timeline.find_all('s'):
        segments += 1
        subsequent_segments = segment.get('r')
        if subsequent_segments is None:
            continue
        for _ in range(int(subsequent_segments)):
            segments += 1
    return segments

@retry
async def fetch(session, url):
    async with session.get(url) as resp:
        return await resp.read()

@retry
async def fetch_json(session, url):
    async with session.get(url) as resp:
        return await resp.json()

@retry
async def fetch_content_length(session, url, semaphore):
    async with semaphore:
        async with session.head(url, headers=HEADERS, timeout=10) as resp:
            return resp.content_length

async def dash_manifest(session, svtplay_id):
    video_data = await fetch_json(session, f'{API_URL}/video/{svtplay_id}')
    url = (f'{API_URL}/ditto/api/V1/web?manifestUrl='
           + manifest_url(video_data['videoReferences']))
    return await fetch(session, url)

async def download_encoding(session, base_url, rep, content_type):
    segment_template = rep.find('segmenttemplate')
    segment_timeline = segment_template.find('segmenttimeline')
    segment_length = (int(segment_timeline.find('s')['d'])
                      / int(segment_template['timescale']))
    media_path = segment_template['media'].replace('$Number$', '{}')
    url = urljoin(base_url, media_path)

    sem = asyncio.Semaphore(SEMAPHORE_COUNTER)
    tasks = [asyncio.create_task(
        fetch_content_length(session, url.format(i+1), sem)
        ) for i in range(n_segments(segment_timeline))]

    segment_sizes = await asyncio.gather(*tasks, return_exceptions=True)
    for result in segment_sizes:
        if isinstance(result, Exception):
            return result

    encoding = {
        'bandwidth': rep['bandwidth'],
        'codecs': rep['codecs'],
        'mime_type': rep['mimetype'],
        'segment_length': segment_length,
        'segment_sizes': segment_sizes
        }

    if content_type == 'audio':
        encoding['sampling_rate'] = rep['audiosamplingrate']
    else:
        encoding['width'] = rep['width']
        encoding['height'] = rep['height']

    return encoding

async def download_encodings(session, svtplay_id):
    manifest = await dash_manifest(session, svtplay_id)
    soup = BeautifulSoup(manifest.decode('utf8'), 'lxml')
    base_url = soup.find('baseurl').text
    if soup.find('segmenttemplate')['media'].startswith('chunk-stream'):
        raise ValueError("wrong manifest schema")

    video_tasks = []
    for adaptation_set in soup.find_all('adaptationset'):
        content_type = adaptation_set['contenttype']
        if content_type == 'video':
            for rep in adaptation_set.find_all('representation'):
                video_tasks.append(asyncio.create_task(
                    download_encoding(session, base_url, rep, content_type)))
        elif content_type == 'audio':
            if adaptation_set.find('role')['value'] == 'main':
                rep = adaptation_set.find('representation')
                audio_task = asyncio.create_task(
                    download_encoding(session, base_url, rep, content_type))

    video_result = await asyncio.gather(*video_tasks, return_exceptions=True)
    audio_result = await audio_task
    for result in video_result:
        if isinstance(result, Exception):
            raise result
    if isinstance(audio_result, Exception):
        raise audio_result

    return {
        'id': svtplay_id,
        'videos': video_result,
        'audio': audio_result
        }

async def run(database):
    timeout = time.time() + TIMEOUT
    svtplay_ids = database.not_downloaded()
    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(
            connector=connector, raise_for_status=True) as session:
        logging.info("Downloading segments for %i videos", len(svtplay_ids))
        for svtplay_id in svtplay_ids:
            if time.time() > timeout:
                break
            msg = f"Downloading {svtplay_id} "
            try:
                video_encodings = await download_encodings(session, svtplay_id)
                database.store(video_encodings=video_encodings)
            except (KeyError, ValueError, TypeError) as ex:
                msg += f"failed because {ex}"
                logging.warning(msg)
            except aiohttp.ClientResponseError as ex:
                msg += (f"failed with {ex.status} on {ex.request_info.url} - "
                        "exhausted retries")
                logging.warning(msg)
            except aiohttp.ClientConnectionError as ex:
                msg += f"failed with {ex} - exhausted retries"
                logging.warning(msg)
            except asyncio.TimeoutError:
                msg += "timed out - exhausted retries"
                logging.warning(msg)

def download(database):
    asyncio.run(run(database))
