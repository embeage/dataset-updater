import os
import logging
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2

class DB:
    def __init__(self):
        load_dotenv()
        self._conn = psycopg2.connect(
            "dbname={0} user={1} password={2}".format(
                os.environ.get('DB_NAME'),
                os.environ.get('DB_USER'),
                os.environ.get('DB_PASSWORD'))
            )

        self._videos = self._memo_videos()

    def close(self):
        self._conn.close()

    def export_csv(self, file_name, international=False):
        with self._conn, self._conn.cursor() as curs:
            with open(file_name, 'w', newline='', encoding='utf8') as file:
                curs.execute("""
                    SELECT id, name, duration, 
                        segment_length, ARRAY_AGG(combined)
                    FROM 
                        (SELECT V.id, name, duration, E1.segment_length,
                        (SELECT ARRAY_AGG(COALESCE(U.v_sizes + U.a_sizes, 0))
                        FROM (SELECT UNNEST(E1.segment_sizes) v_sizes, 
                            UNNEST(E2.segment_sizes) a_sizes) U) AS combined
                        FROM Videos V
                        INNER JOIN VideoEncodings AS E1 ON V.id = E1.video
                        INNER JOIN AudioEncodings AS E2 ON V.id = E2.video
                        WHERE sweden_only IS FALSE OR sweden_only IS NOT %s
                        GROUP BY V.id, name, duration, E1.segment_length, 
                            E1.segment_sizes, E2.segment_sizes) T
                    GROUP BY id, name, duration, segment_length
                    ORDER BY name
                    ;""", (international,))

                csv_writer = csv.writer(file)
                for row in curs.fetchall():
                    seg_size_lists = tuple(','.join(map(str, segment_sizes))
                                          for segment_sizes in row[4])
                    csv_writer.writerow((row[0], row[1], row[2], row[3])
                                        + seg_size_lists)

    def store(self, **kwargs):
        with self._conn, self._conn.cursor() as curs:
            if genres := kwargs.get('genres'):
                self._insert_genres(curs, genres)
            if videos := kwargs.get('videos'):
                self._insert_videos(curs, videos)
            if video_encodings := kwargs.get('video_encodings'):
                self._insert_video_encodings(curs, video_encodings)

    def update(self, raw):

        self._delete_deprecated()

        genres = tuple(format_genre(gen)
                       for gen in raw['data']['genresSortedByName']['genres'])
        episode_urls = {ep['videoSvtId']: ep['urls']['svtplay']
                        for ep in raw['data']['allEpisodesForInternalUse']}
        videos = tuple(format_episode(vid, ep, episode_urls)
                       for vid in raw['data']['programAtillO']['flat']
                       for ep in vid['episodes'])

        self.store(genres=genres, videos=videos)
        self._videos = self._memo_videos()

    def not_downloaded(self):
        with self._conn, self._conn.cursor() as curs:
            curs.execute("""
                (SELECT id AS video 
                FROM Videos)
                EXCEPT
                (SELECT DISTINCT video 
                FROM VideoEncodings)
                ;""")
            return tuple(row[0] for row in curs.fetchall())

    def _memo_videos(self):
        with self._conn, self._conn.cursor() as curs:
            curs.execute("SELECT * FROM Videos;")
            return {video_row[0]: video_row[1:]
                    for video_row in curs.fetchall()}

    def _delete_deprecated(self):
        with self._conn, self._conn.cursor() as curs:
            curs.execute("""
                DELETE FROM Videos
                WHERE valid_to < CURRENT_TIMESTAMP
                OR valid_from > CURRENT_TIMESTAMP
                ;""")
            logging.info("Deleted %i deprecated videos", curs.rowcount)

    def _insert_genres(self, curs, genres):
        for genre in genres:
            curs.execute("""
                INSERT INTO Genres AS G
                VALUES (%(id)s, %(name)s, %(description)s)
                ON CONFLICT (id) DO UPDATE SET 
                    name = EXCLUDED.name,
                    description = EXCLUDED.description
                WHERE
                    G.name <> EXCLUDED.name OR
                    G.description <> EXCLUDED.description
                ;""", genre)

    def _insert_videos(self, curs, videos):
        affected_rows = 0
        for video in videos:
            if not active(video):
                continue
            if len(video['id']) != 7:
                logging.warning("Cannot insert [%s] with unexpected id [%s]",
                                video['name'], video['id'])
                continue
            curs.execute("""
                INSERT INTO Videos AS V
                VALUES (%(id)s, %(name)s, %(duration)s, %(valid_from)s,
                    %(valid_to)s, %(sweden_only)s, %(url)s,
                    %(short_description)s, %(long_description)s,
                    %(production_year)s)
                ON CONFLICT (id) DO UPDATE SET 
                    name = EXCLUDED.name,
                    duration = EXCLUDED.duration,
                    valid_from = EXCLUDED.valid_from,
                    valid_to = EXCLUDED.valid_to,
                    sweden_only = EXCLUDED.sweden_only,
                    url = EXCLUDED.url,
                    short_description = EXCLUDED.short_description,
                    long_description = EXCLUDED.long_description,
                    production_year = EXCLUDED.production_year
                WHERE
                    V.name <> EXCLUDED.name OR
                    V.duration <> EXCLUDED.duration OR
                    V.valid_from <> EXCLUDED.valid_from OR
                    V.valid_to <> EXCLUDED.valid_to OR
                    V.sweden_only <> EXCLUDED.sweden_only OR
                    V.url <> EXCLUDED.url OR
                    V.short_description <> EXCLUDED.short_description OR
                    V.long_description <> EXCLUDED.long_description OR
                    V.production_year <> EXCLUDED.production_year
                ;""", video)
            affected_rows += curs.rowcount

            self._insert_video_genres(curs, video)

        logging.info("Added/updated %i videos", affected_rows)

    def _insert_video_genres(self, curs, video):
        if video['genres']:
            curs.execute("""
                DELETE FROM VideoGenres
                WHERE video = %s AND genre NOT IN %s
                ;""", (video['id'], video['genres']))

        for genre_id in video['genres']:
            curs.execute("""
                INSERT INTO VideoGenres
                VALUES (%s, %s)
                ON CONFLICT (video, genre) DO NOTHING
                ;""", (video['id'], genre_id))

    def _insert_video_encodings(self, curs, video_encodings):
        svtplay_id = video_encodings['id']
        videos = video_encodings['videos']
        audio = video_encodings['audio']

        videos_inserted = 0
        for video in videos:
            curs.execute("""
                INSERT INTO VideoEncodings
                (video, bandwidth, codecs, mime_type, width, height,
                    segment_length, segment_sizes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ;""", (svtplay_id, video['bandwidth'], video['codecs'],
                       video['mime_type'], video['width'], video['height'],
                       video['segment_length'], video['segment_sizes']))
            videos_inserted += curs.rowcount

        curs.execute("""
            INSERT INTO AudioEncodings 
            (video, bandwidth, codecs, mime_type, sampling_rate, 
                segment_length, segment_sizes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ;""", (svtplay_id, audio['bandwidth'], audio['codecs'],
                   audio['mime_type'], audio['sampling_rate'],
                   audio['segment_length'], audio['segment_sizes']))
        audio_inserted = curs.rowcount

        name, duration, *_ = self._videos[svtplay_id]
        logging.info("[%s] [%s] [%s] - stored %i video encoding(s) and "
                     "%i audio encoding",
                     svtplay_id, name, timedelta(seconds=duration),
                     videos_inserted, audio_inserted)

def active(video):
    return (datetime.strptime(video['valid_from'], '%Y-%m-%dT%H:%M:%S')
            < datetime.now()
            < datetime.strptime(video['valid_to'], '%Y-%m-%dT%H:%M:%S'))

def format_genre(genre):
    return {
        'id': genre['id'],
        'name': genre['name'].strip(),
        'description': genre['description'].strip()
        }

def format_episode(video, episode, episode_urls):
    svtplay_id = episode['videoSvtId']
    name = (video['name'].strip() + ': ' + episode['name'].strip()
            if video['name'] != episode['name']
            else video['name'].strip())
    return {
        'id': svtplay_id,
        'name': name,
        'duration': episode['duration'],
        'valid_from': episode['validFrom'][:19],
        'valid_to': episode['validTo'][:19],
        'sweden_only': episode['restrictions']['onlyAvailableInSweden'],
        'url': (video['urls']['svtplay']
                if video['urls']['svtplay'].startswith('/video/')
                else episode_urls.get(svtplay_id, '')),
        'short_description': episode['shortDescription'].strip(),
        'long_description': episode['longDescription'].strip(),
        'production_year': episode['productionYear'],
        'genres': tuple(genre['id'] for genre in episode['genres'])
        }
