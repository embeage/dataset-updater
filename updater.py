import logging
import videos_downloader
import segments_downloader
import drive_uploader
from db import DB

def main():
    logging.basicConfig(
        filename='log.log',
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO)

    database = DB()
    try:
        videos_downloader.download(database)
        segments_downloader.download(database)
        database.export_csv('svtplay_db.csv')
        database.export_csv('svtplay_db_intl.csv', international=True)
        drive_uploader.upload()
    except Exception:
        logging.exception("Unexpected exception")
    finally:
        database.close()

if __name__ == "__main__":
    main()
