import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'service.json'
FILES = ('svtplay_db.csv', 'svtplay_db_intl.csv')

def create_readable_file(service, file_name):
    media = MediaFileUpload(file_name, mimetype='text/csv', resumable=True)
    file_metadata = {'name': file_name}
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id,size,webContentLink'
        ).execute()
    service.permissions().create(
        fileId=file.get('id'),
        body={'type': 'anyone', 'role': 'reader'}
        ).execute()
    logging.info("Uploaded new file %s (%s B) to Drive with link %s",
                 file_name, file.get('size'), file.get('webContentLink'))

def update_file_content(service, file_id, file_name):
    media_body = MediaFileUpload(file_name, mimetype='text/csv',
                                 resumable=True)
    file = service.files().update(
        fileId=file_id,
        media_body=media_body,
        fields='version,size'
        ).execute()
    logging.info("Updated %s content on Drive to version %s (%s B)",
                 file_name, file.get('version'), file.get('size'))

def get_files(service):
    results = service.files().list(
        pageSize=10,
        fields='nextPageToken, files(id, name)'
        ).execute()
    return results.get('files', [])

def upload():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    try:
        service = build('drive', 'v3', credentials=credentials,
                        cache_discovery=False)
        remote_files = get_files(service)
        for file in FILES:
            for remote_file in remote_files:
                if remote_file['name'] == file:
                    update_file_content(service, remote_file['id'], file)
                    break
            else:
                create_readable_file(service, file)
    except HttpError as ex:
        logging.error("Uploading to Drive failed because %s", ex)
