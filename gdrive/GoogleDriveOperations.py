from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
import mimetypes

# If modifying these scopes, delete the file token.json to make a new one.
SCOPES = ['https://www.googleapis.com/auth/drive']

class GDrive():
    
    def __init__(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        self.service = build('drive', 'v3', credentials=creds)

    def createFolder(self, folderName, **kwargs):
        # Function to create new folder 
        # (Note that duplicate folders may be created; Google Drive allows folders with same name to 
        # reside in the same directory since they have different IDs)
        # Params: folderName: name of folder as string, 
        #         optional parent folder path (parentPath=) or parent folder ID (parentID=) as string
        # Since there is no way to directly access a folder from its name/path with the Google Drive API, 
        # using folder ID (if known) is more efficient than folder path.
        # Returns ID of newly created folder

        # Default is top-level folder (if no parent folder info is given)
        file_metadata = {
                'name': folderName,
                'mimeType': 'application/vnd.google-apps.folder'
            }
        
        # If parent folder's ID is given, new folder will be created as subfolder under parent folder
        if kwargs.get('parentID'):
            file_metadata['parents'] = [kwargs.get('parentID')]
        
        # if name of folder given instead of ID, search for folder in Drive 
        # (will put under first folder found with matching name)
        elif kwargs.get('parentPath'): 
            file_metadata['parents'] = [self.folderIDFromPath(kwargs.get('parentPath'))]

        file = self.service.files().create(body=file_metadata, fields='id').execute()
        
        folder_id = file.get('id')
        print('Folder ID: %s' % folder_id)
        return folder_id

    def uploadFile(self, sourceFilePath, destinationFileName, **kwargs):
        # Function to upload file to Google Drive
        # Params: sourceFilePath: file path of local file as string, 
        #         destinationFileName: file name in Google Drive as string
        #         optional parent folder path (parentPath=) or parent folder ID (parentID=) as string
        # Since there is no way to directly access a folder from its name/path with the Google Drive API, 
        # using folder ID (if known) is more efficient than folder path.
        # Returns ID of uploaded file

        file_metadata = {'name': destinationFileName}

        # If parent folder's ID is given, file will be created as subfolder under parent folder
        if kwargs.get('parentID'):
            file_metadata['parents'] = [kwargs.get('parentID')]

        # if name of folder given instead of ID, search for folder in Drive 
        # (will put under first folder found with matching name)
        elif kwargs.get('parentPath'): 
            file_metadata['parents'] = [self.folderIDFromPath(kwargs.get('parentPath'))]

        fileType = mimetypes.guess_type(sourceFilePath)[0]
        if not fileType:
            raise Exception('File type invalid or could not be determined')

        media = MediaFileUpload(sourceFilePath, mimetype=fileType)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        print('File ID: %s' % file.get('id'))
        return file.get('id')

    def folderIDFromPath(self, pathToFolder):
        # Function to get folder id from path to folder 
        # Params: pathToFolder as string
        # (assumes no duplicate folder names within the same directory; uses first folder found)
        # Returns ID of folder

        path_items = pathToFolder.split('/')
        print(path_items)
        folder_id = 'root'

        for i in range(0, len(path_items)):
        
            response = self.service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and name='{}' and '{}' in parents".format(path_items[i], folder_id),
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            if len(response.get('files', [])) == 0:
                raise Exception('Folder not found')
            
            file = response.get('files', [])[0]
            print('Found folder/file: %s (%s)' % (file.get('name'), file.get('id')))
            folder_id = file.get('id')            

        return folder_id