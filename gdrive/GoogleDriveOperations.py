from __future__ import print_function

import logging
import os.path
import pickle
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
import mimetypes

# If modifying these scopes, delete the file token.json to make a new one.
SCOPES = ['https://www.googleapis.com/auth/drive']


class GDrive():

    def __init__(self):
        """
        The constructor for GoogleAPI class

        :param kwargs: used for continuing a log file; specified by logFile - string
        """
        try:
            creds = None

            # The file token.pickle stores the user's access and refresh tokens and is
            # created automatically when the authorization flow completes for the first time
            os.chdir(os.path.dirname(sys.argv[0]))
            print(sys.argv[0])

            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    creds = pickle.load(token)

            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:

                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())

                else:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)

                # Save the credentials for the next run
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)

        except Exception as e:
            logging.warning('Google Drive/Sheets API could not be initialized.')
            logging.error(
                'ERROR INITIALIZING GOOGLE SERVICES. ' + str(e.args[1] if len(e.args) > 1 else e.args[0]).upper())
            raise e

        else:
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

    def uploadShareableFile(self, sourceFilePath, destinationFileName, **kwargs):
        # Function to upload file to Google Drive and share with everyone.
        # Params: sourceFilePath: file path of local file as string, 
        #         destinationFileName: file name in Google Drive as string
        #         optional parent folder path (parentPath=) or parent folder ID (parentID=) as string
        # Since there is no way to directly access a folder from its name/path with the Google Drive API, 
        # using folder ID (if known) is more efficient than folder path.
        # Shares file to anyone with link and returns source link to uploaded file 
        # (source link can be used as src for image in html)
        fid = self.uploadFile(sourceFilePath, destinationFileName, **kwargs)

        shared = self.service.permissions().create(fileId=fid, body={'role': 'reader', 'type': 'anyone'}).execute()

        return "https://drive.google.com/uc?export=view&id=" + fid

    def folderIDFromPath(self, pathToFolder):
        # Function to get folder id from path to folder 
        # Params: pathToFolder as string
        # (assumes no duplicate folder names within the same directory; uses first folder found)
        # Returns ID of folder

        path_items = pathToFolder.split('/')
        folder_id = 'root'

        for item in path_items:

            response = self.service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and name='{}' and '{}' in parents".format(item,
                                                                                                           folder_id),
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            if len(response.get('files', [])) == 0:
                raise Exception('Folder not found')

            file = response.get('files', [])[0]
            print('Found folder/file: %s (%s)' % (file.get('name'), file.get('id')))
            folder_id = file.get('id')

        return folder_id

    def getAllFiles(self, parentFolderID):
        # Function to get all file names and IDs from a folder
        # Param: parentFolderID as string
        # Returns dict of filename : fileID for all files in the folder
        files = {}
        response = self.service.files().list(
            q="parents in '{}'".format(parentFolderID),
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        for item in response.get('files', []):
            files[item.get('name')] = item.get('id')
        return files
