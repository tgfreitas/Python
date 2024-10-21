import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient import discovery

def generate_google_token(token_path:str, client_secret_path:str, SCOPES:list=['https://www.googleapis.com/auth/spreadsheets']):
    '''
    Generates a Google OAuth2 token for accessing Google services.

    Args:
        token_path (str): The file path to store the generated token.
        client_secret_path (str): The file path to the client secret JSON file obtained from the Google Cloud Console.
        SCOPES (list): List of scopes required for the token. Defaults to ['https://www.googleapis.com/auth/spreadsheets'].

    Returns:
        None
    '''    
    creds = None
    if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    print('Valid credential created/present at:',token_path)