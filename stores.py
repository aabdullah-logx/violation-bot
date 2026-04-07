import sys
import time
import traceback

import gspread
import pandas as pd
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
# import server
import settings
from oauth2client import tools
import os
import datetime
# import access_sc
import pytz
import webbrowser

import requests
from io import BytesIO

import get_totp

import cv2
import re
import pyotp

# Get the current working directory
current_directory = os.getcwd()

# Construct the full path to your client_secret.json
CLIENT_SECRETS = os.path.join(current_directory, 'client_secret.json')

timezone = pytz.timezone('America/Toronto')


# Authentication for Google Sheets
def authenticate_gspread():
    SCOPE = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    STORAGE = Storage('sheets_oauth2.dat')

    credentials = STORAGE.get()
    if credentials is None or credentials.invalid:
        flow = flow_from_clientsecrets(CLIENT_SECRETS, scope=SCOPE)
        flags = tools.argparser.parse_args(args=[])
        credentials = tools.run_flow(flow, STORAGE, flags)

    gc = gspread.authorize(credentials)
    return gc


# Authentication for Google Drive
def authenticate_pydrive():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(CLIENT_SECRETS)
    gauth.LoadCredentialsFile("drive_oauth2.dat")

    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile("drive_oauth2.dat")
    drive = GoogleDrive(gauth)
    return drive


class MockObject:
    def __getattr__(self, name):
        def mock_method(*args, **kwargs):
            # print(f"Mocking {name}")
            return self
        return mock_method

def get_stores():
    if getattr(settings, 'LOCAL', False):
        print("Using local stores.csv (LOCAL is True in settings.py)")
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv('stores.csv')
        
        # Filter rows where status is 1
        df = df[df['status'] == "Active"]
        
        # Ensure columns expected by run.py exist
        if 'remark' not in df.columns:
            df['remark'] = ''
        if 'Collections' not in df.columns:
            df['Collections'] = ''
        if 'storename' not in df.columns:
            df['storename'] = df['profile_name']
        if 'QRCODE' not in df.columns and 'qr_key' in df.columns:
            df['QRCODE'] = df['qr_key']
        
        # Create mock objects to satisfy run.py's expected return values
        drive = MockObject()
        gc = MockObject()
        store = MockObject()
        active_list = MockObject()
        
        # Create a mock active_df or load it if available
        active_df = pd.DataFrame(columns=['storename']) # Dummy for now
        
        return df, drive, gc, store, active_list, active_df

    # Original Cloud Scaling logic
    gc = authenticate_gspread()
    drive = authenticate_pydrive()

    sh = gc.open("store_list")
    store = sh.worksheet("Collect Violations")
    # active_list = sh.worksheet("Account Health Metrics")
    data = store.get_all_records()
    df = pd.DataFrame(data)
    # active_data = active_list.get_all_records()
    # active_df = pd.DataFrame(active_data)
    
    return df, drive, gc, store


def download_image_and_save(url, save_path):
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            f.write(response.content)
        return True
    else:
        print(f"Failed to download image from {url}. Status code: {response.status_code}")
        return False



def download_image_from_gdrive_and_load(file_id, drive, gc_store, df, index,  temp_image_path='temp_image_QR.png'):
 
    try:
        # Download the file from Google Drive
        file = drive.CreateFile({'id': file_id})
        file.GetContentFile(temp_image_path)  # Save the file to a temporary path

        # Load the image with OpenCV
        image = cv2.imread(temp_image_path)

        # Optionally, remove the temporary file after loading
        os.remove(temp_image_path)

        return image
    except Exception as e:
        print(f"An error occurred while downloading or loading the image:OTP permission error")
        gc_store.update_cell(index + 2, df.columns.get_loc('remark') + 1, "An error occurred while downloading or loading the image: OTP permission error")
        return None


def extract_file_id_from_url(url):
    try:
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
        if match:
            return match.group(1)
        else:
            raise ValueError(f"Could not extract file ID from URL: {url}")
    except:
        return None


def main():
    df, drive, gc, store = get_stores()

    for index, store in df.iterrows():
        url = store['QRCODE']

        if url:
            file_id = extract_file_id_from_url(url)
        else:
            print('URL for TOTP not found')
            continue
        
        if file_id:
            image = download_image_from_gdrive_and_load(file_id, drive)
            # print(image)
        else:
            print('File id in TOTP Link not found')
            continue
        
        try:
            totp = get_totp.generate_code(image)
        except Exception as e:
            print(f'Error: {e}')
            store.update_cell(index + 2, df.columns.get_loc('remark') + 1, e)
            totp = None

        if totp:
            print(totp)

        break

    # print(df.head())

    sys.exit()
    # df['Link'] = df['OTP'].apply(lambda x: get_hyperlink_from_cell(store.find(x)))


if __name__ == '__main__':
    main()
