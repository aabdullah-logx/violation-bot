import stores
import get_totp
import access_sc
import time
import os
import shutil
from datetime import datetime, timedelta
import sys
import pandas as pd
import settings

# Only import db if NOT running locally (db requires cloud setup)
if not getattr(settings, 'LOCAL', False):
    from db import get_distinct_storenames

# Import local_db when LOCAL_DB is enabled
if getattr(settings, 'LOCAL_DB', False):
    import local_db
    local_db.init_db()


def clear_temp_directory(path):
    try:
        for root, dirs, files in os.walk(path, topdown=False):
            # Remove files
            for name in files:
                file_path = os.path.join(root, name)
                try:
                    os.remove(file_path)
                except Exception as e:

                    print(f"Failed to delete {file_path}: {e}")

            # Remove directories
            for name in dirs:
                dir_path = os.path.join(root, name)
                try:
                    shutil.rmtree(dir_path)
                except Exception as e:
                    # pass
                    print(f"Failed to delete {dir_path}: {e}")
    except Exception as e:
        pass


def run():
    try:
        # ── Load store list ───────────────────────────────────────────────
        if getattr(settings, 'LOCAL', False):
            # LOCAL mode: read directly from stores.csv, no Google Sheets / QR needed
            try:
                df = pd.read_csv('stores.csv')
                df = df[df['status'] == 1]
                # active_df = pd.DataFrame()
                # print(active_df)
                print(f'[LOCAL] total stores: {len(df.index)}')
            except Exception as e:
                print(f'Error reading stores.csv: {e}')
                return None
        else:
            # Cloud mode: fetch from Google Sheets via stores.get_stores()
            try:
                df, drive, gc, store_sheet = stores.get_stores()
                # df = df[df['status'] == 'Active']
                print(f'[CLOUD] total stores: {len(df.index)}')
            except Exception as e:
                print(f'Error fetching stores from Google Sheets: {e}')
                return None

        # ── Process each store ────────────────────────────────────────────
        for index, store in df.iterrows():

            try:
                clear_temp_directory('C:\\Users\\Administrator\\AppData\\Local\\Temp\\2')
            except:
                pass

            # Unified column access — CSV uses 'profile_name', Sheets may use 'storename'
            storename = store.get('profile_name') or store.get('storename', 'Unknown')
            home_url = store.get('Amazon Home Page Link', '')

            print(f'Processing: {storename}')

            print(home_url)


            if not store.get('profile_id'):
                print(f"Skipping {storename} (no profile_id)")
                continue

            # ── QR / TOTP handling ────────────────────────────────────────
            if getattr(settings, 'LOCAL', False):
                # LOCAL: qr_key is already the plain TOTP secret in the CSV
                qr_key = store.get('qr_key', '')
                if not qr_key or (isinstance(qr_key, float)):
                    print(f'QR key missing for {storename}')
                    continue
                # Store the key directly on the row so signin() can use it
                store = store.copy()
                store['qr_key'] = qr_key
            else:
                # Cloud: QR code is a Google Drive image that must be decoded
                qr_url = store.get('QRCODE', '')
                if not qr_url:
                    print(f'QR URL missing for {storename}')
                    continue
                file_id = stores.extract_file_id_from_url(qr_url)
                if not file_id:
                    print(f'Cannot extract file ID from QR URL for {storename}')
                    continue
                image = stores.download_image_from_gdrive_and_load(file_id, drive, None, df, index)
                if image is None:
                    print(f'Failed to download QR image for {storename}')
                    continue
                qr_key = get_totp.generate_qr_key(image, None, df, index)
                if not qr_key:
                    print(f'Failed to decode QR key for {storename}')
                    continue
                store = store.copy()
                store['qr_key'] = qr_key

            # ── Launch browser & run automation ──────────────────────────
            store_success = False
            try:
                profile_id = store['profile_id']
                print(f'Processing-----------: {storename}')
                print(f'Processing----------------- {profile_id}')
                driver = access_sc.load_web_driver_with_gologin(profile_id)
                print("Driver loaded successfully")

                if driver:
                    try:
                        driver = access_sc.signin(store, driver, None, df, index)

                        # Violations / health metrics
                        try:
                            start_date = (datetime.now() - timedelta(days=21)).date()
                            print(f'Extracting from: {start_date} to today')

                            driver = access_sc.get_violations(
                                driver,
                                store,
                                # active_df,
                                start_date=start_date,
                                today=False
                            )
                            store_success = True
                        except Exception as e:
                            print(f'Error Policy Violation: {e}')

                    except Exception as e:
                        print(f'Error during signin/process: {e}')

                    access_sc.quit_driver(driver)

            except Exception as e:
                print(f'Error - Store "{storename}": {e}')

            # ── Update remark column in Google Sheet (cloud mode only) ────
            if not getattr(settings, 'LOCAL', False):
                try:
                    remark_col = df.columns.get_loc('remark') + 1
                    remark_text = "Completed" if store_success else "Failed"
                    store_sheet.update_cell(index + 2, remark_col, remark_text)
                    print(f'[REMARK] {storename}: {remark_text}')
                except Exception as e:
                    print(f'[REMARK] Failed to update remark for {storename}: {e}')

    except Exception as e:
        print(f'Fatal Error: {e}')


def main():
    while True:
        print(f"\n{'='*60}")
        print(f"Starting store processing at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        run()

        wait_hours = 12
        next_run_time = datetime.now() + timedelta(hours=wait_hours)
        print(f"\n{'='*60}")
        print(f"All stores completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Waiting {wait_hours} hours. Next run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        time.sleep(wait_hours * 3600)


if __name__ == '__main__':
    main()