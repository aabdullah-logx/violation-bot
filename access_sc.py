import torch
import os
import time
import math
import re
import inspect
import logging
from sys import platform
from datetime import datetime
import requests
import pandas as pd
from Tools.scripts.mkreal import join
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
# import beautifulsoup4
from gologin import GoLogin
import settings
import get_totp
import db
import local_db
import socket


def wait_for_port(host, port, timeout=30):
    print(f"Waiting for debug port {port}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.create_connection((host, port), timeout=1)
            sock.close()
            print(f"Port {port} is ready!")
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(1)
    print(f"Port {port} never opened!")
    return False


# Initialize local database if LOCAL_DB is enabled
if getattr(settings, 'LOCAL_DB', False):
    local_db.init_db()


# ── Logging setup ──────────────────────────────────────────────────────────────
gologin_path = os.path.abspath(inspect.getfile(GoLogin))
print(f"GoLogin module path: {gologin_path}")

current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = f'app_log_{current_time}.log'
logging.basicConfig(
    level=logging.INFO,
    filename=log_filename,
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# ── Browser launch ─────────────────────────────────────────────────────────────
def load_web_driver_with_gologin(profile_id):
    print('*********************************', profile_id)
    import tempfile
    import shutil
    import subprocess

    # ── Step 1: Stop the GoLogin profile via API FIRST to release file locks ──
    try:
        gl_cleanup = GoLogin({
            'token': settings.token,
            'profile_id': profile_id,
        })
        gl_cleanup.stop()
        print(f"Stopped GoLogin profile {profile_id} via API.")
        time.sleep(3)
    except Exception as e:
        print(f"GoLogin API stop (cleanup): {e}")

    # ── Step 2: Kill ALL processes that may hold locks on the profile ──
    if platform == "win32":
        import subprocess
        # Kill any process whose command line contains this profile_id
        # This catches orbita chrome.exe regardless of executable name
        try:
            ps_cmd = (
                f'Get-WmiObject Win32_Process | '
                f'Where-Object {{ $_.CommandLine -like "*{profile_id}*" }} | '
                f'ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}'
            )
            subprocess.run(["powershell", "-Command", ps_cmd], timeout=15, capture_output=True)
            print(f"Killed processes matching profile {profile_id}")
        except Exception as e:
            print(f"PowerShell process kill (profile): {e}")

        # Kill chrome.exe instances running from GoLogin's orbita-browser path
        # (GoLogin browser runs as chrome.exe under .gologin\browser\orbita-browser-*)
        try:
            ps_orbita = (
                'Get-WmiObject Win32_Process -Filter "Name=\'chrome.exe\'" | '
                'Where-Object { $_.CommandLine -like "*orbita*" -or $_.CommandLine -like "*gologin*" } | '
                'ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }'
            )
            subprocess.run(["powershell", "-Command", ps_orbita], timeout=15, capture_output=True)
            print("Killed orbita chrome.exe processes")
        except Exception as e:
            print(f"PowerShell process kill (orbita): {e}")

        # Kill Gologin desktop app and chromedriver
        for proc_name in ["Gologin.exe", "chromedriver.exe"]:
            try:
                subprocess.run(
                    f"taskkill /F /IM {proc_name} /T",
                    shell=True, timeout=10, capture_output=True
                )
            except:
                pass

        # Wait for processes to fully terminate and release file handles
        time.sleep(5)

    # ── Step 3: Force-delete the temp profile folder with retries ──
    import tempfile
    import shutil
    temp_dir = tempfile.gettempdir()
    profile_temp_path = os.path.join(temp_dir, f"gologin_{profile_id}")
    if os.path.exists(profile_temp_path):
        for attempt in range(5):
            try:
                shutil.rmtree(profile_temp_path)
                print("Cleaned profile temp data.")
                break
            except (PermissionError, OSError) as e:
                print(f"Temp folder locked (attempt {attempt + 1}/5): {e}")
                if platform == "win32":
                    # Try to find and kill whatever is locking files in the folder
                    try:
                        ps_lock = (
                            f'Get-Process | Where-Object {{ $_.Path -like "*orbita*" -or $_.Path -like "*gologin*" }} | '
                            f'Stop-Process -Force -ErrorAction SilentlyContinue'
                        )
                        subprocess.run(["powershell", "-Command", ps_lock], timeout=10, capture_output=True)
                    except:
                        pass
                time.sleep(3)
                # On last attempt, try force-delete via OS command
                if attempt == 4 and platform == "win32":
                    os.system(f'rmdir /s /q "{profile_temp_path}" > NUL 2>&1')
                    time.sleep(2)
            except Exception as e:
                print(f"Notice: Could not delete temp profile: {e}")
                break

    # ── Step 4: Create fresh GoLogin instance and start ──
    gl = GoLogin({
        'token': settings.token,
        'profile_id': profile_id,
        'extra_params': ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    })

    print('******************here')

    # Start GoLogin and capture the debugger address it returns
    debugger_address = gl.start()
    print(f'Debugger address: {debugger_address}')
    host, port = debugger_address.split(":")
    wait_for_port(host, int(port), timeout=30)

    time.sleep(5)

    # Connect Selenium to the GoLogin-managed browser
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", debugger_address)

    # Pick the correct chromedriver for the current OS
    if platform == "linux" or platform == "linux2":
        chrome_driver_path = "./chromedriver"
    elif platform == "darwin":
        chrome_driver_path = "./mac/chromedriver"
    elif platform == "win32":
        chrome_driver_path = "chromedriver.exe"

    print("Driver loaded")
    driver = webdriver.Chrome(
        service=Service(chrome_driver_path),
        options=chrome_options
    )
    print("Driver created successfully")
    time.sleep(15)
    # Attach GoLogin instance so it can be gracefully stopped later
    driver.gl_instance = gl
    print('started..........')

    try:
        driver.set_window_position(0, 0)
        driver.set_window_size(1280, 800)
        driver.maximize_window()
    except:
        pass
    # Close any extra windows to maintain only one active tab
    try:
        handles = driver.window_handles
        if len(handles) > 1:
            main_window = handles[0]
            for handle in handles[1:]:
                driver.switch_to.window(handle)
                driver.close()
            driver.switch_to.window(main_window)
    except Exception as e:
        print(f"Could not close extra tabs: {e}")
        try:
            _ = driver.current_url
        except:
            raise Exception("Browser connection closed immediately after startup")
        
    return driver



def load_driver(store, gc_store, df, index):
    """Try up to 5 times to launch the browser. Returns driver or None."""
    driver = None
    for x in range(5):
        try:
            driver = load_web_driver_with_gologin(store['profile_id'])
            print("Driver loaded successfully")
            break
        except Exception as e:
            print(f"Error: {e}")
            print("Failed to Load Web Driver")
            if gc_store:
                gc_store.update_cell(index + 2, df.columns.get_loc('remark') + 1, 'Failed to Load Web Driver')
            if x == 4:
                return None
            time.sleep(30)
    return driver


def quit_driver(driver):
    if driver is None:
        return
        
    gl_instance = getattr(driver, 'gl_instance', None)
    
    try:
        for window_handle in driver.window_handles:
            driver.switch_to.window(window_handle)
            driver.close()
    except:
        pass
        
    try:
        driver.quit()
    except:
        pass
        
    if gl_instance:
        try:
            gl_instance.stop()
            print("GoLogin profile properly stopped.")
        except:
            pass


# ── Authentication ─────────────────────────────────────────────────────────────
def sign_in_if_needed(driver, profile, gc_store, df, index):
    """
    Handles the full Amazon sign-in flow:
      1. Detects Sign in page
      2. Enters email -> clicks Continue
      3. Enters password -> clicks Sign In
      4. Detects OTP page -> auto-generates and enters 2FA code
      5. Selects Canada marketplace from account picker
    Returns True if sign-in succeeded, False if credentials failed.
    """
    try:
        # Step 1: Wait for Sign in page
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//h1[contains(text(), "Sign in")]'))
        )

        # Step 2: Enter email
        time.sleep(10)
        try:
            email_input = driver.find_element(By.ID, "ap_email")
            email_input.clear()
            email_input.send_keys(profile['email'])
        except:
            pass

        # Click Continue (email and password are on separate pages)
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "continue")))
            driver.find_element(By.ID, 'continue').click()
        except:
            pass

        time.sleep(10)

        # Step 3: Enter password
        password_input = WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.ID, "ap_password"))
        )
        time.sleep(1)

        password_input.clear()
        time.sleep(1)
        password_input.send_keys(profile['pass'])
        time.sleep(1)
        password_input.clear()
        time.sleep(1)
        password_input.send_keys(profile['pass'])
        time.sleep(1)
        driver.find_element(By.ID, 'signInSubmit').click()
        # if password_input.text:
        #     password_input.clear()
        #     # time.sleep(2)
        #     # password_input.send_keys(profile['pass'])
        #     driver.find_element(By.ID, 'signInSubmit').click()
        # elif password_input is None:
        #     password_input.send_keys(profile['pass'])
        #     driver.find_element(By.ID, 'signInSubmit').click()
        # Check for wrong credentials
        try:
            WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.ID, "auth-error-message-box"))
            )
            print(f"Wrong credentials for {profile['storename']}")
            logging.error(f"Wrong credentials for {profile['storename']}")
            return False
        except:
            pass

        time.sleep(7)

        # Step 4: OTP / Two-Step Verification
        try:
            otp_input = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "auth-mfa-otpcode"))
            )
            otp_code = get_totp.generate_2fa_code(profile['qr_key'])
            otp_input.send_keys(otp_code)
            driver.find_element(By.ID, 'auth-signin-button').click()

            # Check if OTP was rejected
            try:
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.ID, "auth-error-message-box"))
                )
                print("OTP entered was not valid.")
                logging.error(f"TOTP is not valid for {profile['storename']}")
                return False
            except:
                pass

        except Exception as e:
            print("No OTP page detected, or other error: ", e)
            if gc_store:
                gc_store.update_cell(index + 2, df.columns.get_loc('remark') + 1, "No OTP page detected")

        # Step 5: Account / marketplace picker
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        try:
            # Try clicking Canada directly
            canada_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//div[contains(@class,'picker-info-container')]//div[contains(text(),'Canada')]"
                ))
            )
            canada_btn.click()

        except:
            # Some accounts need to select the sub-account first
            print(profile['storename'])
            account_select = pd.DataFrame({
                'A': ['Reticulum Star', 'Macros Shop', 'Holistic Ave', 'Galaxy Glow',
                      'DVDMagnet', 'Yes-Shop', 'Antlia star', 'Browse It', 'Capricornuss', 'FITIFO CA'],
                'B': ['ENNEFU', 'ZOFOW', 'Holistic Ave', 'greivaumatralle', 'seda 97',
                      'SW Hair Designs', 'RAMZON', 'WONDERFULCN', 'Capricornuss', 'FITIFO CA']
            })
            ac = None
            for _, row in account_select.iterrows():
                if row['A'] == profile['storename']:
                    ac = row['B']
                    break

            if ac:
                sub_btn = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f"//div[contains(@class,'picker-info-container')]//div[contains(text(),'{ac}')]"
                    ))
                )
                sub_btn.click()

                canada_btn = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "//div[contains(@class,'picker-info-container')]//div[contains(text(),'Canada')]"
                    ))
                )
                canada_btn.click()

        # Click the final Select Account button
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(@class,'picker-switch-accounts-button')]"))
        ).click()

    except Exception as e:
        print(f"Error during sign in: {e}")
        if gc_store:
            gc_store.update_cell(index + 2, df.columns.get_loc('remark') + 1, "Error when signing in")

    return True


def check_switch_account(driver, profile):
    """
    If Amazon shows a Switch accounts page after login,
    find the matching email row and re-enter the password.
    """
    try:
        switch_header = driver.find_element(By.XPATH, '//h1[text()="Switch accounts"]')
        if switch_header:
            account_rows = driver.find_elements(By.XPATH, '//div[@class="a-fixed-left-grid-inner"]')
            for row in account_rows:
                try:
                    email_xpath = (
                        f'.//div[contains(@class,"cvf-account-switcher-claim") and '
                        f'contains(translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ",'
                        f'"abcdefghijklmnopqrstuvwxyz"),"{profile["email"].lower()}")]'
                    )
                    email_el = row.find_element(By.XPATH, email_xpath)
                    if email_el:
                        name_el = row.find_element(
                            By.XPATH,
                            './/div[contains(@class,"cvf-text-truncate") and '
                            'not(contains(@class,"cvf-account-switcher-profile-business-name"))]'
                        )
                        name_el.click()

                        password_input = WebDriverWait(driver, 20).until(
                            EC.visibility_of_element_located((By.ID, "ap_password"))
                        )
                        password_input.click()
                        password_input.send_keys(profile['pass'])
                        driver.find_element(By.ID, "signInSubmit").click()
                        time.sleep(5)
                        break
                except NoSuchElementException:
                    continue
    except NoSuchElementException:
        pass


def check_full_page_account_switcher(driver):
    """
    If Amazon shows the full page 'Select an account' after login,
    select Canada and click 'Select account'.
    Handles two scenarios:
      1. Canada is directly visible as a top-level account option.
      2. Canada is nested under a parent dropdown (e.g. BYJ-US) that
         must be expanded first.
    """
    try:
        time.sleep(3)
        h1 = driver.find_elements(By.XPATH, '//h1[normalize-space(text())="Select an account"]')
        if not h1:
            return
        print("Full page account switcher detected. Selecting Canada...")

        canada_found = False

        # ── Attempt 1: Canada is directly visible as a top-level option ──
        try:
            canada_btn = driver.find_element(
                By.XPATH,
                "//button[.//span[contains(@class, 'full-page-account-switcher-account-label') and normalize-space(text())='Canada']]"
            )
            print("Canada found directly. Clicking...")
            driver.execute_script("arguments[0].click();", canada_btn)
            canada_found = True
        except NoSuchElementException:
            print("Canada not found directly. Checking parent dropdowns...")

        # ── Attempt 2: Expand parent account dropdowns to reveal Canada ──
        if not canada_found:
            # Find all top-level account dropdown buttons (e.g. BYJ-US, BYJ-EU)
            account_btns = driver.find_elements(
                By.CSS_SELECTOR,
                "div.full-page-account-switcher-account button.full-page-account-switcher-account-details"
            )
            print(f"Found {len(account_btns)} parent account dropdown(s)")

            for btn in account_btns:
                try:
                    label_el = btn.find_element(
                        By.CSS_SELECTOR,
                        "span.full-page-account-switcher-account-label"
                    )
                    label_text = label_el.text.strip()
                    print(f"  Expanding dropdown: {label_text}")
                except:
                    label_text = "(unknown)"

                driver.execute_script("arguments[0].click();", btn)
                time.sleep(2)

                # After expanding, look for Canada inside the revealed sub-items
                try:
                    canada_btn = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            "//button[.//span[contains(@class, 'full-page-account-switcher-account-label') and normalize-space(text())='Canada']]"
                        ))
                    )
                    print(f"  Canada found under '{label_text}'. Clicking...")
                    driver.execute_script("arguments[0].click();", canada_btn)
                    canada_found = True
                    break
                except (NoSuchElementException, TimeoutException):
                    print(f"  Canada not under '{label_text}', trying next...")
                    # Collapse this dropdown before trying the next
                    try:
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(1)
                    except:
                        pass

        if not canada_found:
            print("WARNING: Could not find Canada in any dropdown!")
            return

        time.sleep(3)

        # ── Click "Select account" button ──
        select_btn = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//kat-button[@data-test='confirm-selection' or @label='Select account']"
            ))
        )
        driver.execute_script("arguments[0].click();", select_btn)
        print("Clicked 'Select account'. Waiting for page to load...")
        time.sleep(10)

    except Exception as e:
        print(f"Error in full page account switcher: {e}")


def check_pre_signin_account_switcher(driver):
    """
    If Amazon shows the 'Switch accounts' page BEFORE the sign-in page,
    click the first listed account to proceed, then wait for the sign-in
    page to load so email/password can be entered.
    """
    try:
        switch_header = driver.find_elements(By.XPATH, '//h1[normalize-space(text())="Switch accounts"]')
        if switch_header:
            print("Pre-sign-in 'Switch accounts' page detected. Clicking first account...")
            # Click the first account button (not the 'Add account' link)
            first_account_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    'a.cvf-widget-btn-verify-account-switcher'
                ))
            )
            first_account_btn.click()
            print("Clicked first account. Waiting 3 seconds...")
            time.sleep(3)
            return True
    except Exception as e:
        print(f"No pre-sign-in account switcher or error: {e}")
    return False


def signin(store, driver, gc_store, df, index):
    """
    Main entry point for authentication.
    Navigates to the store home URL, retries on Server Busy,
    detects sign-in page, completes full auth flow.
    Returns the authenticated driver, or None on failure.
    """
    wait = WebDriverWait(driver, 5)

    # Navigate to home page, retry if Server Busy
    while True:
        try:
            driver.get(store['Amazon Home Page Link'])
            time.sleep(5)

            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//h4[contains(text(),'Server Busy')]")
            ))
            print("Server Busy page detected. Retrying...")
            time.sleep(1)

        except TimeoutException:
            # Check if the "Switch accounts" page appears before sign-in
            if check_pre_signin_account_switcher(driver):
                print("Account selected from switcher, proceeding to sign-in...")
                # After clicking the account, the sign-in page should load
                # Fall through to the sign-in flow below
                break

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.a-spacing-small")))
                print("Sign in page detected.")
                break
            except TimeoutException:
                time.sleep(2)
                print("No sign in needed")
                return driver

        except Exception as e:
            print(f"Unexpected error navigating to home: {e}")
            break

    # Complete sign in
    try:
        sign_in_ok = sign_in_if_needed(driver, store, gc_store, df, index)
        if sign_in_ok:
            check_switch_account(driver, store)
            check_full_page_account_switcher(driver)
        else:
            quit_driver(driver)
            return None
    except Exception:
        try:
            quit_driver(driver)
        except:
            pass
        return None

    if driver:
        return driver
    else:
        try:
            quit_driver(driver)
        except:
            pass
        return None


# ── QuickBase helpers ──────────────────────────────────────────────────────────
def insert_into_quickbase_x(data_list, violations_list=None):
    if getattr(settings, 'LOCAL_DB', False):
        local_db.insert_health_metrics(data_list)
        if violations_list:
            local_db.insert_violations(violations_list)
        return

    realm = 'cbms'
    api_token = 'b8hbe5_bmq8_0_dmse3m92jctvftc8d47diqdmbd'
    url = "https://api.quickbase.com/v1/records"
    headers = {
        'QB-Realm-Hostname': f'{realm}.quickbase.com',
        'User-Agent': 'bt9cnvf2m',
        'Authorization': f'QB-USER-TOKEN {api_token}'
    }

    # ── Health Metrics ──────────────────────────────────────────
    if data_list:
        try:
            r = requests.post(url, headers=headers, json={
                "to": "bt935dtsk",
                "data": data_list,
                "fieldsToReturn": [7, 17]
            })
            print(f"QB health metrics status: {r.status_code}")
        except Exception as e:
            print(f'ERROR (health metrics): {e}')

    # ── Violations ──────────────────────────────────────────────
    if violations_list:
        # ── Duplicate check: skip violations whose ASIN already exists ──
        new_violations = []
        skipped_count = 0
        for v in violations_list:
            v_asin = v.get('asin', '')
            if v_asin:
                try:
                    query_body = {
                        "from": "buab36fis",
                        "select": [3],
                        "where": f"{{'8'.EX.'{v_asin}'}}"
                    }
                    qr = requests.post(
                        "https://api.quickbase.com/v1/records/query",
                        headers=headers, json=query_body
                    )
                    existing = qr.json().get('data', [])
                    if existing:
                        print(f"  [QB] Skipping existing ASIN: {v_asin}")
                        skipped_count += 1
                        continue
                except Exception as e:
                    print(f"  [QB] Duplicate-check failed for {v_asin}, will insert anyway: {e}")
            new_violations.append(v)

        print(f"  [QB] Violations: {len(new_violations)} new, {skipped_count} skipped (already exist)")

        if not new_violations:
            print("  [QB] No new violations to insert.")
            return

        prepared = []
        for v in new_violations:
            date_value = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

            # Format S.C Date for QuickBase
            # Format S.C Date for QuickBase
            sc_date = ''
            publish_time = v.get('publish_time')

            if publish_time:
                # Format to MM-DD-YYYY because Quickbase evaluates ToDate([S.C Date], "MM-DD-YYYY")
                if hasattr(publish_time, 'strftime'):
                    sc_date = publish_time.strftime('%m-%d-%Y')
                else:
                    # If it happens to be string, we can't reliably format it
                    # but we keep it safe.
                    sc_date = str(publish_time)[:10]

            print(f"  [QB] ASIN: {v.get('asin', '')} | S.C Date: {sc_date} | publish_time raw: {publish_time}")

            prepared.append({
                '6': {'value': date_value},
                '7': {'value': v.get('storename', '')},
                '8': {'value': v.get('asin', '')},
                '9': {'value': ''},
                '10': {'value': v.get('category', '')},
                '11': {'value': v.get('reason', '')},
                '12': {'value': ''},
                '13': {'value': v.get('impact', '')},
                '15': {'value': sc_date},
            })
        try:
            r = requests.post(url, headers=headers, json={
                "to": "buab36fis",
                "data": prepared,
                "fieldsToReturn": ["3"]
            })
            print(f"QB violations status: {r.status_code}")
            print(r.content)
        except Exception as e:
            print(f'ERROR (violations): {e}')



def insert_into_quickbase(data_list):
    print("A Z Claims QB insert")

    # ── LOCAL_DB: store in local SQLite instead of QuickBase ──
    if getattr(settings, 'LOCAL_DB', False):
        local_db.insert_az_claims(data_list)
        return

    realm = 'cbms'
    dbid = "btyyfj2fy"
    api_token = 'b8hbe5_bmq8_0_c2wqnuhbadf87bhd2i5tdcja4z9'
    url = "https://api.quickbase.com/v1/records"
    headers = {
        'QB-Realm-Hostname': f'{realm}.quickbase.com',
        'User-Agent': 'btsubaqig',
        'Authorization': f'QB-USER-TOKEN {api_token}'
    }

    def replace_nan_with_none(d):
        for key in d:
            if isinstance(d[key], float) and math.isnan(d[key]):
                d[key] = None
        return d

    prepared_data = {"to": dbid, "data": [], "fieldsToReturn": ["3"]}
    for record in data_list:
        record = replace_nan_with_none(record)
        prepared_data["data"].append({str(k): {"value": v} for k, v in record.items()})
    try:
        r = requests.post(url, headers=headers, json=prepared_data)
        print(f"QB insert status code (A Z Claims) {r.status_code}")
    except Exception as e:
        print(f'ERROR: {e}')


def update_az_claims(az_claim_dict_list):
    print("A Z Claims QB update")

    # ── LOCAL_DB: update in local SQLite instead of QuickBase ──
    if getattr(settings, 'LOCAL_DB', False):
        local_db.update_az_claims_local(az_claim_dict_list)
        return "Update Completed"

    realm = 'cbms'
    dbid = "btyyfj2fy"
    api_token = 'b8hbe5_bmq8_0_c2wqnuhbadf87bhd2i5tdcja4z9'
    headers = {
        'QB-Realm-Hostname': f'{realm}.quickbase.com',
        'User-Agent': 'btsubaqig',
        'Authorization': f'QB-USER-TOKEN {api_token}',
        'Content-Type': 'application/json'
    }
    for claim in az_claim_dict_list:
        order_asin = f"{claim[9]}-{claim[10]}"
        query = {"from": dbid, "select": [3], "where": f"{{'15'.EX.'{order_asin}'}}"}
        try:
            response = requests.post("https://api.quickbase.com/v1/records/query", headers=headers, json=query)
            records = response.json().get('data', [])
        except Exception as e:
            print(f'Error: Failed to get Record ID {e}')
            continue
        for record in records:
            record_id = record['3']['value']
            update_fields = {str(k): {"value": v} for k, v in claim.items() if k not in [9, 10]}
            update_fields["3"] = {"value": record_id}
            try:
                requests.post("https://api.quickbase.com/v1/records", headers=headers,
                              json={"to": dbid, "data": [update_fields]})
            except Exception as e:
                print(f'Error: Not able to update record {e}')
    return "Update Completed"






# ── TODO: Rewrite below for new Amazon UI ─────────────────────────────────────
def get_violations(driver, store, start_date, today=False):
    if not driver:
        return None

    if store['Collections'] not in ('Violation + Health Metrics', 'Health Metrics Collect'):
        return driver

    current_date = datetime.now().date()

    # ── Account Health + Buybox + Balance ─────────────────────────────────────
    # time.sleep(10)
    print("Collecting account balance & buybox rate")

    time.sleep(5)


    try:
        balance = driver.find_element(By.CSS_SELECTOR, "#KPI_CARD_PAYMENTS casino-currency div").text
        print(f"Balance: {balance}")
    except Exception as e:
        print(e)
        balance = "0"


    try:
        buybox = driver.find_element(By.CSS_SELECTOR, "#KPI_CARD_BUYBOX casino-plain-text div").text
        buybox = buybox.replace('%', '')
        buybox = int(buybox)
        print(f"Buybox: {buybox}")
    except Exception as e:
        print(e)
        buybox = "0"




    print("Before BaseURL")
    try:
        base_url = store['Amazon Home Page Link'].replace('home', '')
        print(base_url)
        health_url = base_url + '/performance/dashboard'
        print(health_url)
        print(f'Processing health: {store["storename"]}')

    except Exception as e:
        # print(e)
        pass
    # print("BaseURL")
    driver.get(health_url)
    time.sleep(6)

    try:
        # Check for "Switch accounts" page before sign-in
        if driver.find_elements(By.XPATH, '//h1[normalize-space(text())="Switch accounts"]'):
            print("Switch accounts page detected on health page. Clicking first account...")
            check_pre_signin_account_switcher(driver)

        if driver.find_elements(By.XPATH, '//h1[contains(text(), "Sign in")]') or driver.find_elements(By.CSS_SELECTOR, "h1.a-spacing-small"):
            if "Sign in" in driver.page_source:
                print("Sign in required on health page. Re-authenticating...")
                sign_in_if_needed(driver, store, None, None, None)
                check_switch_account(driver, store)
                check_full_page_account_switcher(driver)
                time.sleep(5)
                # Re-navigate to the health dashboard after sign in
                driver.get(health_url)
                time.sleep(6)
    except Exception as e:
        print(f"Error during sign-in on health page: {e}")

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".a-box-group"))
        )
    except:
        pass

    time.sleep(3)

    # Health status
    health_row = 'Not found!'
    for css in [".ahr-status-badge-great", ".ahr-status-badge-critical",
                ".ahr-status-badge-at-risk"]:
        try:
            health_row = driver.find_element(By.CSS_SELECTOR, css).text
            break
        except:
            pass
    if health_row == 'Not found!':
        try:
            driver.find_element(By.CSS_SELECTOR, ".ahd-ahr-status-green")
            health_row = 'Healthy'
        except:
            pass

    account_health_rating = 'Not found!'
    odr = nf = az = cbc = lsr = sc = vtr = '0'

    if health_row != 'Not found!':
        try:
            rating = driver.find_element(By.CSS_SELECTOR, ".ahd-numeric-ahr-indicator")
            rating = rating.find_element(By.CSS_SELECTOR, ".a-ws-span-last")
            account_health_rating = rating.find_element(By.TAG_NAME, "h3").text
        except:
            pass

        try:
            csp = driver.find_element(By.ID, "customer-satisfaction-content-rows-section")
            try:
                odr = csp.find_element(By.CSS_SELECTOR, ".sp-summary-row") \
                         .find_element(By.CSS_SELECTOR, ".a-size-large").text
            except:
                odr = '0'
            breakdown = csp.find_element(By.ID, "odr-breakdown-section")
            rows = breakdown.find_elements(By.CSS_SELECTOR, ".sp-middle-col")
            nf  = rows[0].text
            az  = rows[1].text
            cbc = rows[2].text
        except:
            pass

        try:
            late = driver.find_element(By.ID, "shipping-late-shipment-rate-row")
            lsr = late.find_element(By.CSS_SELECTOR, ".a-ws-span-last") \
                      .find_element(By.CSS_SELECTOR, ".a-spacing-none") \
                      .find_element(By.CSS_SELECTOR, ".a-size-large").text
        except:
            lsr = '0'

        try:
            ship_cancel = driver.find_element(By.ID, "shipping-cancellation-rate-row")
            sc = ship_cancel.find_element(By.CSS_SELECTOR, ".a-ws-span-last") \
                            .find_element(By.CSS_SELECTOR, ".a-spacing-none") \
                            .find_element(By.CSS_SELECTOR, ".a-size-large").text
        except:
            sc = '0'

        try:
            vtr_el = driver.find_element(By.ID, "shipping-view-tracking-rate-row")
            vtr = vtr_el.find_element(By.CSS_SELECTOR, ".a-ws-span-last") \
                        .find_element(By.CSS_SELECTOR, ".a-spacing-none") \
                        .find_element(By.CSS_SELECTOR, ".a-size-large").text
        except:
            vtr = '0'

        for val, name in [(nf,'nf'),(az,'az'),(cbc,'cbc'),(buybox,'buybox')]:
            if val == 'N/A':
                val = '0'

        nf  = '0' if nf  == 'N/A' else nf
        az  = '0' if az  == 'N/A' else az
        cbc = '0' if cbc == 'N/A' else cbc
        buybox = '0' if buybox == '--' else buybox

        print(f'Health: {health_row} | Rating: {account_health_rating} | ODR: {odr}')
        print(f'NF: {nf} | AZ: {az} | CBC: {cbc} | LSR: {lsr} | SC: {sc} | VTR: {vtr}')
        # print(store["storename"])
        store_name = store.get('profile_name', store.get('storename', 'Unknown'))
        print(store_name)


        try:
            update_data = [{
                '6': {'value': "today"},
                '7': {'value': store_name},
                '8': {'value': health_row},
                '9': {'value': account_health_rating},
                '10': {'value': odr.replace('%', '')},
                '11': {'value': vtr.replace('%', '')},
                '12': {'value': buybox if isinstance(buybox, (int, float)) else 0},
                '13': {'value': float(balance.replace('$', '').replace(',', ''))},
                '14': {'value': nf.replace('%', '')},
                '15': {'value': az.replace('%', '')},
                '16': {'value': cbc.replace('%', '')},
                '17': {'value': lsr.replace('%', '')},
                '18': {'value': sc.replace('%', '')}

            }]
            # print("Store keys:", store.keys())
            print("Sending to Database", update_data)
            insert_into_quickbase_x(update_data)
        except Exception as e:
            print(f'{e}')

        health = {
            "storename": store_name,
            "status": health_row,
            "health_rating": account_health_rating,
            "odr": odr, "vtr": vtr, "buybox": buybox, "balance": balance,
            "negative_feedback": nf, "a_to_z_claims": az,
            "chargeback_claims": cbc, "late_shipment_rate": lsr,
            "pre_fulfilment_cancel_rate": sc,
        }
        if getattr(settings, 'LOCAL_DB', False):
            # local_db.insert_health(health)
            pass
        else:
            # db.insert_health(health)
            pass
        print("Health Stored")
    # ── Violations ────────────────────────────────────────────────────────────
    if store['Collections'] != 'Violation + Health Metrics':
        return driver

    # Date format used in the new UI: "Apr. 3, 2026" or "Feb. 28, 2026" or "Apr 3, 2026"
    date_formats = ["%b. %d, %Y", "%b %d, %Y", "%B %d, %Y", "%B. %d, %Y",
                    "%b. %d %Y", "%b %d %Y", "%m/%d/%Y", "%Y-%m-%d"]

    def parse_violation_date(date_str):
        if not date_str:
            return None
        cleaned = date_str.strip()
        for fmt in date_formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        # Regex fallback: extract "Mon DD, YYYY" or "Mon. DD, YYYY"
        m = re.match(r'([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})', cleaned)
        if m:
            try:
                return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y")
            except ValueError:
                pass
        print(f"  [DATE] Failed to parse: '{date_str}'")
        return None

    print("Starting Violation...")
    for category, url_path in settings.policy_violation_url_list.items():
        print(f"\n{'='*60}")
        violation_url = settings.base_url + url_path
        print(f'Processing: {store.get("profile_name", store.get("storename", "Unknown"))} - {category}')
        print(f'URL: {violation_url}')

        driver.get(violation_url)
        time.sleep(8)

        try:
            if driver.find_elements(By.XPATH, '//h1[contains(text(), "Sign in")]') or driver.find_elements(By.CSS_SELECTOR, "h1.a-spacing-small"):
                if "Sign in" in driver.page_source:
                    print("Sign in required on violation page. Re-authenticating...")
                    sign_in_if_needed(driver, store, None, None, None)
                    check_switch_account(driver, store)
                    check_full_page_account_switcher(driver)
                    time.sleep(5)
                    # Re-navigate to the violation URL after sign in
                    driver.get(violation_url)
                    time.sleep(8)
        except Exception as e:
            print(f"Error during sign-in on violation page: {e}")

        violations = []
        skip_category = False
        current_page = 1

        while True:
            # ── Wait for the page container to load ───────────────────────
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '#ahd-product-policy-page')
                    )
                )
            except TimeoutException:
                print(f"  Page did not load for {category}")
                break

            # ── Poll for violation cards via innerHTML of the table ────────
            # GoLogin/Orbita Chromium can't do document.querySelectorAll
            # through Declarative Shadow DOM custom elements (kat-tabs).
            # Instead: find #ahd-product-policies-table by ID, check innerHTML.
            table_html_len = 0
            max_polls = 15
            for poll in range(max_polls):
                table_html_len = driver.execute_script("""
                    var t = document.getElementById('ahd-product-policies-table');
                    return t ? t.innerHTML.length : -1;
                """)
                if table_html_len > 100:
                    print(f"  Table content loaded after ~{(poll)*3 + 8}s (innerHTML={table_html_len} chars)")
                    break
                if poll == 0:
                    sub_title = driver.execute_script("""
                        var el = document.querySelector('[data-testid="ahd-pp-sub-title"]');
                        return el ? el.textContent.trim() : '';
                    """) or ''
                    print(f"  Waiting for cards... (subtitle: {sub_title})")
                time.sleep(3)
            else:
                print(f"  Timed out (45s). table innerHTML length={table_html_len}")

            # ── Extract violation data using getElementById as root ────────
            cards_data = driver.execute_script("""
                // Get the table container by ID (bypasses shadow DOM traversal)
                var table = document.getElementById('ahd-product-policies-table');
                if (!table) return {cards: [], debug: 'no table element', tableId: false};

                // Query for mobile cards or desktop rows
                var cards = table.querySelectorAll(
                    'div[data-testid="ahd-mobile-product-policy"], .mobile-product-policy-cards-container, kat-table-row, div[role="row"], tr'
                );
                
                if (!cards || cards.length === 0) {
                    // Last resort: direct children
                    cards = table.children;
                }
                
                var results = [];
                var skipped = 0;
                var errors = 0;

                for (var i = 0; i < cards.length; i++) {
                    try {
                        var card = cards[i];
                        var asin = '';
                        var cardHtml = card.innerHTML || '';
                        var cardText = card.textContent || '';
                        
                        // Ignore header rows
                        if (cardText.includes('Product Policy Compliance') || card.tagName === 'TH' || card.querySelector('th')) {
                           continue;
                        }

                        // Method 1: hidden input in form
                        var asinInput = card.querySelector('input[name="asin"]');
                        if (asinInput) asin = asinInput.value || '';

                        // Method 2: regex on innerHTML
                        if (!asin) {
                            // look for value="B0XXXXX" or similar
                            var m = cardHtml.match(/name="asin"\s+value="([A-Z0-9]{10})"/);
                            if (m) asin = m[1];
                        }
                        if (!asin) {
                            var m = cardHtml.match(/asin=([A-Z0-9]{10})/);
                            if (m) asin = m[1];
                        }
                        if (!asin) {
                            var m = cardHtml.match(/>\s*([A-Z0-9]{10})\s*</);
                            if (m) asin = m[1];
                        }
                        // Method 3: Any 10 char alphanumeric starting with B or numbers 
                        if (!asin) {
                            var m = cardText.match(/\b([B0-9][A-Z0-9]{9})\b/);
                            if (m) asin = m[1];
                        }

                        // If still no ASIN, skip
                        if (!asin) { skipped++; continue; }

                        // Extract Date using regex on text content
                        var dateStr = '';
                        // Usually like "Apr 3, 2026" or "Apr. 3, 2026"
                        var dateMatch = cardText.match(/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+20\d{2}/i);
                        if (dateMatch) {
                            dateStr = dateMatch[0].trim();
                        } else {
                            // Try DD MMM YYYY
                            var dateMatch2 = cardText.match(/\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+20\d{2}/i);
                            if (dateMatch2) dateStr = dateMatch2[0].trim();
                        }

                        // Extract Reason, Action Taken, Health Impact
                        var actionTaken = '', healthImpact = '', reason = '';

                        // In desktop, often we have kat-table-cell elements.
                        var cells = card.querySelectorAll('kat-table-cell, td, div[role="cell"]');
                        if (cells.length >= 3) {
                            // By default, Amazon Desktop Policy Tables usually map to:
                            // [0] Product info / ASIN
                            // [1] Date & Reason (or just Reason)
                            // [2] Next Steps / Action Taken
                            // [3] Impact (Sometimes)
                            // Let's use positional defaults:
                            reason = cells[1] ? cells[1].textContent.trim() : '';
                            if (cells.length >= 4) {
                                actionTaken = cells[2].textContent.trim();
                                healthImpact = cells[3].textContent.trim();
                            } else {
                                actionTaken = cells[2] ? cells[2].textContent.trim() : '';
                            }
                            
                            // Refine using keywords in all cells
                            for(var j=0; j<cells.length; j++) {
                                var cellText = cells[j].textContent.trim();
                                // Health Impact keywords
                                if (/^(?:High|Low|Medium|Critical|No impact)\s*(?:Impact)?$/i.test(cellText) || cellText.includes('Account health impact')) {
                                    healthImpact = cellText.replace(/^Account health impact/i, '').trim() || cellText;
                                }
                                // Reason keywords
                                if (/Complaint|Violation|Issues|Policy|Inauthentic|Safety|Infringement/i.test(cellText) && cellText.length < 150) {
                                    // if it's the product column, don't overwrite
                                    if (j !== 0) reason = cellText;
                                }
                                // Action taken keywords
                                if (/removed|deactivated|warning|appeal|submit|review|address|acknowledge|reactivate/i.test(cellText) && cellText.length < 150) {
                                    if (j !== 0 && j !== 1) actionTaken = cellText;
                                }
                            }
                        } else {
                           // Mobile data rows extraction
                           var dataRows = card.querySelectorAll('.mobile-violation-data');
                           if (dataRows.length >= 2) {
                               var actionCols = dataRows[1].querySelectorAll('div[class*="kat-col"]');
                               if (actionCols.length > 0) actionTaken = actionCols[0].textContent.trim();
                               if (actionCols.length > 1) healthImpact = actionCols[1].textContent.trim();
                           }
                        }
                        
                        // Forcefully extract Action Taken from Action Buttons if present
                        var actionBtns = card.querySelectorAll('kat-button, [data-testid*="action"], [id*="action"]');
                        if (actionBtns.length > 0) {
                            var acts = [];
                            for(var b=0; b<actionBtns.length; b++) {
                                var txt = actionBtns[b].textContent.trim();
                                if (txt && !acts.includes(txt) && !/details/i.test(txt)) {
                                    acts.push(txt);
                                }
                            }
                            if (acts.length > 0) {
                                // Prefer explicit button texts over cell text
                                actionTaken = acts.join(' | ');
                            }
                        }

                        // Reason from kat-link or exact testid
                        var reasonLink = card.querySelector('kat-link[data-testid*="reason"]');
                        if (reasonLink) {
                            var spans = reasonLink.querySelectorAll(':scope > span > span');
                            if (spans.length > 0) {
                                reason = spans[0].textContent.trim();
                            } else {
                                reason = reasonLink.textContent.trim();
                            }
                        } else if (card.querySelector('[data-testid*="reason"]')) {
                            reason = card.querySelector('[data-testid*="reason"]').textContent.trim();
                        }
                        
                        // Impact from testid
                        var impactBadge = card.querySelector('[data-testid*="ahd-ppc-row-ahr-"]');
                        if (impactBadge && impactBadge.textContent.trim()) {
                            healthImpact = impactBadge.textContent.trim();
                        } else {
                            var oldImpactBadge = card.querySelector('[data-testid*="impact"]');
                            if (oldImpactBadge && oldImpactBadge.textContent.trim()) {
                                healthImpact = oldImpactBadge.textContent.trim();
                            }
                        }
                        
                        // Clean up newlines in text
                        reason = reason.replace(/\\n+/g, ' ').replace(/\\s{2,}/g, ' ').trim();
                        actionTaken = actionTaken.replace(/\\n+/g, ' ').replace(/\\s{2,}/g, ' ').trim();
                        healthImpact = healthImpact.replace(/\\n+/g, ' ').replace(/\\s{2,}/g, ' ').trim();

                        results.push({
                            asin: asin,
                            date: dateStr,
                            action_taken: actionTaken,
                            health_impact: healthImpact,
                            reason: reason
                        });
                    } catch(e) {
                        errors++;
                    }
                }
                return {
                    cards: results,
                    total: cards.length,
                    skipped: skipped,
                    errors: errors
                };
            """)

            # ── Parse result ──────────────────────────────────────────────
            if isinstance(cards_data, dict):
                debug_info = cards_data.get('debug', '')
                total_cards = cards_data.get('total', 0)
                skipped     = cards_data.get('skipped', 0)
                js_errors   = cards_data.get('errors', 0)
                cards_data  = cards_data.get('cards', [])
                if debug_info:
                    print(f"  [DEBUG] {debug_info}")
                    snippet = cards_data if not cards_data else None
                    if not snippet:
                        # Try printing the htmlSnippet from debug
                        pass
                print(f"  Page {current_page}: {total_cards} cards on page, {len(cards_data)} extracted, {skipped} skipped, {js_errors} errors")

                # ── Fallback: parse innerHTML with regex if selectors fail ─
                if not cards_data and table_html_len > 100:
                    print("  [FALLBACK] Trying innerHTML regex extraction...")
                    cards_data = driver.execute_script("""
                        var table = document.getElementById('ahd-product-policies-table');
                        if (!table) return [];
                        var html = table.innerHTML;
                        var results = [];

                        // Extract all asin values from hidden inputs
                        var asinRegex = /name="asin"\\s+value="([A-Z0-9]{10})"/g;
                        var asins = [];
                        var m;
                        while ((m = asinRegex.exec(html)) !== null) {
                            asins.push(m[1]);
                        }

                        // For each ASIN, find surrounding violation data
                        for (var i = 0; i < asins.length; i++) {
                            results.push({
                                asin: asins[i],
                                date: '',
                                action_taken: '',
                                health_impact: '',
                                reason: ''
                            });
                        }

                        // If no hidden inputs, try the ASIN column pattern
                        if (results.length === 0) {
                            var colRegex = /kat-col-xs-7">\\s*([A-Z0-9]{10})\\s*</g;
                            while ((m = colRegex.exec(html)) !== null) {
                                results.push({
                                    asin: m[1],
                                    date: '',
                                    action_taken: '',
                                    health_impact: '',
                                    reason: ''
                                });
                            }
                        }

                        return results;
                    """) or []
                    if cards_data:
                        print(f"  [FALLBACK] Found {len(cards_data)} ASINs via innerHTML regex")
            else:
                cards_data = cards_data or []
                print(f"  Page {current_page}: found {len(cards_data)} violation card(s)")

            if not cards_data:
                if current_page == 1:
                    print(f"  No violations found for {category}")
                break

            # ── Process each extracted card ───────────────────────────────
            page_violations = []
            for card_data in cards_data:
                asin         = card_data.get('asin', '')
                date_str     = card_data.get('date', '')
                action_taken = card_data.get('action_taken', '')
                health_impact = card_data.get('health_impact', '')
                reason       = card_data.get('reason', '')

                publish_time_dt = parse_violation_date(date_str)

                if not publish_time_dt:
                    print(f"  Could not parse date: '{date_str}' for ASIN {asin}")
                print(f"  {asin} {date_str} {action_taken} {health_impact} {reason} {publish_time_dt}")
                page_violations.append({
                    "storename":    store.get('profile_name', store.get('storename', '')),
                    "asin":         asin,
                    "impact":       health_impact,
                    "action_taken": action_taken,
                    "reason":       reason,
                    "category":     category,
                    "publish_time": publish_time_dt,
                })

            # ── Save violations for this page immediately ─────────────────
            if page_violations:
                print(f"  >>> Inserting {len(page_violations)} violations from page {current_page} of {category}")
                if getattr(settings, 'LOCAL_DB', False):
                    local_db.insert_violations(page_violations)
                else:
                    insert_into_quickbase_x(data_list=[], violations_list=page_violations)
                violations.extend(page_violations)

            # ── Pagination: click next page via JavaScript ────────────────
            has_next = driver.execute_script("""
                var pagination = document.querySelector(
                    '#ahd-pp-pagination-katal-control'
                );
                if (!pagination) return false;

                var shadow = pagination.shadowRoot;
                if (!shadow) return false;

                var rightNav = shadow.querySelector(
                    'span[part="pagination-nav-right"]'
                );
                if (!rightNav) return false;
                if (rightNav.classList.contains('end')) return false;

                rightNav.click();
                return true;
            """)

            if not has_next:
                print(f"  No more pages for {category}")
                break

            current_page += 1
            print(f"  Navigating to page {current_page}...")
            time.sleep(5)

        # ── Summary for this category ─────────────────────────────────────
        if violations:
            print(f"  Total {len(violations)} violations stored for {category}")
        else:
            print(f"  No violations found for {category}")

    return driver


def main():
    # time.sleep(60)
    pass



if __name__ == '__main__':
    main()