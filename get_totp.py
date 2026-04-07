# pip install opencv-python
# pip install pyotp

import cv2
import re
import pyotp, sys
from qreader import QReader
#import qrcode
#import matplotlib.pyplot as plt


def get_secret_key(uri):
    match = re.search(r'secret=([^&]+)', uri)
    if match:
        secret_key = match.group(1)
        return secret_key
    else:
        raise ValueError("Secret key not found in the OTPAuth URI.")
    

# Load the QR code image
def generate_qr_key(image, store, df, index):
    try:
        '''
        # Initialize the QR code detector
        detector = cv2.QRCodeDetector()

        # Detect and decode the QR code
        mirrored_image = cv2.flip(image, 1)
        #mirrored_image = cv2.cvtColor(mirrored_image, cv2.COLOR_BGR2RGB)
        #print(mirrored_image)
        val = detector.detectAndDecode(mirrored_image)
        #print(val)
        '''
        qreader = QReader()

        # Get the image (as RGB)
        #image = cv2.cvtColor(cv2.imread(image), cv2.COLOR_BGR2RGB)
        mirrored_image = cv2.flip(image, 1)
        # Use the detect_and_decode function to get the decoded QR data
        decoded_texts = qreader.detect_and_decode(image=mirrored_image)
        #print(decoded_texts[0])
        #sys.exit()
        key = get_secret_key(decoded_texts[0])
        
        return key
    except Exception as e:
        print(f'Error....*: {e}')
        store.update_cell(index + 2, df.columns.get_loc('remark') + 1, 'Secret key not found in the OTPAuth URI.')
        return None

def generate_2fa_code(key):
    try:
        # Initialize the TOTP object
        totp = pyotp.TOTP(key)

        # Generate the Google Authenticator code
        code_2fa = totp.now()

        return code_2fa
    except Exception as e:
        print(f'Error: {e}')
        return None

def main():
    key = 'AL4Q5U4FR4DA2TYWIWXGXT47ONIUECK5KOXOAACV7KM2HW6W6EEA'
    totp = generate_2fa_code(key)
    print(totp)

if __name__ == "__main__":
    main()
