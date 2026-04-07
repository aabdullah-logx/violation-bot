import  os
from dotenv import load_dotenv
load_dotenv()


policy_violation_url_list = load_dotenv('policy_violation_url_list')

token = load_dotenv('token')

a_z_claims = load_dotenv('a_z_claims')

base_url = load_dotenv('base_url')

AMAZON_HOME = os.getenv('AMAZON_HOME')

LOCAL = load_dotenv('LOCAL')
LOCAL_DB = load_dotenv('LOCAL_DB')