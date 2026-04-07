import time
from sys import platform
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from gologin import GoLogin
from gologin import getRandomPort


gl = GoLogin({
	"token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NDAxNDE5MGI0OWU0ZTU4ZTBhMjQ0YWMiLCJ0eXBlIjoiZGV2Iiwiand0aWQiOiI2ODgxZDZiYmY3YjM4NzkxNDJjOGJlYTkifQ.0NaNXMhN2GL-E-dt2-Kof_grRlMWZBOPhd6J9da-Qtw",
	"profile_id": "644bd79f7c9bb06b32d688fd"
	})

if platform == "linux" or platform == "linux2":
	chrome_driver_path = "./chromedriver"
elif platform == "darwin":
	chrome_driver_path = "./mac/chromedriver"
elif platform == "win32":
	print("Win32")
	chrome_driver_path = "chromedriver.exe"

debugger_address = gl.start()
chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", debugger_address)
driver = webdriver.Chrome(executable_path=chrome_driver_path, options=chrome_options)
driver.get("http://www.python.org")
assert "Python" in driver.title
time.sleep(3)
driver.quit()
time.sleep(3)
gl.stop()
