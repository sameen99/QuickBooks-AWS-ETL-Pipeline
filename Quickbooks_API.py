import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch credentials from environment variables
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
refresh_token = os.getenv("REFRESH_TOKEN")

# API URL
url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# Prepare headers
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
}

# Prepare data payload
data = {
    "grant_type": "refresh_token",
    "refresh_token": refresh_token
}

# Make the POST request
response = requests.post(url, headers=headers, data=data)

# Print the response
if response.status_code == 200:
    print("Token refreshed successfully:", response.json())
else:
    print("Failed to refresh token:", response.status_code, response.text)
