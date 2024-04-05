import requests
 
X_API_KEY = "MjY3MjE4NTIyMzU3MjM1NzEy.GjSZHJ.u_2GGDRJU-8gR_Z1IF_-foKE6buRktEa1eQ6QM" 
 
endpoint = "https://api.midjourneyapi.xyz/mj/v2/imagine"
 
headers = {
    "X-API-KEY": X_API_KEY
}
 
data = {
    "prompt": "a cute cat",
    "aspect_ratio": "4:3",
    "process_mode": "fast",
    "webhook_endpoint": endpoint,
    "webhook_secret": X_API_KEY
}
 
response = requests.post(endpoint, headers=headers, json=data)
 
print(response.status_code)
print(response.json())