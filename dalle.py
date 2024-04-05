from openai import OpenAI
import os
import requests
from PIL import Image

# setup credentials
client = OpenAI(
  api_key=os.environ.get("MY_OPENAI_APIKEY"),
)
# request
response = client.images.generate(
  model="dall-e-2",
  prompt="Pacman chasing Superman",
  size="1024x1024",
  quality="standard",
  n=5,
)
# output
for i in range(0, 5):
  img_url = response.data[i].url
  img = requests.get(img_url).content
  with open("image_" + str(i) + ".png", "wb") as image_file:
      image_file.write(img)