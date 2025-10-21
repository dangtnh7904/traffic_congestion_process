import requests

api_key = "TYOkXocsoD_sOF9X41vpIzUARGmIU01AkV4z-l7XGRY"

# Bounding box for central Hanoi
# Format: west,south,east,north (longitude, latitude)
bbox = "105.759888,20.951340,105.898247,21.090747"  # Adjust for specific area

url = f"https://data.traffic.hereapi.com/v7/flow?locationReferencing=shape&in=bbox:{bbox}&apiKey={api_key}"

response = requests.get(url)
traffic_data = response.json()
with open("hanoi_traffic_data.json", "w") as f:
    f.write(response.text)