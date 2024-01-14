import requests
from bs4 import BeautifulSoup


def get_session_id():
    response = requests.get("http://www.data.jma.go.jp/risk/obsdl/index.php")
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    session_id_input = soup.find("input", {"id": "sid"})
    return session_id_input["value"]


# Go to this address
# https://www.data.jma.go.jp/risk/obsdl/index.php

# Open devtools
# Go to network tab
# Select Fetch/XHR and reload the page
# You should see requests for station, element, period
# These contain the codes you need to create the download URL
# When you click a location, you should see another station request containing the station codes
