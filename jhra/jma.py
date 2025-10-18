import requests
from bs4 import BeautifulSoup


def get_session_id():
    response = requests.get("http://www.data.jma.go.jp/risk/obsdl/index.php")
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    session_id_input = soup.find("input", {"id": "sid"})
    return session_id_input["value"]
