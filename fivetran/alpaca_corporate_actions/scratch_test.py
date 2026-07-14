# scratch_test.py
import json
from alpaca.data.historical.corporate_actions import CorporateActionsClient
from alpaca.data.requests import CorporateActionsRequest
import datetime

with open("configuration.json") as f:
    config = json.load(f)

client = CorporateActionsClient(
    api_key=config["APCA-API-KEY-ID"],
    secret_key=config["APCA-API-SECRET-KEY"],
)
request = CorporateActionsRequest(
    symbols=["AAPL"],
    start=datetime.date(2023, 1, 1),
    end=datetime.date.today(),
)
response = client.get_corporate_actions(request_params=request)

print(type(response))
print(response)
