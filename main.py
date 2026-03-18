import requests
import os
from google.cloud import bigquery

bq_client = bigquery.Client()

PROJECT = os.environ.get("PROJECT")
DATASET = os.environ.get("DATASET")
TABLE = os.environ.get("TABLE")

TABLE_ID = f"{PROJECT}.{DATASET}.{TABLE}"


def load_steam_games(request):

    charts_url = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
    charts_response = requests.get(charts_url, timeout=5)

    if charts_response.status_code != 200:
        return f"Charts API failed: {charts_response.text}"

    charts_data = charts_response.json()

    games = charts_data.get("response", {}).get("ranks", [])[:10]

    rows = []

    for game in games:

        appid = int(game["appid"])
        player_count = int(game["peak_in_game"])
        game_name = "Unknown"

        try:
            store_url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
            store_response = requests.get(store_url, timeout=5)

            if store_response.status_code == 200:
                store_data = store_response.json()

                if (
                    str(appid) in store_data and
                    store_data[str(appid)].get("success") and
                    "data" in store_data[str(appid)]
                ):
                    game_name = store_data[str(appid)]["data"]["name"]

        except Exception as e:
            print("Store API error:", e)

        rows.append({
            "appid": appid,
            "game_name": game_name,
            "player_count": player_count
        })

    print("TABLE_ID:", TABLE_ID)
    print("Rows:", rows)

    errors = bq_client.insert_rows_json(TABLE_ID, rows)

    if errors:
        print("Insert errors:", errors)
        return str(errors)

    return f"Inserted {len(rows)} rows"