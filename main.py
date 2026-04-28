import requests
import os
from google.cloud import bigquery
from datetime import datetime, timezone

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
    current_time = datetime.now(timezone.utc).isoformat()

    for game in games:

        appid = int(game["appid"])
        player_count = int(game.get("peak_in_game", 0))

        game_name = "Unknown"
        genre = "Unknown"
        price = 0.0
        positive = 0
        total = 0

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
                    data = store_data[str(appid)]["data"]

                    game_name = data.get("name", "Unknown")

                    # Genres
                    genres = data.get("genres", [])
                    if genres:
                        genre = ", ".join([g.get("description", "") for g in genres])

                    # Price
                    if data.get("price_overview"):
                        price = data["price_overview"].get("final", 0) / 100

        except Exception as e:
            print("Store API error:", e)

        try:
            review_url = f"https://store.steampowered.com/appreviews/{appid}?json=1"
            review_response = requests.get(review_url, timeout=5)

            if review_response.status_code == 200:
                review_data = review_response.json()
                summary = review_data.get("query_summary", {})

                positive = summary.get("total_positive", 0)
                total = summary.get("total_reviews", 0)

        except Exception as e:
            print("Review API error:", e)

        rows.append({
            "appid": appid,
            "game_name": game_name,
            "player_count": player_count,
            "price": price,
            "genre": genre,
            "positive_reviews": positive,
            "total_reviews": total,
            "timestamp": current_time
        })

    print("TABLE_ID:", TABLE_ID)
    print("Rows:", rows)

    errors = bq_client.insert_rows_json(TABLE_ID, rows)

    if errors:
        print("Insert errors:", errors)
        return str(errors)

    return f"Inserted {len(rows)} rows"
