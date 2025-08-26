import time
import threading
import httpx
import json


BASE_URL = "http://localhost:8000"

def get_metadata_count():
    """Fetch number of metadata rows."""
    r = httpx.get(f"{BASE_URL}/get-metadata")
    r.raise_for_status()
    return r.json()["count"]

def post_payload(payload, payload2, post_finish_time_holder):
    """Run the POST request and capture completion time."""
    r = httpx.post(f"{BASE_URL}/process", 
    json={
        "payload": payload,
        "payload2": payload2   
    },
    timeout=60.0
    )
    post_finish_time_holder["time"] = time.perf_counter()
    print(f"POST finished with status {r.status_code}")


def lag_test(payload, payload2):
    """
    Run lag test: measure delay between POST completion and GET visibility.
    """
    initial_count = get_metadata_count()
    print(f"Initial row count = {initial_count}")

    post_finish_time = {}
    post_thread = threading.Thread(
        target=post_payload, args=(payload, payload2, post_finish_time)
    )
    post_thread.start()

    lag = None
    while True:
        current_count = get_metadata_count()
        if current_count > initial_count:
            read_time = time.perf_counter()
            lag = (read_time - post_finish_time["time"]) * 1000
            break

    post_thread.join()
    print(f"Row appeared in {lag:.2f} ms after POST finished")
    return lag

if __name__ == "__main__":
    with open("api_body.json", "r") as f:
        data = json.load(f)
    payload = data["payload"]
    payload2 = data["payload2"]

    lag_test(payload, payload2)
