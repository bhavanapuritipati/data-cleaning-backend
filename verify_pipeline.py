import requests
import json
import time
import os
import pandas as pd
from websockets.sync.client import connect
import threading

# Config
API_URL = "http://127.0.0.1:8000/api/v1"
WS_URL = "ws://127.0.0.1:8000/api/v1/ws"
TEST_FILE = "test_data.csv"

def create_test_data():
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", None, "David", "Eve"],
        "age": [25, 30, 35, 1000, None], # 1000 is outlier
        "city": ["New York", "London", "Paris", "Tokyo", "Berlin"]
    }
    df = pd.DataFrame(data)
    df.to_csv(TEST_FILE, index=False)
    print(f"Created {TEST_FILE}")

def listen_to_ws(job_id):
    try:
        with connect(f"{WS_URL}/{job_id}") as websocket:
            print(f"Connected to WS for {job_id}")
            while True:
                message = websocket.recv()
                print(f"WS Update: {message}")
                data = json.loads(message)
                if data.get("status") in ["completed", "failed"]:
                    break
    except Exception as e:
        print(f"WS Error: {e}")

def run_test():
    create_test_data()
    
    # 1. Upload
    print("\n1. Uploading CSV...")
    with open(TEST_FILE, "rb") as f:
        response = requests.post(f"{API_URL}/upload", files={"file": f})
    
    if response.status_code != 200:
        print(f"Upload failed: {response.text}")
        return
    
    job_id = response.json()["job_id"]
    print(f"Upload success! Job ID: {job_id}")
    
    # 2. Start WebSocket Listener
    ws_thread = threading.Thread(target=listen_to_ws, args=(job_id,))
    ws_thread.start()
    
    # 3. Start Processing
    print("\n3. Starting Processing...")
    response = requests.post(f"{API_URL}/process/{job_id}")
    if response.status_code != 200:
        print(f"Process start failed: {response.text}")
        return
    
    print("Processing started...")
    
    # 4. Poll status until done
    for _ in range(30):
        response = requests.get(f"{API_URL}/status/{job_id}")
        status = response.json()
        print(f"Status Poll: {status['status']} - Progress: {status.get('progress')}%")
        
        if status['status'] in ['completed', 'failed']:
            break
        time.sleep(1)
        
    ws_thread.join(timeout=2)
    
    # 5. Download results
    if status['status'] == 'completed':
        print("\n5. Downloading Results...")
        csv_res = requests.get(f"{API_URL}/download/{job_id}/csv")
        if csv_res.status_code == 200:
            with open("cleaned_test.csv", "wb") as f:
                f.write(csv_res.content)
            print("Downloaded cleaned_test.csv")
            print(pd.read_csv("cleaned_test.csv").head().to_string())
        else:
            print("Failed to download CSV")

if __name__ == "__main__":
    # Ensure server is running before executing this
    try:
        run_test()
    except Exception as e:
        print(f"Test failed: {e}")
