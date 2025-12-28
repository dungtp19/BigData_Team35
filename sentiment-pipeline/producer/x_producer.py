import time
import sys
import requests
from kafka import KafkaProducer

# =========================
# CONFIG
# =========================

# X API
BEARER_TOKEN = "YOUR TOKEN IS HERE"
QUERY = "crypto OR bitcoin OR ethereum lang:en -is:retweet" # thay đổi keyword ở đây
MAX_RESULTS = 10   # acc free

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "social_raw"

# Run mode
AUTO_MODE = True      # đổi thành False nếu muốn chạy manual mỗi lần
INTERVAL_SECONDS = 20 * 60 # call API mỗi lần cách nhau 20 phút (do acc free giới hạn 15 phút 1 lần)
MAX_RUNS = 2          # tránh lỡ xài hết hạn mức acc free (2 × 10 = 20 posts)

# =========================
# KAFKA PRODUCER
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode("utf-8")
)

print("Kafka producer connected")

# =========================
# X API CALL
# =========================

url = "https://api.x.com/2/tweets/search/recent"
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}
params = {
    "query": QUERY,
    "max_results": MAX_RESULTS
}

print("==> Calling X API...")

# =========================
# MAIN LOOP
# =========================

run_count = 0

try:
    while True:
        run_count += 1
        print(f"\n==> Run #{run_count}: Calling X API")

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print("X API ERROR")
            print("Status:", response.status_code)
            print("Response:", response.text)
            break

        payload = response.json()
        tweets = payload.get("data", [])

        print(f"Retrieved {len(tweets)} posts")

        if not tweets:
            print("No data returned – exiting to protect quota")
            break

        for t in tweets:
            text = t.get("text", "").strip()
            if not text:
                continue

            producer.send(KAFKA_TOPIC, text)
            print("Sent:", text[:80], "...")

        producer.flush()
        print("Kafka flush complete")

        # ===== MANUAL MODE =====
        if not AUTO_MODE:
            print("Manual mode → exit after 1 run")
            break

        # ===== AUTO MODE SAFETY =====
        if run_count >= MAX_RUNS:
            print(f"Max runs ({MAX_RUNS}) reached → exit to protect quota")
            break

        print(f"Sleeping {INTERVAL_SECONDS // 60} minutes...")
        time.sleep(INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\n Interrupted by user (Ctrl+C)")

finally:
    producer.close()
    print("Kafka producer closed cleanly")
    print("x_producer stopped")