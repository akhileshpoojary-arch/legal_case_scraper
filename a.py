import requests
import time

DOWNLOAD_URL = "https://proxy.webshare.io/api/v2/proxy/list/download/cljfobotqdqmxmdnvelsntcqbyfuibeqxsvtamot/-/any/username/backbone"

TARGET_COUNT = 50000

proxies = set()  # use set to avoid duplicates

while len(proxies) < TARGET_COUNT:
    response = requests.get(DOWNLOAD_URL)

    if response.status_code != 200:
        print("Error:", response.status_code)
        break

    lines = response.text.strip().split("\n")

    for line in lines:
        if line:
            proxies.add(line)

    print(f"Collected unique proxies: {len(proxies)}")

    # stop if no new proxies are coming
    if len(lines) == 0:
        break

    time.sleep(3)  # avoid rate limit

# Save to file
with open("proxies.txt", "w") as f:
    f.write("\n".join(proxies))

print(f"\nSaved {len(proxies)} unique proxies to proxies.txt")
