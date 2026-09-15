import argparse
import csv
import random
import string
import threading
import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser()
parser.add_argument("--users", type=int, default=10)
parser.add_argument("--duration", type=int, default=60)
parser.add_argument("--url", default="https://127.0.0.1:3257")
parser.add_argument("--min-interval", type=float, default=0.2)
parser.add_argument("--max-interval", type=float, default=1.0)
args = parser.parse_args()

results = []
utilization = []
lock = threading.Lock()
end_time = time.time() + args.duration


def random_message():
    length = random.randint(20, 500)
    return "".join(
        random.choices(
            string.ascii_letters + string.digits + " ",
            k=length
        )
    )


def get_utilization():
    try:
        response = requests.get(
            "http://127.0.0.1:3000/lb/metrics",
            verify=False,
            timeout=5
        )

        data = response.json()

        sample = {
            "timestamp": time.time(),
            "sys1_cpu": 0,
            "sys1_memory": 0
        }

        for backend in data["backends"]:
            instance = backend["instance"]

            if instance == "Sys2":
                sample["sys2_cpu"] = backend["cpu"]
                sample["sys2_memory"] = backend["memory"]

            elif instance == "Sys3":
                sample["sys3_cpu"] = backend["cpu"]
                sample["sys3_memory"] = backend["memory"]

            elif instance == "Sys4":
                sample["sys4_cpu"] = backend["cpu"]
                sample["sys4_memory"] = backend["memory"]

        # Sys1 utilization
        try:
            with open("/proc/stat") as f:
                line = f.readline().split()

            idle = float(line[4])
            total = sum(float(x) for x in line[1:])

            if not hasattr(get_utilization, "previous"):
                get_utilization.previous = (idle, total)
                cpu = 0
            else:
                prev_idle, prev_total = get_utilization.previous
                idle_delta = idle - prev_idle
                total_delta = total - prev_total
                cpu = (
                    100 * (1 - idle_delta / total_delta)
                    if total_delta > 0 else 0
                )
                get_utilization.previous = (idle, total)

            sample["sys1_cpu"] = round(cpu, 2)

        except Exception:
            pass

        try:
            with open("/proc/meminfo") as f:
                meminfo = f.read()

            values = {}

            for line in meminfo.splitlines():
                key, value = line.split(":", 1)
                values[key] = float(value.strip().split()[0])

            total_mem = values["MemTotal"]
            available_mem = values["MemAvailable"]

            sample["sys1_memory"] = round(
                100 * (1 - available_mem / total_mem), 2
            )

        except Exception:
            pass

        with lock:
            utilization.append(sample)

    except Exception:
        pass


def utilization_monitor():
    while time.time() < end_time:
        get_utilization()
        time.sleep(1)


def worker(user_id):
    session = requests.Session()
    session.verify = False

    while time.time() < end_time:

        payload = {
            "client-name": f"load-user-{user_id}",
            "msg": random_message()
        }

        start = time.perf_counter()

        try:
            response = session.post(
                f"{args.url}/message",
                json=payload,
                timeout=10
            )

            elapsed = (time.perf_counter() - start) * 1000

            with lock:
                results.append({
                    "user": user_id,
                    "status": response.status_code,
                    "response_time_ms": round(elapsed, 2),
                    "message_length": len(payload["msg"]),
                    "timestamp": time.time()
                })

        except Exception:
            elapsed = (time.perf_counter() - start) * 1000

            with lock:
                results.append({
                    "user": user_id,
                    "status": "ERROR",
                    "response_time_ms": round(elapsed, 2),
                    "message_length": len(payload["msg"]),
                    "timestamp": time.time()
                })

        time.sleep(
            random.uniform(
                args.min_interval,
                args.max_interval
            )
        )


# Start utilization monitoring
monitor = threading.Thread(target=utilization_monitor)
monitor.start()

# Start users
threads = []

for user_id in range(1, args.users + 1):
    thread = threading.Thread(
        target=worker,
        args=(user_id,)
    )
    thread.start()
    threads.append(thread)

# Wait for users
for thread in threads:
    thread.join()

monitor.join()


# Save request results
with open("load_results.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "user",
            "status",
            "response_time_ms",
            "message_length",
            "timestamp"
        ]
    )

    writer.writeheader()
    writer.writerows(results)


# Save utilization results
with open("utilization_results.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "timestamp",
            "sys1_cpu",
            "sys1_memory",
            "sys2_cpu",
            "sys2_memory",
            "sys3_cpu",
            "sys3_memory",
            "sys4_cpu",
            "sys4_memory"
        ]
    )

    writer.writeheader()
    writer.writerows(utilization)


successful = [
    r for r in results
    if isinstance(r["status"], int)
    and 200 <= r["status"] < 300
]

errors = len(results) - len(successful)

print(f"Users: {args.users}")
print(f"Duration: {args.duration}s")
print(f"Total requests: {len(results)}")
print(f"Successful requests: {len(successful)}")
print(f"Errors: {errors}")

if successful:
    avg = sum(
        r["response_time_ms"] for r in successful
    ) / len(successful)

    print(f"Average response time: {avg:.2f} ms")

print(f"Utilization samples: {len(utilization)}")
print("Results saved to load_results.csv")
print("Utilization saved to utilization_results.csv")
