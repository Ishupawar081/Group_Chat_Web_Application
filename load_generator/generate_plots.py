import pandas as pd
import matplotlib.pyplot as plt

load = pd.read_csv("load_results.csv")
util = pd.read_csv("utilization_results.csv")

# 1. Response time
plt.figure()
plt.plot(load["response_time_ms"])
plt.xlabel("Request")
plt.ylabel("Response Time (ms)")
plt.title("Request Response Time")
plt.grid(True)
plt.tight_layout()
plt.savefig("response_time.png", dpi=200)
plt.close()

# 2. CPU utilization
plt.figure()
plt.plot(util["sys1_cpu"], label="Sys1")
plt.plot(util["sys2_cpu"], label="Sys2")
plt.plot(util["sys3_cpu"], label="Sys3")
plt.plot(util["sys4_cpu"], label="Sys4")
plt.xlabel("Time Sample")
plt.ylabel("CPU Utilization (%)")
plt.title("CPU Utilization of All Four Systems")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("cpu_utilization.png", dpi=200)
plt.close()

# 3. Memory utilization
plt.figure()
plt.plot(util["sys1_memory"], label="Sys1")
plt.plot(util["sys2_memory"], label="Sys2")
plt.plot(util["sys3_memory"], label="Sys3")
plt.plot(util["sys4_memory"], label="Sys4")
plt.xlabel("Time Sample")
plt.ylabel("Memory Utilization (%)")
plt.title("Memory Utilization of All Four Systems")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("memory_utilization.png", dpi=200)
plt.close()

print("Plots generated:")
print("response_time.png")
print("cpu_utilization.png")
print("memory_utilization.png")
