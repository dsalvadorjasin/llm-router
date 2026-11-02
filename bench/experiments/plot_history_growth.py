"""Plot per-turn /v1/chat latency from history_growth.py outputs.

    uv run --with matplotlib python bench/experiments/plot_history_growth.py OUT.png fixed.json [stock.json]
"""
import json
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

out, inputs = sys.argv[1], sys.argv[2:]
fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
for path in inputs:
    label = "fixed 100ms backend" if "fixed" in path else "stock fleet"
    data = json.load(open(path))["turns"]
    longest = data[max(data, key=int)]
    turns = [t["turn"] for t in longest]
    axes[0].plot(turns, [t["total_ms"] for t in longest], ".-", label=f"{label}: total turn latency", alpha=.8)
    axes[1].plot(turns, [t["router_ms"] for t in longest], ".-", label=f"{label}: router-side (total - upstream)", alpha=.8)
ax2 = axes[1].twinx()
ax2.plot(turns, [t["prompt_chars"] / 1000 for t in longest], "k--", label="prompt size (k chars)")
ax2.set_ylabel("prompt size sent upstream (k chars)")
axes[0].set(title="/v1/chat latency per turn (100-turn conversation)", xlabel="turn", ylabel="ms")
axes[1].set(title="router-side time vs prompt size", xlabel="turn", ylabel="ms", ylim=(0, 20))
axes[0].legend(fontsize=8)
h1, l1 = axes[1].get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
axes[1].legend(h1 + h2, l1 + l2, fontsize=8, loc="upper left")
fig.tight_layout()
fig.savefig(out, dpi=110)
print("wrote", out)
