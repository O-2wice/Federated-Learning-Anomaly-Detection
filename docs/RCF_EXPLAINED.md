# Robust Random Cut Forest (RRCF) Anomaly Detection

The Flink job scores every reading of the IoT stream with a **Robust Random Cut
Forest** (Guha et al., 2016) before it is used for federated training. RRCF is
unsupervised and built for streams: the model updates one point at a time, keeps
bounded memory and follows drift without a retraining phase.

Code: `RandomCutForest` and `AdaptiveThresholdManager` in
[`scripts/flink_models.py`](../scripts/flink_models.py), wired into the job in
[`scripts/03_flink_local_training.py`](../scripts/03_flink_local_training.py).
The trees come from [`rrcf`](https://github.com/kLabUM/rrcf), the reference
open-source implementation.

## How it works

A random cut tree splits its points with random cuts. The cut dimension is
drawn in proportion to each dimension's range, so points that sit far from the
rest are isolated close to the root. When a new point arrives, its
**collusive displacement (CoDisp)** measures how many points would move if it
were removed. Normal points displace little; outliers displace a lot.

```text
reading (46 standardized features)
        │
        ▼
 insert into each of 4 trees ── evict oldest point once a tree holds 256
        │
        ▼
 mean CoDisp over trees  ──►  percentile among the last 500 raw scores
        │
        ▼
 score = 0 below the 90th percentile, linear to 1 at the top
        │
        ▼
 score > device threshold (adaptive, starts at 0.4) ──► `anomalies` topic
```

### Design choices

| Choice | Setting | Why |
| --- | --- | --- |
| Point | All 46 standardized features of one reading | Readings of a device have no meaningful order (rows are shuffled into devices), so a shingle of one value carries no temporal pattern. A one-feature shingle scored at chance (AUC 0.51). |
| Forest scope | One shared forest per Flink worker | The stream is keyed by `device_id`, so each worker serves a stable part of the fleet. A shared forest compares every reading with current fleet traffic. 2,400 per-device forests would each see only a few hundred readings and cost 2,400 times the memory. |
| Size | 4 trees × 256 points (`FLEAD_RCF_TREES`, `FLEAD_RCF_TREE_SIZE`) | Best quality of the sizes tested, and fast enough for the 150 readings/s producer rate (see below). |
| Score scale | Percentile of the raw CoDisp among the last 500 scores | Raw CoDisp has no fixed scale and shifts as the forest's contents change. A rank is stable: a 0.4 threshold means "above the 94th percentile". |
| Threshold | Per device, starts at 0.4, bounded to [0.2, 0.8] | Every 50 readings, the threshold rises by 0.02 if more than 7.5% of the device's last 100 scores exceed it, and falls by 0.02 if fewer than 2.5% do (target rate 5%). |

Each anomaly message carries the 0–1 score, the raw CoDisp, the threshold used,
a severity and the reading's ground-truth label. Severity is `critical` when the
score is above 0.8 or beats the threshold by more than 0.3, and `warning` when
the score is above 0.6 or beats the threshold by more than 0.15. The label lets
the dashboards show what share of flagged readings are real attacks.

## Measured results

The experiments streamed real device files through `RandomCutForest`,
interleaving devices the way the producer does. ROC AUC is computed on the raw
CoDisp; the first `tree_size` readings of each run were skipped as warm-up.

**Tree count.** 60 devices × 150 readings, two device samples (28% attacks),
adaptive thresholds, nothing else running. Values are sample 1 / sample 2.

| Forest | ROC AUC | Readings flagged | Attacks among flagged | Time per reading |
| --- | --- | --- | --- | --- |
| 1 × 256 | 0.538 / 0.551 | 6.0% / 5.9% | 62% / 60% | 1.7 / 1.3 ms |
| 2 × 256 | 0.554 / 0.595 | 6.3% / 6.0% | 44% / 54% | 3.0 / 1.5 ms |
| **4 × 256** | **0.650 / 0.663** | **6.1% / 6.0%** | **86% / 81%** | **5.7 / 4.2 ms** |

**More trees and smaller trees.** 30 devices × 150 readings (27% attacks),
fixed 0.4 threshold. Another experiment was running, so these timings are
pessimistic.

| Forest | ROC AUC | Readings flagged | Attacks among flagged | Time per reading |
| --- | --- | --- | --- | --- |
| 8 × 256 | 0.647 | 6.2% | 73% | 37.8 ms |
| 4 × 256 | 0.658 | 5.9% | 72% | 11.0 ms |
| 8 × 128 | 0.638 | 6.2% | 67% | 15.9 ms |
| 4 × 128 | 0.646 | 5.9% | 65% | 8.3 ms |

**Inside the job.** The whole per-reading path ran offline on 60 devices ×
660 readings: producer message → `AnomalyDetectionFunction` (4 × 256,
adaptive thresholds, no warm-up skipped) → local training → FedAvg. It flagged
5.8% of readings, and 64.5% of them were attacks (27.6% base rate).

Across these runs, **4 × 256 flags about 6% of readings, and 65–86% of the
flagged readings are attacks against a 27–28% base rate: 2.3 to 3.1 times
better than flagging at random.** Fewer trees lose most of that precision;
more trees or smaller trees add nothing.

Scoring takes 4–6 ms per reading. The rest of the per-reading path, including
local training, adds about 3 ms (4.8 ms in total with a 1-tree forest). One
Flink slot therefore handles roughly 110–140 readings/s, and the two slots
cover the producer's 150 readings/s. Flink's Python bridge adds overhead that
these offline numbers do not include.

### What RRCF does and does not do here

- An AUC of 0.66 means RRCF ranks readings usefully but does not separate
  attacks from benign traffic on its own: many attack readings are not
  outliers in feature space.
- RRCF uses no labels, so it can flag new behaviour that no labelled model has
  seen. The supervised federated model (logistic regression, FedAvg) is the
  classifier; RRCF is the label-free early-warning signal next to it.

## References

- S. Guha, N. Mishra, G. Roy, O. Schrijvers. *Robust Random Cut Forest Based
  Anomaly Detection on Streams.* ICML 2016.
  <https://proceedings.mlr.press/v48/guha16.html>
- M. Bartos et al. *rrcf: Implementation of the Robust Random Cut Forest
  algorithm for anomaly detection on streams.* JOSS 2019.
  <https://github.com/kLabUM/rrcf>
