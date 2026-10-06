# Split Boundary Strategy Experiment — 2026-10-07

## Goal

Test whether FeatherDB should replace hash-midpoint tablet splitting with a data-aware boundary.

## Strategies

The research module compares:

- HashMidpoint
- ByteMedian
- HeatMedian

The input is an ordered set of token samples carrying:

    bytes
    heat

The chosen boundary must remain strictly inside the parent range.

## Skewed workload

The synthetic workload intentionally separates storage skew from access heat.

Observed:

| Strategy | Byte imbalance (ppm) | Heat imbalance (ppm) |
|---|---:|---:|
| HashMidpoint | 800,000 | 923,076 |
| ByteMedian | **500,000** | 948,717 |
| HeatMedian | 934,000 | **358,974** |

Interpretation:

- ByteMedian materially improves byte balance but slightly worsens heat balance.
- HeatMedian materially improves heat balance but produces very poor byte balance.
- HashMidpoint is poor on both signals here, but requires no telemetry and is deterministic from metadata alone.

## Same-token hotspot

Two equally hot samples at exactly the same token cannot be separated by any token boundary.

Observed:

    HeatMedian boundary = token + 1
    heat imbalance = 1,000,000 ppm

This is an important negative result.

A single-key/same-token hotspot requires another mechanism such as:

- request-level hotspot handling;
- key-aware sharding above the hash tablet layer;
- caching/admission controls;
- application-level decomposition.

Tablet range split alone cannot solve it.

## Quality gate

- cargo test: **68 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Decision

Do not replace midpoint split with one universal data-aware strategy.

Keep the architecture:

    split trigger
      -> boundary objective
      -> boundary strategy
      -> range transform

Current candidate policy:

1. HashMidpoint remains the deterministic fallback.
2. ByteMedian may be used when the objective is storage-size equalization and telemetry confidence is sufficient.
3. HeatMedian may be used for a hot-range isolation experiment, not as a general storage-balancing boundary.
4. A future multi-objective/Pareto policy should be tested before any production default changes.

The split boundary is therefore a policy input, not a hidden implementation detail.


## Multi-objective policy layer

A policy layer now decides whether to remain on HashMidpoint or adopt a data-aware boundary.

Policy inputs:

    byte_weight_ppm
    heat_weight_ppm
    max_byte_imbalance_ppm
    max_heat_imbalance_ppm
    min_telemetry_confidence_ppm
    min_score_improvement_ppm

The weighted score is computed from byte and heat imbalance.

A data-aware candidate is eligible only when:

1. telemetry confidence meets the minimum;
2. byte imbalance is within the configured hard limit;
3. heat imbalance is within the configured hard limit;
4. weighted score improves on midpoint;
5. improvement exceeds the minimum materiality threshold.

Otherwise the decision falls back to HashMidpoint.

### Storage-focused policy

Configuration emphasizes bytes:

    byte weight = 900,000 ppm
    heat weight = 100,000 ppm
    max byte imbalance = 600,000 ppm
    telemetry confidence = 950,000 ppm

Observed:

    chosen = ByteMedian
    reason = DataAwareImprovement
    midpoint score = 812,307
    chosen score = 544,871

### Heat-focused policy

Configuration emphasizes request heat:

    byte weight = 100,000 ppm
    heat weight = 900,000 ppm
    max heat imbalance = 500,000 ppm
    telemetry confidence = 950,000 ppm

Observed:

    chosen = HeatMedian
    reason = DataAwareImprovement
    midpoint score = 910,768
    chosen score = 416,476

### Low-confidence telemetry

With telemetry confidence below the configured threshold:

    chosen = HashMidpoint
    reason = LowTelemetryConfidence

No data-aware boundary is allowed to override the deterministic fallback.

### Updated quality gate

- cargo test: **73 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Updated decision

Boundary strategy must remain an explicit policy choice.

HashMidpoint is the safety/default fallback.

Data-aware boundaries require:

    confident telemetry
      + objective-specific weighting
      + hard safety limits
      + material improvement over midpoint

This prevents noisy telemetry from causing unnecessary tablet-map churn.
