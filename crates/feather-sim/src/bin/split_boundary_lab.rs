use feather_sim::{
    RangeLoadSample, SplitBoundaryPolicy, SplitBoundaryStrategy, choose_split_boundary,
    choose_split_boundary_with_policy,
};

fn main() {
    let samples = vec![
        RangeLoadSample {
            token: 10,
            bytes: 400,
            heat: 10,
        },
        RangeLoadSample {
            token: 20,
            bytes: 350,
            heat: 10,
        },
        RangeLoadSample {
            token: 30,
            bytes: 150,
            heat: 10,
        },
        RangeLoadSample {
            token: (u64::MAX / 5) * 3,
            bytes: 34,
            heat: 240,
        },
        RangeLoadSample {
            token: (u64::MAX / 10) * 7,
            bytes: 33,
            heat: 260,
        },
        RangeLoadSample {
            token: (u64::MAX / 10) * 9,
            bytes: 33,
            heat: 250,
        },
    ];

    println!("split-boundary-lab samples={}", samples.len());
    println!(
        "strategy,boundary,left_bytes,right_bytes,byte_imbalance_ppm,left_heat,right_heat,heat_imbalance_ppm"
    );

    for strategy in [
        SplitBoundaryStrategy::HashMidpoint,
        SplitBoundaryStrategy::ByteMedian,
        SplitBoundaryStrategy::HeatMedian,
    ] {
        let d = choose_split_boundary(0, 1_u128 << 64, &samples, strategy)
            .expect("valid split decision");
        println!(
            "{:?},{},{},{},{},{},{},{}",
            d.strategy,
            d.boundary,
            d.left_bytes,
            d.right_bytes,
            d.byte_imbalance_ppm(),
            d.left_heat,
            d.right_heat,
            d.heat_imbalance_ppm()
        );
    }

    for (name, policy, confidence) in [
        (
            "storage-focused",
            SplitBoundaryPolicy {
                byte_weight_ppm: 900_000,
                heat_weight_ppm: 100_000,
                max_byte_imbalance_ppm: 600_000,
                max_heat_imbalance_ppm: 1_000_000,
                min_telemetry_confidence_ppm: 700_000,
                min_score_improvement_ppm: 10_000,
            },
            950_000,
        ),
        (
            "heat-focused",
            SplitBoundaryPolicy {
                byte_weight_ppm: 100_000,
                heat_weight_ppm: 900_000,
                max_byte_imbalance_ppm: 950_000,
                max_heat_imbalance_ppm: 500_000,
                min_telemetry_confidence_ppm: 700_000,
                min_score_improvement_ppm: 10_000,
            },
            950_000,
        ),
        (
            "low-confidence",
            SplitBoundaryPolicy {
                byte_weight_ppm: 500_000,
                heat_weight_ppm: 500_000,
                max_byte_imbalance_ppm: 1_000_000,
                max_heat_imbalance_ppm: 1_000_000,
                min_telemetry_confidence_ppm: 800_000,
                min_score_improvement_ppm: 10_000,
            },
            500_000,
        ),
    ] {
        let d = choose_split_boundary_with_policy(0, 1_u128 << 64, &samples, confidence, &policy)
            .expect("policy decision");
        println!(
            "policy={name},confidence_ppm={confidence},chosen={:?},reason={:?},midpoint_score_ppm={},chosen_score_ppm={}",
            d.chosen.strategy, d.reason, d.midpoint_score_ppm, d.chosen_score_ppm
        );
    }

    let hotspot = vec![
        RangeLoadSample {
            token: 42,
            bytes: 1,
            heat: 500,
        },
        RangeLoadSample {
            token: 42,
            bytes: 1,
            heat: 500,
        },
    ];
    let hot = choose_split_boundary(0, 1_u128 << 64, &hotspot, SplitBoundaryStrategy::HeatMedian)
        .expect("hotspot decision");

    println!(
        "same-token-hotspot,boundary={},heat_imbalance_ppm={}",
        hot.boundary,
        hot.heat_imbalance_ppm()
    );
}
