use feather_sim::{RangeLoadSample, SplitBoundaryStrategy, choose_split_boundary};

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
