# queue-bench

**queue-bench** is a Rust benchmarking suite designed to evaluate the performance of various lock-free queues, particularly in producer-consumer scenarios. This project was developed as part of a thesis project.

## Getting Started

Clone the repository:

```bash
git clone https://github.com/yourusername/queue-bench.git
cd queue-bench
```

## Running Benchmarks

To execute the benchmarks:

```bash
cargo bench
```

Benchmark results will be saved in the `target/bench_res` directory.

## Generating Plots

To visualize the benchmark results:

1. Install the required Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Run the plotting script:

   ```bash
   python line_plot.py
   ```

Generated plots will be saved in the `plots` directory.

## License

This project is licensed under the MIT License.

