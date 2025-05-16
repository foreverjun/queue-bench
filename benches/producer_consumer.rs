use itertools::iproduct;
use queue_bench::bq_impl::BQ;
use queue_bench::faa_ebr_impl::FAAebrBQ;
use queue_bench::faa_hazard_impl::FAAhazardBQ;
use queue_bench::BatchQueue;
use queue_bench::SendPtr;
use rand::rng;
use rand_distr::{Distribution, Geometric};
use std::fs;
use std::iter;
use std::path::PathBuf;
use std::{
    fs::OpenOptions,
    hint::black_box,
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

fn random_additional_work(mean: f64) {
    if mean <= 0.0 {
        return;
    }
    let p_success = (1.0 / mean).min(1.0).max(f64::EPSILON);
    let mut rng = rng();
    let geom = Geometric::new(p_success).expect("Invalid probability for Geometric distribution");
    let num_ops = geom.sample(&mut rng) + 1;

    for _ in 0..num_ops {
        black_box(42u64);
    }
}

const WARMUP_OPERATIONS_PER_THREAD: usize = 100_000;

#[allow(clippy::too_many_arguments)]
fn run_once(
    queue: Arc<dyn BatchQueue<Item = u8> + Send + Sync>,
    batch_size: usize,
    measurement_duration: Duration,
    num_producers: usize,
    num_consumers: usize,
    additional_work: f64,
    balanced_load: bool,
) -> (u64, u64) {
    let barrier = Arc::new(Barrier::new(num_producers + num_consumers + 1));
    let stop_signal = Arc::new(AtomicBool::new(false));

    let (producer_additional_work, consumer_additional_work) = if balanced_load {
        let total = (num_producers + num_consumers) as f64;
        let reference = additional_work * 2.0 / total;
        (
            num_producers as f64 * reference,
            num_consumers as f64 * reference,
        )
    } else {
        (additional_work, additional_work)
    };

    let mut consumer_handles = Vec::with_capacity(num_consumers);
    for _ in 0..num_consumers {
        let q_clone = queue.clone();
        let barrier_clone = barrier.clone();
        let stop_clone = stop_signal.clone();

        consumer_handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for _ in 0..(WARMUP_OPERATIONS_PER_THREAD / num_consumers + 1) {
                black_box(q_clone.dequeue_batch(batch_size));
            }
            barrier_clone.wait();

            while q_clone.dequeue_batch(batch_size) != 0 {}
            let mut success_deq = 0u64;

            barrier_clone.wait();
            while !stop_clone.load(Ordering::Acquire) {
                let deq_num = q_clone.dequeue_batch(batch_size);
                success_deq += deq_num as u64;
                random_additional_work(consumer_additional_work);
            }
            success_deq
        }));
    }

    let mut producer_handles = Vec::with_capacity(num_producers);
    for _ in 0..num_producers {
        let q_clone = Arc::clone(&queue);
        let barrier_clone = Arc::clone(&barrier);
        let stop_clone = Arc::clone(&stop_signal);

        producer_handles.push(thread::spawn(move || {
            let val = 42u8;
            let ptr = SendPtr::new(&val as *const u8 as *mut u8);
            let items_to_enqueue: Vec<SendPtr<u8>> = vec![ptr; batch_size];

            barrier_clone.wait();
            for _ in 0..(WARMUP_OPERATIONS_PER_THREAD / num_producers + 1) {
                q_clone.enqueue_batch(&items_to_enqueue);
            }
            barrier_clone.wait();
            barrier_clone.wait();
            while !stop_clone.load(Ordering::Acquire) {
                q_clone.enqueue_batch(&items_to_enqueue);
                random_additional_work(producer_additional_work);
            }
        }));
    }

    barrier.wait(); // start warmup
    barrier.wait(); // finish warmup
    barrier.wait(); // start measurements
    let start_time = Instant::now();
    thread::sleep(measurement_duration);
    stop_signal.store(true, Ordering::Release);

    let duration_ns = start_time.elapsed().as_nanos() as u64;

    for handle in producer_handles {
        handle.join().expect("Producer thread panicked");
    }

    let mut total_deq = 0;
    for handle in consumer_handles {
        match handle.join() {
            Ok(count) => total_deq += count,
            Err(e) => eprintln!("Consumer thread panicked: {:?}", e),
        }
    }
    (duration_ns, total_deq)
}

struct SegBQ<T> {
    items: Arc<crossbeam_queue::SegQueue<*mut T>>,
}
unsafe impl<T> Send for SegBQ<T> {}
unsafe impl<T> Sync for SegBQ<T> {}

impl SegBQ<u8> {
    fn new() -> Self {
        SegBQ {
            items: Arc::new(crossbeam_queue::SegQueue::new()),
        }
    }
}

impl<T: Send + Sync> BatchQueue for SegBQ<T> {
    type Item = T;

    fn enqueue_batch(&self, items_to_enqueue: &[SendPtr<Self::Item>]) {
        for item_ptr in items_to_enqueue {
            self.items.push(item_ptr.as_ptr());
        }
    }

    fn dequeue_batch(&self, max_to_dequeue: usize) -> usize {
        let mut deq_count = 0;
        for _ in 0..max_to_dequeue {
            if self.items.pop().is_some() {
                deq_count += 1;
            } else {
                break;
            }
        }
        deq_count
    }
}

fn main() {
    let measurement_duration = Duration::from_secs(3);
    let additional_works = [512.0];
    let balanced_load_options = [true];
    let num_runs = 20;
    let batch_sizes_to_test = [64];

    let one_to_one = [1, 2, 4, 6, 8, 12, 16, 24].iter().map(|&p| (p, p));

    let one_to_two = [1, 2, 4, 6, 8, 12, 16]
        .iter()
        .flat_map(|&p| iter::once((p, 2 * p)).chain(iter::once((2 * p, p))));

    let num_prod_cons = one_to_one.chain(one_to_two);

    // Write to csv
    let mut path = PathBuf::from("target");
    path.push("bench_res");
    let _ = fs::create_dir_all(&path);
    let filename = format!(
        "prod_cons_{}.csv",
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );
    path.push(filename);
    let mut csv_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("Failed to open CSV file");

    writeln!(csv_file, "queue_type,prod,cons,batch_size,additional_work,is_balanced,run_iter,dur_ns,transf,throughput").unwrap();
    println!("Starting benchmarks...");
    println!("Measurement duration per run: {:?}", measurement_duration);
    let queue_factories: Vec<(
        &str,
        Box<dyn Fn() -> Arc<dyn BatchQueue<Item = u8> + Send + Sync>>,
    )> = vec![
        ("seg_queue", Box::new(|| Arc::new(SegBQ::<u8>::new()))),
        ("faa_ebr", Box::new(|| Arc::new(FAAebrBQ::<u8>::new()))),
        ("faa_hazard", Box::new(|| Arc::new(FAAhazardBQ::<u8>::new()))),
        ("BQ", Box::new(|| Arc::new(BQ::<u8>::new()))),
    ];

    for (batch_size_val, factory_tuple, prod_cons, additional_work, balanced) in iproduct!(
        batch_sizes_to_test.iter().cloned(),
        queue_factories.iter(),
        num_prod_cons,
        additional_works.iter().cloned(),
        balanced_load_options.iter().cloned()
    ) {
        let (queue_name, make_queue_fn) = factory_tuple;
        let (num_producers, num_consumers) = prod_cons;

        println!(
            "\nTesting: Queue={}, P={}, C={}, Batch={}, Work={}, Balanced={}",
            queue_name, num_producers, num_consumers, batch_size_val, additional_work, balanced
        );

        let mut throughputs = Vec::new();

        for run_num in 1..=num_runs {
            let queue_instance = make_queue_fn();
            let (duration_ns, transfers) = run_once(
                queue_instance,
                batch_size_val,
                measurement_duration,
                num_producers,
                num_consumers,
                additional_work,
                balanced,
            );
            let throughput = if duration_ns > 0 {
                transfers as f64 / duration_ns as f64 * 1_000_000_000.0
            } else {
                0.0
            };
            throughputs.push(throughput);

            writeln!(
                csv_file,
                "{},{},{},{},{},{},{},{},{},{:.2}",
                queue_name,
                num_producers,
                num_consumers,
                batch_size_val,
                additional_work,
                balanced,
                run_num,
                duration_ns,
                transfers,
                throughput
            )
            .unwrap();
            println!(
                "  Run {}: {} items in {} ns => {:.2} transfers/sec",
                run_num, transfers, duration_ns, throughput
            );
        }
    }
    println!(
        "\nBenchmarking complete. Results saved to {}",
        path.to_str().unwrap()
    );
}
