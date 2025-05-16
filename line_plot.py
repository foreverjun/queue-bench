import glob
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

INPUT_DIR = './target/bench_res'
OUTPUT_DIR = './plots'
PARAMETER_GROUPING = ['additional_work', 'batch_size', 'is_balanced']
PRODUCER_CONSUMER_RATIOS = {"1:1": (1, 1), "2:1": (2, 1), "1:2": (1, 2)}


def create_plots_for_file(csv_file_path, base_plot_dir):
    try:
        df = pd.read_csv(csv_file_path)
    except pd.errors.EmptyDataError:
        print(f"File {csv_file_path} empty.")
        return
    except Exception as e:
        print(f"Read error {csv_file_path}: {e}.")
        return

    if df.empty:
        return

    df['total_threads'] = df['prod'] + df['cons']
    grouped = df.groupby(
        ['queue_type', 'prod', 'cons', 'total_threads', 'batch_size', 'additional_work', 'is_balanced'], as_index=False)

    sum_df = grouped.agg(throughput_mean=('throughput', 'mean'), throughput_std=('throughput', 'std'),
        throughput_median=('throughput', 'median'), transf_mean=('transf', 'mean'), transf_std=('transf', 'std'),
        transf_median=('transf', 'median'))

    for params, group_df in sum_df.groupby(PARAMETER_GROUPING):
        param_values_str = "_".join(map(str, params))
        plot_subdir = os.path.join(base_plot_dir, param_values_str)
        os.makedirs(plot_subdir, exist_ok=True)

        for ratio_name, (p_ratio, c_ratio) in PRODUCER_CONSUMER_RATIOS.items():

            if p_ratio > c_ratio:  # 2:1
                current_ratio_df = group_df[group_df['prod'] == p_ratio * group_df['cons'] / c_ratio]
            elif c_ratio > p_ratio:  # 1:2
                current_ratio_df = group_df[group_df['cons'] == c_ratio * group_df['prod'] / p_ratio]
            else:  # 1:1
                current_ratio_df = group_df[group_df['prod'] == group_df['cons']]

            if current_ratio_df.empty:
                continue

            plt.figure(figsize=(10, 6))
            sns.set_style("whitegrid")

            plot = sns.lineplot(x='total_threads', y='throughput_mean', hue='queue_type', style='queue_type',
                data=current_ratio_df, markers=True, dashes=True, )

            plot.set_xticks(sorted(current_ratio_df['total_threads'].unique()))

            plt.title(
                f"{ratio_name} producer-consumer benchmark\n({', '.join(f'{k}={v}' for k, v in zip(PARAMETER_GROUPING, params))})")
            plt.xlabel("Threads")
            plt.ylabel("Throughput, transfers / s")
            plt.legend(title="Queue Type")
            plt.tight_layout()

            plot_filename = f"{ratio_name.replace(':', '_')}_benchmark.png"
            plt.savefig(os.path.join(plot_subdir, plot_filename))
            plt.close()


if __name__ == "__main__":
    if not os.path.exists(INPUT_DIR):
        print(f"Dir not found: {INPUT_DIR}")
        exit()

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))

    if not csv_files:
        print(f"No CSV in {INPUT_DIR}")
        exit()

    for csv_file in csv_files:
        file_basename = os.path.splitext(os.path.basename(csv_file))[0]
        specific_plot_dir = os.path.join(OUTPUT_DIR, file_basename)
        os.makedirs(specific_plot_dir, exist_ok=True)
        create_plots_for_file(csv_file, specific_plot_dir)
