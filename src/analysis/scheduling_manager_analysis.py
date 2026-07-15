import datetime as dt
import json
import itertools
import pathlib
from collections import defaultdict

import numpy as np

import pandas as pd
from matplotlib import pyplot as plt, gridspec
import matplotlib.patches as mpatches
import seaborn as sns

from src.utils import plot


def analyze_waiting_times_varying_backends(
    data_folder: pathlib.Path, plot_folder: pathlib.Path
) -> None:
    metadata_filenames = {
        4: "4_backends/metadata_2023-12-10T17:23+00:00.json",
        8: "8_backends/metadata_2023-12-10T14:54+00:00.json",
        16: "16_backends/metadata_2023-12-10T19:32+00:00.json",
    }
    queue_filenames = {
        4: "4_backends/queue_size_2023-12-10T17:23+00:00.json",
        8: "8_backends/queue_size_2023-12-10T14:54+00:00.json",
        16: "16_backends/queue_size_2023-12-10T19:32+00:00.json",
    }
    solution_means = defaultdict(list)
    times = defaultdict(list)
    for backends, filename in metadata_filenames.items():
        with open(data_folder / queue_filenames[backends]) as f:
            queue_sizes = json.load(f)
        start_time = dt.datetime.fromisoformat(queue_sizes[0]["time"])
        with open(data_folder / filename) as f:
            all_metadata = json.load(f)
        for metadata in all_metadata:
            waiting_time_mean = np.array(metadata["mean_waiting_time"])
            solution_index = metadata["solution_index"]
            solution_means[backends].append(waiting_time_mean[solution_index])
            times[backends].append(
                (
                    dt.datetime.fromisoformat(metadata["time"]) - start_time
                ).total_seconds()
            )

    fig = plt.figure(figsize=plot.FIGURE_SIZE)
    gs = gridspec.GridSpec(nrows=1, ncols=1)
    axis = fig.add_subplot(gs[0, 0])
    axis.set_ylabel("Mean Job Waiting Time (s)")
    axis.set_xlabel("Time since simulation start (s)")
    axis.grid(axis="y", linestyle="-", zorder=-1)
    color_index = 0
    for backends, waiting_times in solution_means.items():
        axis.plot(
            times[backends][:-1],
            waiting_times[:-1],
            label=f"{backends} Backends",
            color=plot.COLORS[color_index],
            linewidth=2,
            zorder=2000,
        )
        color_index += 1
    axis.legend(loc="upper left", ncol=1, framealpha=1.0)
    axis.set_ylim(bottom=0)
    axis.set_xlim(left=0)

    axis.set_title(
        "Mean Waiting Time of Scheduled Jobs",
        fontsize=12,
        fontweight="bold",
    )
    plt.savefig(
        plot_folder / f"waiting_time_vs_qpu.pdf",
        dpi=600,
        bbox_inches="tight",
    )


def analyze_waiting_times(
    experiment_timestamp: str,
    data_folder: pathlib.Path,
    plot_folder: pathlib.Path,
) -> None:
    """
    Analyze the waiting times of jobs
    :param experiment_timestamp: Timestamp of the experiment
    :param data_folder: Folder to load the data from
    :param plot_folder: Folder to save the plots to
    """
    metadata_file = f"metadata_{experiment_timestamp}.json"
    with open(data_folder / metadata_file) as f:
        all_metadata = json.load(f)
    max_95_percentile = []
    min_95_percentile = []
    max_90_percentile = []
    min_90_percentile = []
    max_mean = []
    min_mean = []
    solution_95_percentile = []
    solution_90_percentile = []
    solution_mean = []
    for metadata in all_metadata:
        waiting_time_95_percentile = np.array(
            metadata["waiting_time_95_percentile"]
        )
        waiting_time_90_percentile = np.array(
            metadata["waiting_time_90_percentile"]
        )
        waiting_time_mean = np.array(metadata["mean_waiting_time"])
        solution_index = metadata["solution_index"]
        max_95_percentile.append(np.max(waiting_time_95_percentile))
        max_90_percentile.append(np.max(waiting_time_90_percentile))
        max_mean.append(np.max(waiting_time_mean))
        min_95_percentile.append(np.min(waiting_time_95_percentile))
        min_90_percentile.append(np.min(waiting_time_90_percentile))
        min_mean.append(np.min(waiting_time_mean))
        solution_95_percentile.append(
            waiting_time_95_percentile[solution_index]
        )
        solution_90_percentile.append(
            waiting_time_90_percentile[solution_index]
        )
        solution_mean.append(waiting_time_mean[solution_index])

    data = {
        "95th Percentile": {
            "max": max_95_percentile,
            "min": min_95_percentile,
            "solution": solution_95_percentile,
        },
        "90th Percentile": {
            "max": max_90_percentile,
            "min": min_90_percentile,
            "solution": solution_90_percentile,
        },
        "Mean": {"max": max_mean, "min": min_mean, "solution": solution_mean},
    }
    for plot_name, plot_data in data.items():
        fig = plt.figure(figsize=plot.FIGURE_SIZE)
        gs = gridspec.GridSpec(nrows=1, ncols=1)
        axis = fig.add_subplot(gs[0, 0])
        axis.set_ylabel("Waiting Time (s)")
        axis.set_xlabel("Scheduling Cycle")
        axis.grid(axis="y", linestyle="-", zorder=-1)
        axis.plot(
            range(len(plot_data["max"])),
            plot_data["max"],
            label="Max Pareto Front Waiting Time",
            color=plot.COLORS[0],
            linewidth=2,
            zorder=2000,
        )
        axis.plot(
            range(len(plot_data["min"])),
            plot_data["min"],
            label="Min Pareto Front Waiting Time",
            color=plot.COLORS[1],
            linewidth=2,
            zorder=2000,
        )
        axis.plot(
            range(len(plot_data["solution"])),
            plot_data["solution"],
            label="Solution Waiting Time",
            color=plot.COLORS[2],
            linewidth=2,
            zorder=2000,
        )
        axis.legend(loc="lower center", bbox_to_anchor=(0.5, -0.35), ncol=2)
        axis.set_title(
            f"{plot_name} Waiting Time of Scheduled Jobs",
            fontsize=12,
            fontweight="bold",
        )
        plt.savefig(
            plot_folder / f"waiting_time_{plot_name.lower().replace(' ', '_')}"
            f"_{experiment_timestamp}.pdf",
            dpi=600,
            bbox_inches="tight",
        )
        plt.clf()


def analyze_fidelity(
    experiment_timestamp: str,
    data_folder: pathlib.Path,
    plot_folder: pathlib.Path,
) -> None:
    """
    Analyze the fidelity of jobs
    :param experiment_timestamp: Timestamp of the experiment
    :param data_folder: Folder to load the data from
    :param plot_folder: Folder to save the plots to
    """
    metadata_file = f"metadata_{experiment_timestamp}.json"
    with open(data_folder / metadata_file) as f:
        all_metadata = json.load(f)
    max_95_percentile = []
    min_95_percentile = []
    max_90_percentile = []
    min_90_percentile = []
    max_mean = []
    min_mean = []
    solution_95_percentile = []
    solution_90_percentile = []
    solution_mean = []
    for metadata in all_metadata:
        fidelity_95_percentile = np.array(metadata["fidelity_95_percentile"])
        fidelity_90_percentile = np.array(metadata["fidelity_90_percentile"])
        error_mean = np.array(metadata["mean_error"])
        fidelity_mean = 1 - error_mean
        solution_index = metadata["solution_index"]
        max_95_percentile.append(np.max(fidelity_95_percentile))
        max_90_percentile.append(np.max(fidelity_90_percentile))
        max_mean.append(np.max(fidelity_mean))
        min_95_percentile.append(np.min(fidelity_95_percentile))
        min_90_percentile.append(np.min(fidelity_90_percentile))
        min_mean.append(np.min(fidelity_mean))
        solution_95_percentile.append(fidelity_95_percentile[solution_index])
        solution_90_percentile.append(fidelity_90_percentile[solution_index])
        solution_mean.append(fidelity_mean[solution_index])

    data = {
        "95th Percentile": {
            "max": max_95_percentile,
            "min": min_95_percentile,
            "solution": solution_95_percentile,
        },
        "90th Percentile": {
            "max": max_90_percentile,
            "min": min_90_percentile,
            "solution": solution_90_percentile,
        },
        "Mean": {"max": max_mean, "min": min_mean, "solution": solution_mean},
    }
    for plot_name, plot_data in data.items():
        fig = plt.figure(figsize=plot.FIGURE_SIZE)
        gs = gridspec.GridSpec(nrows=1, ncols=1)
        axis = fig.add_subplot(gs[0, 0])
        axis.set_ylabel("Fidelity")
        axis.set_xlabel("Scheduling Cycle")
        axis.grid(axis="y", linestyle="-", zorder=-1)
        axis.plot(
            range(len(plot_data["max"])),
            plot_data["max"],
            label="Max Pareto Front Fidelity",
            color=plot.COLORS[0],
            linewidth=2,
            zorder=2000,
        )
        axis.plot(
            range(len(plot_data["min"])),
            plot_data["min"],
            label="Min Pareto Front Fidelity",
            color=plot.COLORS[1],
            linewidth=2,
            zorder=2000,
        )
        axis.plot(
            range(len(plot_data["solution"])),
            plot_data["solution"],
            label="Solution Fidelity",
            color=plot.COLORS[2],
            linewidth=2,
            zorder=2000,
        )
        axis.legend(loc="lower center", bbox_to_anchor=(0.5, -0.35), ncol=2)
        axis.set_title(
            f"{plot_name} Fidelity of Scheduled Jobs",
            fontsize=12,
            fontweight="bold",
        )
        plt.savefig(
            plot_folder / f"fidelity_{plot_name.lower().replace(' ', '_')}"
            f"_{experiment_timestamp}.pdf",
            dpi=600,
            bbox_inches="tight",
        )
        plt.clf()


def analyze_queue_size(
    experiment_timestamp: str,
    data_folder: pathlib.Path,
    plot_folder: pathlib.Path,
) -> None:
    """
    Analyze the queue size
    :param experiment_timestamp: Timestamp of the experiment
    :param data_folder: Folder to load the data from
    :param plot_folder: Folder to save the plots to
    """
    queue_size_file = f"queue_size_{experiment_timestamp}.json"
    with open(data_folder / queue_size_file) as f:
        queue_sizes = json.load(f)
    while queue_sizes[-2]["size"] == 0:
        queue_sizes.pop()
    sizes = [x["size"] for x in queue_sizes]
    times = [dt.datetime.fromisoformat(x["time"]) for x in queue_sizes]
    times = [(x - times[0]).total_seconds() for x in times]
    fig = plt.figure(figsize=plot.FIGURE_SIZE)
    gs = gridspec.GridSpec(nrows=1, ncols=1)
    axis = fig.add_subplot(gs[0, 0])
    axis.set_ylabel("Queue Size")
    axis.set_xlabel("Time (s)")
    axis.grid(axis="y", linestyle="-", zorder=-1)
    axis.plot(
        times,
        sizes,
        label="Queue Size",
        color=plot.COLORS[0],
        linewidth=2,
        zorder=2000,
    )
    axis.set_title(
        "Queue Size During Simulation",
        fontsize=12,
        fontweight="bold",
    )
    plt.savefig(
        plot_folder / f"queue_size_{experiment_timestamp}.pdf",
        dpi=600,
        bbox_inches="tight",
    )

def analyze_backend_distribution(
    experiment_timestamp: str,
    data_folder: pathlib.Path,
    plot_folder: pathlib.Path,
) -> None:
    """
    Analyze the distribution of jobs over backends
    :param experiment_timestamp: Timestamp of the experiment
    :param data_folder: Folder to load the data from
    :param plot_folder: Folder to save the plots to
    """
    backend_times_file = f"backend_times_{experiment_timestamp}.json"
    with open(data_folder / backend_times_file) as f:
        backend_times = json.load(f)

    fig = plt.figure(figsize=plot.FIGURE_SIZE)
    gs = gridspec.GridSpec(nrows=1, ncols=1)
    axis = fig.add_subplot(gs[0, 0])
    axis.set_ylabel("Execution Time (s)")
    axis.set_xlabel("IBM Backend")
    axis.set_xticklabels(backend_times.keys(), rotation=45, ha="right")
    axis.grid(axis="y", linestyle="-", zorder=-1)
    bar_width = 0.95 / 2
    axis.bar(
        backend_times.keys(),
        backend_times.values(),
        bar_width,
        color=plot.COLORS[0],
        edgecolor="black",
        linewidth=1.5,
        error_kw=dict(lw=2, capsize=3),
        zorder=2000,
    )
    axis.set_title(
        "Backend Workload Distribution",
        fontsize=12,
        fontweight="bold",
    )
    plt.savefig(
        plot_folder / f"backend_distribution_{experiment_timestamp}.pdf",
        dpi=600,
        bbox_inches="tight",
    )

def plot_scheduler_performance(data_folder: pathlib.Path, plot_folder: pathlib.Path):
    metadata_file = "metadata_2023-12-09T16:18+00:00.json"
    with open(data_folder / "1500_jobs" / metadata_file) as f:
        all_metadata = json.load(f)
    
    max_waiting_times = []
    min_waiting_times = []
    solution_waiting_times = []
    solution_waiting_times_95 = []

    for metadata in all_metadata:
        mean_waiting_time = np.array(metadata["mean_waiting_time"])
        solution_index = metadata["solution_index"]
        max_waiting_times.append(np.max(mean_waiting_time))
        min_waiting_times.append(np.min(mean_waiting_time))
        solution_waiting_times_95.append(metadata["waiting_time_90_percentile"][solution_index])
        #print(np.min(mean_waiting_time), mean_waiting_time[solution_index], np.max(mean_waiting_time), metadata["waiting_time_90_percentile"][solution_index])

        solution_waiting_times.append(mean_waiting_time[solution_index])
       
    max_fidelities = []
    min_fidelities = []
    solution_fidelities = []
    solution_fidelities_95 = []
    for metadata in all_metadata:
        mean_error = np.array(metadata["mean_error"])
        fidelity = 1 - mean_error
        solution_index = metadata["solution_index"]
        max_fidelities.append(np.max(fidelity))
        min_fidelities.append(np.min(fidelity))
        solution_fidelities.append(fidelity[solution_index])
        solution_fidelities_95.append(metadata["fidelity_95_percentile"][solution_index])

    print(np.mean(solution_waiting_times), np.mean(solution_waiting_times_95), np.mean(min_waiting_times), np.mean(max_waiting_times))
    print(np.mean(solution_fidelities), np.mean(solution_fidelities_95), np.mean(min_fidelities), np.mean(max_fidelities))

    df_waiting = pd.DataFrame({
        'scheduling_cycle': range(1, len(max_waiting_times) + 1), 
        'Min Pareto Front': min_waiting_times, 
        'Max Pareto Front': max_waiting_times, 
        'Solution Mean': solution_waiting_times, 
        'Solution 95th': solution_waiting_times_95
    })
    
    melted_df_waiting = pd.melt(df_waiting, id_vars='scheduling_cycle', var_name='Pareto Front', value_name='time')

    df_fidelites = pd.DataFrame({
        'scheduling_cycle': range(1, len(max_waiting_times) + 1), 
        'Min Pareto Front': min_fidelities, 
        'Max Pareto Front': max_fidelities, 
        'Solution Mean': solution_fidelities,
        'Solution 95th': solution_fidelities_95
    })
    melted_df_fidelities = pd.melt(df_fidelites, id_vars='scheduling_cycle', var_name='Pareto Front', value_name='Fidelity')

    backend_queues_files = ["backend_times_2023-12-09T16:18+00:00.json", "backend_times_2023-12-09T19:21+00:00.json", "backend_times_2023-12-09T21:27+00:00.json"]
    backend_queues = {}

    with open(data_folder / pathlib.Path("1500_jobs") / backend_queues_files[0]) as f:    
        backend_queues['1500 j/h'] = json.load(f)

    with open(data_folder / pathlib.Path("3000_jobs") / backend_queues_files[1]) as f:    
        backend_queues['3000 j/h'] = json.load(f)

    with open(data_folder / pathlib.Path("4500_jobs") / backend_queues_files[2]) as f:    
        backend_queues['4500 j/h'] = json.load(f)


    rows = []
    for workload, values in backend_queues.items():
        for k,v in values.items():
            rows.append({'Workload': workload, 'backend': k.split("_")[1], 'queue_size': v})                
    
    backend_queue_df = pd.DataFrame(rows)
    filtered_df = backend_queue_df[backend_queue_df['Workload'] == '3000 j/h']

    largest_difference = (filtered_df['queue_size'].max() - filtered_df['queue_size'].min()) / filtered_df['queue_size'].min() * 100
    #print(largest_difference)

    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows, ncols)
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    sns.lineplot(
        melted_df_waiting, 
        x=melted_df_waiting['scheduling_cycle'], 
        y=melted_df_waiting['time'], 
        ax=axis[0], 
        hue=melted_df_waiting["Pareto Front"], 
        palette=sns.color_palette("deep"), 
        style=melted_df_waiting['Pareto Front'],
        markers=True
    )
    axis[0].text(0.5, 1.18, plot.LOWERISBETTER, ha="center", va="center", transform=axis[0].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    
    axis[0].set_title(
        "(a) JCT of Scheduled Jobs",
        fontsize=12,
        fontweight="bold",
    )
    axis[0].set_ylabel("JCT [s]")
    axis[0].set_xlabel("Scheduling Cycle")
    axis[0].set_ylim(0, 15000)
    axis[0].legend(loc="upper center", bbox_to_anchor=(1.05, -0.23), ncol=4)


    sns.lineplot(
        melted_df_fidelities, 
        x=melted_df_fidelities['scheduling_cycle'], 
        y=melted_df_fidelities['Fidelity'], 
        legend=False, 
        ax=axis[1], 
        hue=melted_df_fidelities["Pareto Front"], 
        palette=sns.color_palette("deep"), 
        style=melted_df_fidelities['Pareto Front'],
        markers=True,
    )
    axis[1].text(0.5, 1.18, plot.HIGHERISBETTER, ha="center", va="center", transform=axis[1].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    
    axis[1].set_title(
        "(b) Fidelity of Scheduled Jobs",
        fontsize=12,
        fontweight="bold",
    )
    axis[1].set_ylabel("Fidelity")
    axis[1].set_xlabel("Scheduling Cycle")
    axis[1].set_ylim(0.65, 0.8)

    bar_plot = sns.barplot(
        backend_queue_df, 
        x=backend_queue_df['backend'], 
        y=backend_queue_df['queue_size'], 
        hue=backend_queue_df['Workload'], 
        palette=sns.color_palette("pastel"), 
        edgecolor="black",
        linewidth=1.5,
        ax=axis[2],
    )

    num_machines = len(backend_queue_df['backend'].unique())
    hatches = ['xx', '..', '--']
    ihatches = itertools.cycle(hatches)
    for i, bar in enumerate(axis[2].patches):
        if i % num_machines == 0:
            hatch = next(ihatches)
        bar.set_hatch(hatch)


    axis[2].text(0.5, 1.18, "Equal is better", ha="center", va="center", transform=axis[2].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    axis[2].set_ylabel("Total Runtime [s]")
    axis[2].set_xlabel("IBM QPU")
    axis[2].set_xticklabels(backend_queue_df['backend'].unique(), rotation=30)
    axis[2].set_ylim(0, 50000)
    #axis[2].set_yscale("log")
    legend_handles = [
        mpatches.Patch(facecolor=plot.COLORS[0], edgecolor='black', hatch=hatches[0]),
        mpatches.Patch(facecolor=plot.COLORS[1], edgecolor='black', hatch=hatches[1]),
        mpatches.Patch(facecolor=plot.COLORS[2], edgecolor='black', hatch=hatches[2]),
    ]
    legend_labels = backend_queue_df['Workload'].unique()
    axis[2].legend(legend_handles, legend_labels)
    axis[2].set_title(
        "(c) QPU Load as Total Runtime",
        fontsize=12,
        fontweight="bold",
    )

    plt.subplots_adjust(wspace=0.3)
    plt.savefig(
        plot_folder / "scheduler_performance_analysis.pdf",
        dpi=600,
        bbox_inches="tight"
    )

def plot_scheduler_scalability(data_folder: pathlib.Path, plot_folder: pathlib.Path):
    queue_size_files = ["queue_size_2023-12-09T16:18+00:00.json", "queue_size_2023-12-09T19:21+00:00.json", "queue_size_2023-12-09T21:27+00:00.json"]
    job_waiting_times = ["metadata_2023-12-10T17:23+00:00.json", "metadata_2023-12-10T14:54+00:00.json", "metadata_2023-12-10T19:32+00:00.json"]
    scheduler_queue_size = {}
    scheduler_breakdown = {}
    waiting_times = {}


    with open(data_folder / pathlib.Path("1500_jobs") / queue_size_files[0]) as f:    
        scheduler_queue_size['1500 j/h'] = json.load(f)
    while scheduler_queue_size['1500 j/h'][-2]["size"] == 0:
        scheduler_queue_size.pop()

    with open(data_folder / pathlib.Path("3000_jobs") / queue_size_files[1]) as f:    
        scheduler_queue_size['3000 j/h'] = json.load(f)
    while scheduler_queue_size['3000 j/h'][-2]["size"] == 0:
        scheduler_queue_size.pop()

    with open(data_folder / pathlib.Path("4500_jobs") / queue_size_files[2]) as f:    
        scheduler_queue_size['4500 j/h'] = json.load(f)
    while scheduler_queue_size['4500 j/h'][-2]["size"] == 0:
        scheduler_queue_size.pop()    

    with open(data_folder / pathlib.Path("4_backends") / job_waiting_times[0]) as f:
        tmp = json.load(f)
        to_save = {}
        for elem in tmp:
            to_save[elem["time"]] = {"mean_waiting_time" : elem["mean_waiting_time"], "estimation_time": elem["estimation_time"], "optimization_time": elem["optimization_time"], "mcdm_time": elem["mcdm_time"]}
        waiting_times['4 QPUs'] = to_save

    with open(data_folder / pathlib.Path("8_backends") / job_waiting_times[1]) as f:
        tmp = json.load(f)
        to_save = {}
        for elem in tmp:
            to_save[elem["time"]] = {"mean_waiting_time" : elem["mean_waiting_time"], "estimation_time": elem["estimation_time"], "optimization_time": elem["optimization_time"], "mcdm_time": elem["mcdm_time"]}    
        waiting_times['8 QPUs'] = to_save

    with open(data_folder / pathlib.Path("16_backends") / job_waiting_times[2]) as f:
        tmp = json.load(f)
        to_save = {}
        for elem in tmp:
            to_save[elem["time"]] = {"mean_waiting_time" : elem["mean_waiting_time"], "estimation_time": elem["estimation_time"], "optimization_time": elem["optimization_time"], "mcdm_time": elem["mcdm_time"]}  
        waiting_times['16 QPUs'] = to_save 

    
    rows = []
    for backends, values in waiting_times.items():
        time_zero = dt.datetime.fromisoformat(list(values.keys())[0])

        for k,v in values.items():
            time = dt.datetime.fromisoformat(k)
            time = (time - time_zero).total_seconds()
            if time < 3610:
                rows.append({'System Size': backends, 'mean_waiting_time': np.mean(v["mean_waiting_time"]), 'time': time})
    
    waiting_times_df = pd.DataFrame(rows)
   
    rows = []
    for backends, values in waiting_times.items():
        estimation_times = [time["estimation_time"] for time in list(values.values())]
        optimization_times = [time["optimization_time"] for time in list(values.values())]
        mcdm_times = [time["mcdm_time"] for time in list(values.values())]
            
        rows.append({'System Size': backends, 'Job Pre-processing': np.mean(estimation_times), 'Optimization': np.mean(optimization_times), 'Selection': np.mean(mcdm_times)})
    
    scheduler_breakdown_df = pd.DataFrame(rows)
    melted_breakdown = pd.melt(scheduler_breakdown_df, id_vars='System Size')

    rows = []
    counter = int(len(list(scheduler_queue_size.values())[0]) / 50)
    for workload, values in scheduler_queue_size.items():
            time_zero = dt.datetime.fromisoformat(values[0]['time'])
            for entry in values:
                if counter == int(len(values) / 50):
                    time = dt.datetime.fromisoformat(entry['time'])
                    time = (time - time_zero).total_seconds()
                    if time < 3600:
                        rows.append({'Workload': workload, 'queue_size': entry['size'], 'time': time})
                    counter = 0
                else:
                    counter = counter + 1
    
    queue_df = pd.DataFrame(rows)


    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows, ncols, width_ratios=[1, 1, 1])
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    sns.lineplot(
        waiting_times_df, 
        x=waiting_times_df['time'], 
        y=waiting_times_df['mean_waiting_time'], 
        ax=axis[0], 
        hue=waiting_times_df['System Size'], 
        palette=sns.color_palette("deep"), 
        style=waiting_times_df['System Size'],
        markers=True
    )
    axis[0].text(0.5, 1.3, plot.LOWERISBETTER, ha="center", va="center", transform=axis[0].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    axis[0].legend(ncols=1)
    axis[0].set_ylabel("Mean JCT [s]")
    axis[0].set_xlabel("Time [s]")
    axis[0].set_xlim(0, 3600)
    axis[0].set_ylim(0, 30000)
    
    axis[0].set_title(
        "(a) Mean JCT vs. Quantum Cluster Size",
        fontsize=12,
        fontweight="bold",
    )

    sns.lineplot(
        queue_df, 
        x=queue_df['time'], 
        y=queue_df['queue_size'], 
        ax=axis[1], 
        hue=queue_df['Workload'], 
        palette=sns.color_palette("deep"), 
        style=queue_df['Workload'],
        markers=True,
        markevery=2
    )
    axis[1].text(0.5, 1.3, plot.LOWERISBETTER, ha="center", va="center", transform=axis[1].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    axis[1].set_ylabel("Scheduler's Queue Size")
    axis[1].set_xlabel("Time [s]")
    axis[1].set_xlim(0, 3600)
    axis[1].set_ylim(0, 150)
    axis[1].legend(ncols=3, prop={'size': 10})
    axis[1].set_title(
        "(b) Scheduler's Queue Size vs. Workload",
        fontsize=12,
        fontweight="bold",
    )

    sns.barplot(
        melted_breakdown, 
        x=melted_breakdown['variable'], 
        y=melted_breakdown['value'], 
        hue=melted_breakdown['System Size'], 
        palette=sns.color_palette("pastel"), 
        edgecolor="black",
        linewidth=1.5,
        ax=axis[2]
    )

    num_machines = len(melted_breakdown['variable'].unique())
    hatches = ['/', 'oo', '--']
    ihatches = itertools.cycle(hatches)
    for i, bar in enumerate(axis[2].patches):
        if i % num_machines == 0:
            hatch = next(ihatches)
        bar.set_hatch(hatch)

    legend_handles = [
        mpatches.Patch(facecolor=plot.COLORS[0], edgecolor='black', hatch=hatches[0]),
        mpatches.Patch(facecolor=plot.COLORS[1], edgecolor='black', hatch=hatches[1]),
        mpatches.Patch(facecolor=plot.COLORS[2], edgecolor='black', hatch=hatches[2]),
    ]
    legend_labels = melted_breakdown['System Size'].unique()
    axis[2].legend(legend_handles, legend_labels)

    axis[2].set_ylabel("Time [s]")
    axis[2].set_xlabel("Scheduling Stage")
    axis[2].set_yscale("log")
    axis[2].set_title(
        "(c) Stages Runtime vs. Quantum Cluster Size",
        fontsize=12,
        fontweight="bold",
    )
    axis[2].text(0.5, 1.3, plot.LOWERISBETTER, ha="center", va="center", transform=axis[2].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    plt.tight_layout(w_pad=0.2)
    plt.savefig(
        plot_folder / "scheduler_scalability_analysis.pdf",
        dpi=600,
        bbox_inches="tight",
    )

plot_scheduler_scalability(pathlib.Path("data/scheduling_manager"), pathlib.Path("plots/scheduling_manager"))
plot_scheduler_performance(pathlib.Path("data/scheduling_manager"), pathlib.Path("plots/scheduling_manager"))