import gc
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
import jsonpickle
import pandas as pd
import seaborn as sns

import numpy as np
import multiprocessing
from matplotlib import pyplot as plt, gridspec
from src.utils import plot
from matplotlib.ticker import FormatStrFormatter

from src.scheduler.multi_objective_scheduler import (
    MultiObjectiveScheduler,
    TranspilationLevel,
    ProblemType,
)
from src.utils.benchmark import (
    get_benchmark_names,
    generate_random_job,
    get_fake_backends,
)

logger = logging.getLogger(__name__)


def compare_scheduler_steps(plot_folder: str, data_folder: str) -> None:
    """
    Compare time taken for each step of the scheduler
    :param plot_folder: Folder to save the plots
    :param data_folder: Folder to save the data
    """
    backends = get_fake_backends(remove_retired=True)
    benchmark_names = get_benchmark_names()
    job_counts = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    backend_counts = range(1, 10)

    scheduler = MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PROCESSOR_TYPE,
        problem_type=ProblemType.DISCRETE,
    )

    seed = int(os.environ.get("SEED", time.time()))
    random_generator = np.random.default_rng(seed)
    logger.info("Random seed: %d", seed)

    repeat_count = 5
    benchmark_size = 5

    for backend_count in backend_counts:
        optimization_times = defaultdict(list)
        mcdm_times = defaultdict(list)
        schedule_generation_times = defaultdict(list)
        estimation_times = defaultdict(list)
        execution_times = defaultdict(list)
        for _ in range(repeat_count):
            random_backends = random_generator.choice(
                backends, size=backend_count, replace=False
            )
            for job_count in job_counts:
                logger.info(
                    "Backend Count: %d, Job Count: %d",
                    backend_count,
                    job_count,
                )
                circuit_count = int(random_generator.normal(50, 20))
                circuit_count = max(1, min(circuit_count, 100))
                jobs = [
                    generate_random_job(
                        benchmark_size,
                        benchmark_names,
                        random_generator,
                        circuit_count=circuit_count,
                        shots=4000,
                    )
                    for _ in range(job_count)
                ]
                _, _, metadata = scheduler.schedule(jobs, random_backends)
                optimization_times[job_count].append(
                    metadata["optimization_time"]
                )
                mcdm_times[job_count].append(metadata["mcdm_time"])
                schedule_generation_times[job_count].append(
                    metadata["schedule_generation_time"]
                )
                estimation_times[job_count].append(metadata["estimation_time"])
                execution_times[job_count].append(
                    np.mean(metadata["solution_execution_times"])
                )

                collected = gc.collect()
                logger.info(
                    "Garbage collector: collected %d objects.", collected
                )

        average_optimization_times = [
            np.mean(times) for times in optimization_times.values()
        ]
        average_mcdm_times = [np.mean(times) for times in mcdm_times.values()]
        average_schedule_generation_times = [
            np.mean(times) for times in schedule_generation_times.values()
        ]
        average_estimation_times = [
            np.mean(times) for times in estimation_times.values()
        ]
        average_execution_times = [
            np.mean(times) for times in execution_times.values()
        ]
        plt.plot(
            job_counts,
            average_optimization_times,
            label="Optimization Time",
        )
        plt.plot(
            job_counts,
            average_schedule_generation_times,
            label="Schedule Generation Time",
        )
        plt.plot(
            job_counts,
            average_estimation_times,
            label="Estimation Time",
        )
        plt.plot(
            job_counts,
            average_mcdm_times,
            label="MCDM Time",
        )
        plt.plot(
            job_counts,
            average_execution_times,
            label="Execution Time",
        )
        plt.xlabel("Number of Jobs")
        plt.ylabel("Time (s)")
        plt.title(f"Backend Count: {backend_count}")
        plt.legend()
        plot_path = (
            Path(__file__).absolute().parents[2]
            / plot_folder
            / f"{backend_count}_backends.png"
        )
        plt.savefig(plot_path)
        plt.clf()
        logger.info("Saved plot to %s", plot_path)

        json_path = (
            Path(__file__).absolute().parents[2]
            / data_folder
            / f"{backend_count}_backends.json"
        )
        with open(json_path, "w") as f:
            json.dump(
                {
                    "execution_times": execution_times,
                    "optimization_times": optimization_times,
                    "mcdm_times": mcdm_times,
                    "schedule_generation_times": schedule_generation_times,
                    "estimation_times": estimation_times,
                },
                f,
                indent=4,
            )
        logger.info("Saved data to %s", json_path)

def compare_binary_and_discrete_problems(
    plot_folder: str, data_folder: str
) -> None:
    """
    Compare binary and discrete problems
    :param plot_folder: Folder to save the plots
    :param data_folder: Folder to save the data
    """
    backends = get_fake_backends(remove_retired=True)
    benchmark_names = get_benchmark_names()

    binary_problem_scheduler = MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PRE_TRANSPILED,
        problem_type=ProblemType.BINARY,
    )

    discrete_problem_scheduler = MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PRE_TRANSPILED,
        problem_type=ProblemType.DISCRETE,
    )

    seed = int(os.environ.get("SEED", time.time()))
    random_generator = np.random.default_rng(seed)
    logger.info("Random seed: %d", seed)

    repeat_count = 5

    binary_done = False
    discrete_done = False

    job_count = 1
    job_increment = 10

    binary_optimization_times = defaultdict(list)
    discrete_optimization_times = defaultdict(list)

    while not binary_done or not discrete_done:
        logger.info(
            "Job Count: %d",
            job_count,
        )

        for _ in range(repeat_count):
            min_backend_size = min(backend.num_qubits for backend in backends)
            circuit_count = int(random_generator.normal(50, 20))
            circuit_count = max(1, min(circuit_count, 100))

            jobs = [
                generate_random_job(
                    min_backend_size,
                    benchmark_names,
                    random_generator,
                    circuit_count=circuit_count,
                    shots=4000,
                )
                for _ in range(job_count)
            ]
            if not binary_done:
                logger.info("Binary Problem")
                assignments, _, metadata = binary_problem_scheduler.schedule(
                    jobs, backends
                )
                if assignments:
                    binary_optimization_times[job_count].append(
                        metadata["optimization_time"]
                    )

            if not discrete_done:
                logger.info("Discrete Problem")
                assignments, _, metadata = discrete_problem_scheduler.schedule(
                    jobs, backends
                )
                if assignments:
                    discrete_optimization_times[job_count].append(
                        metadata["optimization_time"]
                    )

            collected = gc.collect()
            logger.info("Garbage collector: collected %d objects.", collected)

        binary_done = binary_optimization_times[job_count] == []
        discrete_done = discrete_optimization_times[job_count] == []

        if binary_done:
            logger.info(
                "Binary Problem Done at %d jobs",
                job_count,
            )
        if discrete_done:
            logger.info(
                "Discrete Problem Done at %d jobs",
                job_count,
            )

        job_count += job_increment

    average_binary_optimization_times = [
        np.mean(times) for times in binary_optimization_times.values()
    ]

    average_discrete_optimization_times = [
        np.mean(times) for times in discrete_optimization_times.values()
    ]

    binary_job_counts = list(binary_optimization_times.keys())
    discrete_job_counts = list(discrete_optimization_times.keys())

    plt.plot(
        binary_job_counts,
        average_binary_optimization_times,
        label="Binary Problem",
    )

    plt.plot(
        discrete_job_counts,
        average_discrete_optimization_times,
        label="Discrete Problem",
    )
    plt.xlabel("Number of Jobs")
    plt.ylabel("Optimization time (s)")
    plt.title(f"Optimization Time Comparison")
    plt.legend()
    plot_path = (
        Path(__file__).absolute().parents[2]
        / plot_folder
        / f"binary_vs_discrete.png"
    )
    plt.savefig(plot_path)
    plt.clf()
    logger.info("Saved plot to %s", plot_path)

    json_path = (
        Path(__file__).absolute().parents[2]
        / data_folder
        / f"binary_vs_discrete.json"
    )
    with open(json_path, "w") as f:
        json.dump(
            {
                "binary_optimization_times": binary_optimization_times,
                "discrete_optimization_times": discrete_optimization_times,
            },
            f,
            indent=4,
        )
    logger.info("Saved data to %s", json_path)

def generate_job(benchmark_names, random_generator, ncircuits=50, nshots=4000):
    circuit_count = int(random_generator.normal(ncircuits, int(ncircuits/2)))
    circuit_count = max(1, min(circuit_count, 100))

    return generate_random_job(
        5,
        benchmark_names,
        random_generator,
        circuit_count,
        nshots,
    )

def generate_jobs(njobs=100, ncircuits=50, nshots=4000):
    backends = get_fake_backends()[:16]

    benchmark_names = get_benchmark_names()

    scheduler = MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PRE_TRANSPILED,
        problem_type=ProblemType.DISCRETE,
    )
    print("instatiated scheduler")
    scheduler.load_pre_transpiled_circuits(
        backends, benchmark_names, [3, 4, 5]
    )
    print("circuits loaded")
    seed = 1702322517
    random_generator = np.random.default_rng(seed)
    logger.info("Random seed: %d", seed)

    jobs = []
    
    with multiprocessing.Pool(processes=int(multiprocessing.cpu_count() / 2)) as pool:
        jobs = pool.starmap(generate_job, [(benchmark_names, random_generator, ncircuits, nshots) for i in range(njobs)])
   

    with open('data/scheduling_manager/jobs.json', 'w') as json_file:
        for j in jobs:
            json_file.write(jsonpickle.encode(j))
            json_file.write('\n')

def generate_schedules(weights=[0.5,0.5]):
    backends = get_fake_backends()[:16]

    benchmark_names = get_benchmark_names()

    scheduler = MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PRE_TRANSPILED,
        problem_type=ProblemType.DISCRETE,
    )
    print("instatiated scheduler")
    scheduler.load_pre_transpiled_circuits(
        backends, benchmark_names, [3, 4, 5]
    )
    print("circuits loaded")

    jobs = []      
    
    with open('data/scheduling_manager/jobs.json', 'r') as json_file:
        for line in json_file:
            jobs.append(jsonpickle.decode(line))

    start = time.perf_counter()
    _, _, metadataFid = scheduler.schedule(jobs, backends, weights=[0,1])
    total = time.perf_counter() - start
    print(total)
    print("fid schedule")
    _, _, metadata = scheduler.schedule(jobs, backends, weights=[0.5,0.5])
    print("balanced schedule")
    _, _, metadataWait = scheduler.schedule(jobs, backends, weights=[1,0])
    print("wait schedule")

    with open('data/scheduling_manager/metadata_balanced.json', 'w') as json_file:
        json.dump(metadata, json_file)

    with open('data/scheduling_manager/metadata_fid.json', 'w') as json_file:
        json.dump(metadataFid, json_file)

    with open('data/scheduling_manager/metadata_wait.json', 'w') as json_file:
        json.dump(metadataWait, json_file)

def plot_fidelity_and_waiting_time():
    metadata_file = "metadata_2023-12-09T16:18+00:00.json"
    with open("data/scheduling_manager/1500_jobs/" + metadata_file) as f:
        all_metadata = json.load(f)
    
    min_execution_times = []
    max_execution_times = []
    solution_execution_times = []

    for metadata in all_metadata:
        mean_execution_time = np.array(metadata["solution_execution_times"])
        solution_index = metadata["solution_index"]
        max_execution_times.append(np.max(mean_execution_time))
        min_execution_times.append(np.min(mean_execution_time))
        solution_execution_times.append(mean_execution_time[solution_index])

    print(np.mean(min_execution_times), np.mean(max_execution_times), np.mean(solution_execution_times))

    df_execution = pd.DataFrame({
        'scheduling_cycle': range(1, len(max_execution_times) + 1), 
        'Min Pareto Front': min_execution_times, 
        'Max Pareto Front': max_execution_times, 
        'Solution Mean': solution_execution_times, 
    })
    
    melted_df_waiting = pd.melt(df_execution, id_vars='scheduling_cycle', var_name='Pareto Front', value_name='time')

    with open("data/scheduling_manager/metadata_balanced.json") as f:
        metadata = json.load(f)

    with open("data/scheduling_manager/metadata_fid.json") as f:
        metadataFid = json.load(f)

    with open("data/scheduling_manager/metadata_wait.json") as f:
        metadataWait = json.load(f)

    fidelity = 1 - np.array(metadata["mean_error"])
    waiting_time = metadata["mean_waiting_time"]
    solution_index = metadata["solution_index"]
    solution_index_fid = metadataFid["solution_index"]
    solution_index_wait = metadataWait["solution_index"]
    
    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 2
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
        "(a) Mean Execution Time of Scheduled Jobs",
        fontsize=12,
        fontweight="bold",
    )
    axis[0].set_ylabel("Execution Time [s]")
    axis[0].set_xlabel("Scheduling Cycle")
    axis[0].set_ylim(0, 200)
    axis[0].legend(loc="upper center", bbox_to_anchor=(0.8, 0.8))

    axis[1].yaxis.set_major_formatter(FormatStrFormatter("%.2f"))
    axis[1].xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
    axis[1].set_xlabel("Fidelity")
    axis[1].set_ylabel("JCT [s]")
    axis[1].grid(axis="y", linestyle="-", zorder=-1)
    axis[1].set_ylim(400, 1800)
    axis[1].set_xlim(0.68, 0.8)

    fidelity = sorted(fidelity)
    waiting_time = sorted(waiting_time)

    print(waiting_time[0], waiting_time[len(waiting_time) - 1], waiting_time[solution_index])
    print(fidelity[0], fidelity[len(fidelity) - 1], fidelity[solution_index])

    axis[1].scatter(
        fidelity,
        waiting_time,
        s=50,
        color=plot.COLORS[0],
        linewidth=1,
        edgecolor="black",
        alpha=0.8,
        zorder=2000,
    )

    # add solution as a star
    axis[1].scatter(
        fidelity[solution_index],
        waiting_time[solution_index],
        marker="X",
        s=100,
        color=plot.COLORS[3],
        edgecolor="black",
        linewidth=1,
        zorder=3000,
    )
    axis[1].scatter(
        fidelity[solution_index_wait],
        waiting_time[solution_index_wait],
        marker="s",
        s=60,
        color=plot.COLORS[8],
        edgecolor="black",
        linewidth=1,
        zorder=3000,
    )
    axis[1].scatter(
        fidelity[solution_index_fid],
        waiting_time[solution_index_fid],
        marker="p",
        s=80,
        color=plot.COLORS[2],
        edgecolor="black",
        linewidth=1,
        zorder=3000,
    )
    # add annotation that star is the solution
    axis[1].annotate(
        "Balanced",
        xy=(fidelity[solution_index], waiting_time[solution_index] + 50),
        xytext=(-100, 50),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="fancy",
            fc="black",
            ec="none",
            connectionstyle="angle3,angleA=0,angleB=90",
        ),
        bbox=dict(boxstyle="round4", fc="w", ec="black"),
    )
    axis[1].annotate(
        "Fidelity",
        xy=(fidelity[solution_index_fid], waiting_time[solution_index_fid] + 35),
        xytext=(-100, -40),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="fancy",
            fc="black",
            ec="none",
            connectionstyle="angle3,angleA=90,angleB=0",
        ),
        bbox=dict(boxstyle="round4", fc="w", ec="black"),
    )
    axis[1].annotate(
        "JCT",
        xy=(fidelity[solution_index_wait], waiting_time[solution_index_wait] + 20),
        xytext=(50, 50),
        textcoords="offset points",
        arrowprops=dict(
            arrowstyle="fancy",
            fc="black",
            ec="none",
            connectionstyle="angle3,angleA=0,angleB=90",
        ),
        bbox=dict(boxstyle="round4", fc="w", ec="black"),
    )
    axis[1].set_title(
        "(b) JCT vs. Fidelity Pareto Front",
        fontsize=12,
        fontweight="bold",
    )


    plot_path = "plots/scheduling_manager/waiting_time_fidelity.pdf"
    plt.savefig(str(plot_path), bbox_inches="tight", dpi=600)

generate_jobs()
generate_schedules()
plot_fidelity_and_waiting_time()