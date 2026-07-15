import logging
from pathlib import Path
from collections import Counter

import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.linear_model import LinearRegression

from src.utils.plot import *
from src.utils.database import (
    get_jobs_from_database,
    extract_jobs_from_ibm_quantum,
)

logger = logging.getLogger(__name__)


def analyze_overall_taken_time(plot_path: str = None) -> None:
    """
    Analyze the taken time of the jobs
    :param plot_path: Path to save the plot
    """
    if plot_path is None:
        plot_path = (
            Path(__file__).absolute().parents[2] / "plots" / "taken_time"
        )

    job_taken_time = []
    jobs = get_jobs_from_database()
    for job in jobs:
        if len(job.circuits) > 1:
            job_taken_time.append(job.taken_time)

    sns.set_style("ticks")
    sns.color_palette("pastel")

    sns.displot(
        job_taken_time,
        kind="ecdf",
        stat="count",
        height=6,
        aspect=1.5,
    )

    plt.axvline(x=10, color="r", linestyle="-")
    plt.text(
        20,
        2000,
        "10 seconds",
        verticalalignment="center",
        bbox=dict(facecolor="white", alpha=0.5),
        fontsize=12,
    )
    plt.ylabel("Job count", fontsize=14, labelpad=14)
    plt.xlabel("QR utilization, seconds", fontsize=14, labelpad=14)
    plt.title(f"Job Taken Time CDF", fontsize=18, pad=14)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.subplots_adjust(bottom=0.14, top=0.9, left=0.12)
    plt.savefig(plot_path / "overall_utilization.png", dpi=600)


def analyze_backend_taken_time(plot_path: str = None) -> None:
    """
    Analyze the taken time of the jobs for each backend
    :param plot_path: Path to save the plots
    """
    if plot_path is None:
        plot_path = (
            Path(__file__).absolute().parents[2] / "plots" / "taken_time"
        )

    job_info = []
    jobs = get_jobs_from_database()
    for job in jobs:
        logger.info("Processing job %s", job.ibm_quantum_id)
        if len(job.circuits) > 1:
            job_info.append(
                {
                    "backend": job.backend_name,
                    "taken_time": job.taken_time,
                }
            )
    job_info = pd.DataFrame(job_info)

    sns.set_style("ticks")
    sns.color_palette("tab10")

    for backend in job_info["backend"].unique():
        # Plot distribution of job sizes
        sns.displot(
            job_info[job_info["backend"] == backend],
            y="taken_time",
            kind="ecdf",
            stat="count",
            height=6,
            aspect=1.5,
        )
        plt.xlabel("Job count", fontsize=14, labelpad=14)
        plt.ylabel("QR utilization, seconds", fontsize=14, labelpad=14)
        plt.title(f"Backend {backend} utilization CDF", fontsize=18, pad=14)
        plt.yticks(fontsize=12)
        plt.xticks(fontsize=12)
        plt.subplots_adjust(bottom=0.14, top=0.9, left=0.12)
        plt.savefig(plot_path / f"{backend}_utilization.png", dpi=600)

        plt.clf()


def analyze_data_distribution(plot_path: str = None) -> None:
    """
    Run analysis on the dataset
    :param plot_path: Path to save the plots
    """
    if plot_path is None:
        plot_path = Path(__file__).absolute().parents[2] / "plots" / "dataset"

    jobs = get_jobs_from_database()

    # Plot distribution of job sizes
    sns.displot(
        [len(job.circuits) for job in jobs],
        kde=True,
        height=6,
        aspect=1.5,
        color=plot.COLORS[0],
    )
    plt.xlabel("Job Size (#Circuits)", fontsize=14, labelpad=14)
    plt.ylabel("Count", fontsize=14, labelpad=14)
    plt.title("Distribution of Job Sizes", fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.subplots_adjust(bottom=0.14, top=0.9, left=0.12)
    plt.savefig(
        plot_path / "job_size_distribution.pdf", dpi=600, bbox_inches="tight"
    )

    plt.clf()

    # Plot distribution of backend jobs
    sns.displot(
        [
            job.backend_name
            for job in jobs
            if job.backend_name != "ibmq_qasm_simulator"
        ],
        kde=True,
        height=6,
        aspect=1.5,
        color=plot.COLORS[0],
    )
    plt.ylabel("Count", fontsize=14, labelpad=14)
    plt.xlabel("Backend", fontsize=14, labelpad=0)
    plt.title("Job Distribution over Backends", fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=10, rotation=45, ha="right")
    plt.subplots_adjust(bottom=0.25, top=0.9, left=0.12, right=0.95)
    plt.savefig(
        plot_path / "job_backend_distribution.pdf",
        dpi=600,
        bbox_inches="tight",
    )

    plt.clf()

    # Plot distribution of job shots
    sns.displot(
        [job.shots for job in jobs],
        kde=True,
        height=6,
        aspect=1.5,
        color=plot.COLORS[0],
    )
    plt.xlabel("Shots", fontsize=14, labelpad=14)
    plt.ylabel("Count", fontsize=14, labelpad=14)
    plt.title("Distribution of Job Shots", fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=10)
    plt.subplots_adjust(bottom=0.14, top=0.9, left=0.12)
    plt.savefig(
        plot_path / "job_shots_distribution.pdf", dpi=600, bbox_inches="tight"
    )

def analyze_data_distribution_single_plot(plot_path: str = None) -> None:
    """
    Run analysis on the dataset
    :param plot_path: Path to save the plots
    """
    if plot_path is None:
        plot_path = Path(__file__).absolute().parents[2] / "plots" / "dataset"

    jobs = get_jobs_from_database()

    fig = plt.figure(figsize=WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows=nrows, ncols=ncols)

    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")
    #fig, axis = plt.subplots(1, ncols, figsize=WIDE_FIGSIZE, sharey=True)

    # Plot distribution of job sizes
    ls = [len(job.circuits) for job in jobs]
    counts = Counter(ls)
    ls = [elem for elem, count in counts.items() for _ in range(count) if count > 9]

    sns.histplot(
        ls,
        kde=True,
        bins=20,
        color= COLORS[0],
        #palette=sns.color_palette("pastel"),
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        alpha=0.85,
        #height=6,
        #aspect=1.5,        
        ax=axis[0]
    )
    axis[0].set_xlabel("Job Size (# of Circuits)", fontsize=12, labelpad=14)
    axis[0].set_ylabel("Count", fontsize=12, labelpad=14)
    axis[0].set_title("(a) Distribution of Job Sizes", fontsize=12, fontweight="bold")
    axis[0].set_xlim(0, 105)
    #axis[0].set_yticks(fontsize=12)
    #axis[0].set_xticks(fontsize=12)
    #axis[0].subplots_adjust(bottom=0.14, top=0.9, left=0.12)
    

    #plt.clf()

    # Plot distribution of backend jobs
    ls = [
            job.backend_name.split("_")[1]
            for job in jobs
            if "simulator" not in job.backend_name
        ]
    counts = Counter(ls)
    ls = [elem for elem, count in counts.items() for _ in range(count) if count >= 10]
    sns.histplot(
        ls,
        kde=True,
        bins=10,
        color= COLORS[0],
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        alpha=0.85,
        #height=6,
        #aspect=1.5,        
        ax=axis[1]
    )
    axis[1].set_ylabel("", fontsize=12, labelpad=14)
    axis[1].set_xlabel("IBM QPU", fontsize=12, labelpad=0)
    axis[1].set_title("(b) Job Distribution over IBM QPUs", fontsize=12, fontweight="bold")
    axis[1].set_xticklabels(axis[1].get_xticklabels(), rotation=30, ha="center")
    #axis[1].set_yticks(fontsize=12)
    #axis[1].set_xticklabels(fontsize=12, rotation=45, ha="right")
    #axis[1].subplots_adjust(bottom=0.25, top=0.9, left=0.12, right=0.95)
    
    # Plot distribution of job shots

    sns.histplot(
        [job.shots for job in jobs],
        kde=True,
        bins=25,
        color= COLORS[0],
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        alpha=0.85,
        #height=6,
        #aspect=1.5,        
        ax=axis[2]
    )
    axis[2].set_xlabel("Number of Shots", fontsize=12, labelpad=14)
    axis[2].set_ylabel("", fontsize=12, labelpad=14)
    axis[2].set_title("(c) Distribution of Job Shots", fontsize=12, fontweight="bold")
    axis[2].set_xlim(0, 21000)
    #axis[2].set_yticks(fontsize=12)
    #axis[2].set_xticks(fontsize=12)
    #axis[2].subplots_adjust(bottom=0.14, top=0.9, left=0.12)

    plt.tight_layout()
    plt.savefig(
        "job_shots_distribution.pdf", dpi=600, bbox_inches="tight"
    )

def determine_cold_start() -> float:
    """
    Determine the cold start of the jobs
    :return: Cold start time
    """
    x = []
    y = []
    for job in extract_jobs_from_ibm_quantum():
        logger.info("Extracting features from job %s", job.job_id())
        x.append(sum([circuit.size() for circuit in job.circuits()]))
        y.append(job.result().time_taken)

    logger.info("Fitting linear regression model")
    model = LinearRegression(n_jobs=-1)
    model.fit(np.array(x).reshape(-1, 1), np.array(y))
    logger.info("Model fitted")

    return model.intercept_


analyze_data_distribution_single_plot(".")