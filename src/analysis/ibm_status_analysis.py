import datetime as dt
import json
import logging
import pathlib
import time
import datetime
import numpy as np

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt, gridspec
from qiskit_ibm_provider import IBMProvider

from src.utils import plot

logger = logging.getLogger(__name__)


def monitor_ibm_status(
    account: str,
    period: int,
    duration: dt.timedelta,
    data_folder: pathlib.Path,
) -> None:
    """
    Monitor the status of IBM Quantum backends for a given account
    :param account: IBM Quantum account
    :param period: Time between measurements in seconds
    :param duration: Duration of load monitoring
    :param data_folder: Folder to save the data to
    """
    logger.info("Monitoring load on IBM Quantum for account %s", account)
    provider = IBMProvider(name=account)
    backends = provider.backends(simulator=False)
    flush_period = dt.timedelta(hours=1)
    flush_time = dt.datetime.now(dt.timezone.utc) + flush_period
    monitoring_time = dt.datetime.now(dt.timezone.utc) + duration
    monitoring_data = []
    data_folder.mkdir(parents=True, exist_ok=True)
    while dt.datetime.now(dt.timezone.utc) < monitoring_time:
        iteration_start = dt.datetime.now(dt.timezone.utc)
        logger.info("Monitoring iteration at %s", iteration_start)
        for backend in backends:
            monitoring_data.append(
                {
                    "backend": backend.name,
                    "status": backend.status().to_dict(),
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
            )
        if dt.datetime.now(dt.timezone.utc) > flush_time:
            file_path = data_folder / (flush_time.isoformat() + ".json")
            logger.info("Flushing data to %s", file_path)
            with open(file_path, "w+") as file:
                json.dump(monitoring_data, file)
            flush_time = dt.datetime.now(dt.timezone.utc) + flush_period
            monitoring_data = []
        iteration_end = dt.datetime.now(dt.timezone.utc)
        if iteration_end - iteration_start < dt.timedelta(seconds=period):
            time.sleep(
                period - (iteration_end - iteration_start).total_seconds()
            )

    file_path = data_folder / (
        dt.datetime.now(dt.timezone.utc).isoformat() + ".json"
    )
    logger.info("Flushing data to %s", file_path)
    with open(file_path, "w+") as file:
        json.dump(monitoring_data, file)
    logger.info("Monitoring finished")


def analyze_job_frequency(
    data_folder: pathlib.Path, plot_folder: pathlib.Path
) -> None:
    """
    Analyze job frequency based on collected data
    :param data_folder: Folder containing the IBM monitoring data
    :param plot_folder: Folder to save the plots to
    """
    plot_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Analyzing job frequency")
    combined_data = pd.DataFrame()

    for file in data_folder.glob("*.json"):
        logger.info("Processing file %s", file)
        with open(file) as f:
            data = json.load(f)
        data = pd.DataFrame(data)
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    combined_data["timestamp"] = pd.to_datetime(
        combined_data["timestamp"], utc=True, format="ISO8601"
    )

    combined_data = pd.concat(
        [
            combined_data.drop(["status"], axis=1),
            combined_data["status"].apply(pd.Series),
        ],
        axis=1,
    )

    combined_data = combined_data[
        (combined_data["operational"] == True)
        & (combined_data["status_msg"] == "active")
    ]

    
    combined_data["jobs_diff"] = (
        combined_data.sort_values(["backend", "timestamp"])
        .groupby("backend")["pending_jobs"]
        .diff()
        .fillna(0)
    )
    
    combined_data["new_jobs"] = combined_data["jobs_diff"].apply(
        lambda x: x if 0 < x < 50 else 0
    )
    
    for day in combined_data["timestamp"].dt.date.unique():
        day_data = combined_data[
            (combined_data["timestamp"].dt.date == day)
            & (combined_data["new_jobs"] > 0)
        ]
        day_data["hour"] = day_data["timestamp"].dt.hour
        day_data = day_data.groupby("hour")["new_jobs"].sum()
        fig = plt.figure(figsize=plot.FIGURE_SIZE)
        gs = gridspec.GridSpec(nrows=1, ncols=1)
        axis = fig.add_subplot(gs[0, 0])
        axis.set_ylabel("Number of Jobs")
        axis.set_xlabel("Hour")
        axis.grid(axis="y", linestyle="-", zorder=-1)
        bar_width = 0.95 / 2
        axis.bar(
            day_data.index,
            day_data,
            bar_width,
            hatch=plot.HATCHES[0],
            color=plot.COLORS[0],
            edgecolor="black",
            linewidth=1.5,
            error_kw=dict(lw=2, capsize=3),
            zorder=2000,
        )
        axis.set_title(
            f"Job Frequency for {day}",
            fontsize=12,
            fontweight="bold",
        )
        axis.set_xticks(range(0, 25, 4))
        plt.savefig(
            plot_folder / f"{day}_job_frequency.png",
            dpi=600,
            bbox_inches="tight",
        )
        plt.clf()
        logger.info("Saved plot for %s", day)

    combined_data["day"] = combined_data["timestamp"].dt.date
    combined_data["hour"] = combined_data["timestamp"].dt.hour
    combined_data = combined_data.groupby(["day", "hour"])["new_jobs"].sum()
    combined_data = combined_data.groupby("hour").mean()
    fig = plt.figure(figsize=plot.COLUMN_FIGSIZE)
    gs = gridspec.GridSpec(nrows=1, ncols=1)
    axis = fig.add_subplot(gs[0, 0])
    axis.set_ylabel("Number of Incoming Jobs")
    axis.set_xlabel("Hour (UTC)")
    axis.grid(axis="y", linestyle="-", zorder=-1)
    bar_width = 0.95 / 2
    axis.bar(
        combined_data.index,
        combined_data,
        bar_width,
        color=plot.COLORS[0],
        edgecolor="black",
        linewidth=1.5,
        error_kw=dict(lw=2, capsize=3),
        zorder=2000,
    )
    #axis.set_title(
        #"IBM Quantum Job Frequency",
        #fontsize=12,
        #fontweight="bold",
    #)
    axis.axhline(
        combined_data.mean(),
        color=plot.COLORS[1],
        linestyle="--",
        linewidth=1.5,
        zorder=2000,
    )
    axis.set_xticks(range(0, 25, 4))
    plt.savefig(
        plot_folder / "job_frequency.pdf", dpi=600, bbox_inches="tight"
    )
    logger.info("Saved plot for average job frequency")

def analyze_and_plot_ibm_status(
    data_folder: pathlib.Path, plot_folder: pathlib.Path
) -> None:
    """
    :param data_folder: Folder containing the IBM monitoring data
    :param plot_folder: Folder to save the plots to
    """
    plot_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Analyzing job frequency")
    combined_data = pd.DataFrame()

    queue_folder = data_folder / "load_imbalance"

    for file in queue_folder.glob("*.json"):
        logger.info("Processing file %s", file)
        with open(file) as f:
            data = json.load(f)
        data = pd.DataFrame(data)
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    combined_data["timestamp"] = pd.to_datetime(
        combined_data["timestamp"], utc=True, format="ISO8601"
    )

    combined_data = pd.concat(
        [
            combined_data.drop(["status"], axis=1),
            combined_data["status"].apply(pd.Series),
        ],
        axis=1,
    )

    combined_data = combined_data[
        (combined_data["operational"] == True)
        & (combined_data["status_msg"] == "active")
    ]

    specific_backends = ['ibm_brisbane', 'ibm_cusco', 'ibm_nazca', 'ibm_sherbrooke','ibm_lagos', 'ibm_perth', 'ibm_nairobi']
    combined_data = combined_data[~combined_data['backend_name'].isin(specific_backends)]
    
    combined_data["jobs_diff"] = (
        combined_data.sort_values(["backend", "timestamp"])
        .groupby("backend")["pending_jobs"]
        .diff()
        .fillna(0)
    )
    
    combined_data["new_jobs"] = combined_data["jobs_diff"].apply(
        lambda x: x if 0 < x < 50 else 0
    )
       
    combined_data['day'] = pd.to_datetime(combined_data['timestamp'])
    combined_data['date'] = combined_data['day'].dt.date

    # removing these dates to plot just one week
    dates_to_remove = [datetime.date(2023, 11, 16), datetime.date(2023, 11, 17), datetime.date(2023, 11, 18), datetime.date(2023, 11, 19)]
    
    combined_data = combined_data[~combined_data['date'].isin(dates_to_remove)]
    
    new_df = combined_data.groupby(['date', 'backend_name']).agg({'new_jobs': 'sum', 'pending_jobs': 'mean'}).reset_index()
    new_df['backend_name'] = new_df['backend_name'].str.split('_').str[-1]
    print(new_df)

    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows=nrows, ncols=ncols)
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    temporal_dt = pd.read_csv("data/ibm_status/temporal_variance/perth_fidelities_6_qubits.csv")
    spatial_dt = pd.read_csv("data/ibm_status/spatial_variance/spatial_variance_12_qubits.csv")
    temporal_dt["tmp"] = [0 for i in range(0, 120)]

    x0 = ["12 qubits", "24 qubits"]

    y = np.array(
		[
			[16.6, 31.3, 2.6],
			[2.5, 12, 456.5]
		]
	)
    data = {
        "qubits": [12, 24],
        "Classical Runtime": [16.6, 2.5],
        "Quantum Runtime": [31.3, 12],
        "Fidelity": [2.6, 456.5]
    }
    overheads_dt = pd.DataFrame(data)
    overheads_dt = overheads_dt.set_index("qubits")
    overheads_dt_long = overheads_dt.reset_index().melt(id_vars="qubits", var_name="overhead", value_name="value")

    bplt = sns.barplot(
        overheads_dt_long, 
        x=overheads_dt_long["qubits"], 
        y=overheads_dt_long["value"],
        hue=overheads_dt_long["overhead"],
        palette=[plot.COLORS[2], plot.COLORS[3], plot.COLORS[0]],
        #color=plot.COLORS[0],
        edgecolor="black",
        #hatch="/",
        #markers=True,
        linewidth=1.5,
        ax=axis[0],                  
    )
    #hatches = ['**', 'OO', 'XX', '++', '--', '||', '//', '\\\\', '..',]
    hatches = ['//', '//', '\\\\', '\\\\', '..', '..', '//', '\\\\', '..',]
    hatches = ['..', '..', '\\\\', '\\\\', '--', '--', '..', '\\\\', '--',]
    #hatches = ['//', '//', '//', '\\\\', '\\\\', '\\\\', '..', '..', '..']
    for i, bar in enumerate(bplt.patches):
        bar.set_hatch(hatches[i])

	#grouped_bar_plot(axis[0], y, yerr, ["Classical Overhead", "Quantum Overhead", "Fidelity Improvement"], show_average_text=False)
    axis[0].set_xlabel("Number of Qubits")
    axis[0].set_ylabel("Relative Increase (×)")
    axis[0].set_xticklabels(x0)
    axis[0].grid(axis="y", linestyle="-", zorder=-1)
    axis[0].set_yscale("log")
    axis[0].set_title("(a) Impact of Circuit Cutting", fontsize=12, fontweight="bold")
    #axis[0].text(0.5, 1.18, plot.LOWERISBETTER, ha="center", va="center", fontweight="bold", color="navy", fontsize=ISBETTER_FONTSIZE, transform=axis[0].transAxes)
    axis[0].legend()
    axis[0].axhline(1, color="red", linestyle="--", linewidth=2)


    x = np.array(["cairo", "hanoi", "kolkata", "mumbai", "algiers", "auckland"])

    sns.barplot(
        spatial_dt, 
        x=spatial_dt["bench_name"], 
        y=spatial_dt["fidelity"],
        yerr=[spatial_dt["fidelity_std"]],
        #hue=spatial_dt["bench_name"],
        color=plot.COLORS[2],
        #style=spatial_dt['fidelity Front'],
        #markers=True,
        edgecolor="black",
        hatch="..",
        linewidth=1.5,
        ax=axis[1],                  
    )

    for index, row in spatial_dt.iterrows():
        axis[1].text(index, row['fidelity'] + 0.05, str(round(row['fidelity'], 2)), color='black', ha="center")

    axis[1].set_ylabel("Fidelity")
    axis[1].set_xlabel("IBM QPU")
    axis[1].set_ylim(0.2, 1)
    axis[1].set_xticklabels(x, rotation=20)
    axis[1].set_title("(b) Spatial Performance Variance", fontsize=12, fontweight="bold")
    axis[1].text(2.5, 1.18, plot.HIGHERISBETTER, ha="center", va="center", fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    """
    sns.lineplot(
        temporal_dt, 
        x=range(0, 120), 
        y=temporal_dt["fidelity"], 
        palette=sns.color_palette("deep"),
        ax=axis[1],             
    )

    axis[1].set_ylabel("Fidelity")
    axis[1].set_xlabel("Calibration Day")
    axis[1].set_ylim(0, 1)
    axis[1].set_xlim(0, 120)
    axis[1].set_title("(b) Temporal Performance Variance", fontsize=12, fontweight="bold")
    axis[1].text(60, 1.18, plot.HIGHERISBETTER, ha="center", va="center", fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    """

    sns.lineplot(
        new_df, 
        x=new_df["date"], 
        y=new_df["pending_jobs"], 
        ax=axis[2], 
        #legend=False, 
        hue='backend_name', 
        palette=sns.color_palette("deep"), 
        style='backend_name', 
        markers=True,
    )

    x = np.array(["20-11-23", "21-11-23", "22-11-23", "23-11-23", "24-11-23", "25-11-23", "26-11-23"])
    axis[2].set_ylabel("Queue Size")
    axis[2].set_xlabel("Date")
    axis[2].set_ylim(1, 700)
    axis[2].set_yscale('log')
    axis[2].set_xticklabels(x, rotation=20)
    axis[2].set_title("(c) QPU Load Imbalance", fontsize=12, fontweight="bold")
    axis[2].text(0.5, 1.18, "Equal is better", ha="center", va="center", transform=plt.gca().transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    axis[2].legend(loc='upper left', bbox_to_anchor=(1, 1), ncol=1, title="IBM QPU")

    plt.savefig(
        plot_folder / "nisq-cloud-characteristics.pdf",
        dpi=600,
        bbox_inches="tight",
    )
    
analyze_and_plot_ibm_status(pathlib.Path("data/ibm_status"), pathlib.Path("plots/ibm_status"))