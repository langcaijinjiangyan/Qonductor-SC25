import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import pyplot as plt, gridspec

from src.utils import plot

def plot_jct_fidelity(your_method_file, opponent_method_file, utilizations):
    # Read the CSV files
    df_your_method = pd.read_csv(your_method_file)
    df_opponent_method = pd.read_csv(opponent_method_file)
    df_utilizations = pd.read_csv(utilizations)

    mean_qonductor = np.mean(df_your_method["JCT"])
    mean_FCFS = np.mean(df_opponent_method["JCT"])
    print((mean_FCFS-mean_qonductor)/mean_FCFS * 100)

    # Convert the 'timestamp' column to datetime
    df_your_method['time'] = range(1, 3600, 36)
    df_opponent_method['time'] = range(1, 3600, 36)
    df_utilizations['time'] = range(1, 3600, 36)

    df_melted = pd.melt(df_utilizations, id_vars=['time'], value_vars=['Qonductor', 'FCFS'],
                    var_name='method', value_name='util')

    #print(df_melted)

    df_your_method['Scheduling'] = 'Qonductor'
    df_opponent_method['Scheduling'] = 'FCFS'

    # Concatenate the dataframes
    df_combined = pd.concat([df_your_method, df_opponent_method])

    # --- Adaptive y-axis limits ---
    f_min = df_combined["fidelity"].min()
    f_max = df_combined["fidelity"].max()
    f_pad = (f_max - f_min) * 0.15
    fidelity_ylim = (f_min - f_pad, f_max + f_pad)

    jct_max = df_combined["JCT"].max()
    jct_ylim = (0, jct_max * 1.1)

    
    # Melt the dataframe for easier plotting
    #df_melted = pd.melt(df_combined, id_vars=['time', 'Scheduling'], value_vars=['JCT', 'fidelity'], var_name='Metric', value_name='Value')


    # Create the JCT plot
    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows, ncols)
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    sns.lineplot(
        data=df_combined,
        x='time', 
        y='fidelity', 
        ax=axis[0], 
        hue='Scheduling', 
        palette=sns.color_palette("deep"), 
        style='Scheduling',
        markers=True,
        markevery=2
    )
        
    axis[0].set_xlabel('Time [s]')
    axis[0].set_xlim(0,3600)
    axis[0].set_ylim(*fidelity_ylim)
    axis[0].set_ylabel('Fidelity')
    axis[0].set_title('(a) Mean End-to-End Fidelity', fontsize=12, fontweight="bold")
    axis[0].text(0.5, 1.3, plot.HIGHERISBETTER, ha="center", va="center", transform=axis[0].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    axis[0].legend(title='')

    sns.lineplot(
        data=df_combined,
        x='time', 
        y='JCT', 
        ax=axis[1], 
        hue='Scheduling', 
        palette=sns.color_palette("deep"), 
        style='Scheduling',
        markers=True,
        markevery=4
    )

    axis[1].set_xlim(0,3600)
    axis[1].legend(title='')
    axis[1].set_ylim(*jct_ylim)
    axis[1].set_xlabel('Time [s]')
    axis[1].set_ylabel('Completion Time [s]')
    axis[1].set_title('(b) Mean End-to-End Completion Time', fontsize=12, fontweight="bold")
    axis[1].text(0.5, 1.3, plot.LOWERISBETTER, ha="center", va="center", transform=axis[1].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    sns.lineplot(
        data=df_melted,
        x='time', 
        y='util', 
        ax=axis[2], 
        hue='method', 
        palette=sns.color_palette("deep"), 
        style='method',
        markers=True,
        markevery=3
    )

    axis[2].set_ylim(0,100)
    axis[2].legend(title='')
    axis[2].set_xlim(0,3600)
    axis[2].set_xlabel('Time [s]')
    axis[2].set_ylabel('Utilization [%]')
    axis[2].set_title('(c) Mean QPU Utilization', fontsize=12, fontweight="bold")
    axis[2].text(0.5, 1.3, plot.HIGHERISBETTER, ha="center", va="center", transform=axis[2].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    plt.tight_layout(w_pad=0.2)
    plt.savefig(
        "plots/end_to_end_performance.pdf",
        dpi=600,
        bbox_inches="tight",
    )

# Call the function with the CSV file names
your_method_file = 'data/end_to_end/jct_fidelity_1500.csv'
opponent_method_file = 'data/end_to_end/jct_fidelity_fcfs.csv'
utilizations = 'data/end_to_end/utilizations.csv'
plot_jct_fidelity(your_method_file, opponent_method_file, utilizations)
