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


def plot_resource_estimator():
    df_pareto = pd.read_csv('data/resource_estimator/fidelity_runtime.csv')

    df_exec_estims = pd.read_csv('data/resource_estimator/execution_time_estimations.csv')
    df_exec_estims['Qonductor'] = abs(df_exec_estims['predicted'] - df_exec_estims['real'])
    df_exec_estims['Numerical'] = abs(df_exec_estims['predicted'] - df_exec_estims['dag'])

    df_melted_execs = df_exec_estims.melt(value_vars=['Qonductor', 'Numerical'], 
                    var_name='Estimation Method', 
                    value_name='absolute_difference')

    df_fid_estims = pd.read_csv('data/resource_estimator/fidelity_estimations.csv')
    df_fid_estims['Qonductor'] = abs(df_fid_estims['predicted'] - df_fid_estims['real'])
    df_fid_estims['Numerical'] = abs(df_fid_estims['predicted'] - df_fid_estims['model'])

    df_melted_fids = df_fid_estims.melt(value_vars=['Qonductor', 'Numerical'], 
                    var_name='Estimation Method', 
                    value_name='absolute_difference')

    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows, ncols, width_ratios=[1, 1, 1])
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    def is_pareto_efficient(costs):
        is_efficient = np.ones(costs.shape[0], dtype=bool)
        for i, c in enumerate(costs):
            if is_efficient[i]:
                is_efficient[is_efficient] = np.any(costs[is_efficient] < c, axis=1)  # Keep any point with a lower cost
                is_efficient[i] = True  # And keep self
        return is_efficient

    costs = np.column_stack((df_pareto['runtime'], 1 - df_pareto['fidelity']))
    pareto_efficient_mask = is_pareto_efficient(costs)
    print(df_pareto['runtime'][pareto_efficient_mask])
    print(df_pareto['fidelity'][pareto_efficient_mask])

    axis[0].scatter(        
        df_pareto['runtime'],
        df_pareto['fidelity'],
        label='Data Points',
        color=plot.COLORS[0],
        linewidth=1,
        edgecolor="black",
    )

    axis[0].scatter(
        df_pareto['runtime'][pareto_efficient_mask], 
        df_pareto['fidelity'][pareto_efficient_mask], 
        #label='Pareto Front',
        marker="*",
        s=120,
        color=plot.COLORS[1],
        edgecolor="black",
        linewidth=1,
    )


    axis[0].set_xlim(40, 160)
    axis[0].set_xlabel('Estimated Runtime [s]')
    axis[0].set_ylabel('Estimated Fidelity')
    axis[0].set_title('(a) Pareto Front of Fidelity-Runtime Tradeoff', fontsize=12, fontweight="bold",)

    sns.ecdfplot(data=df_melted_fids, x='absolute_difference', ax=axis[1], hue='Estimation Method', palette=sns.color_palette("deep"))
    axis[1].set_xlabel('Fidelity Estimation Error')
    axis[1].set_ylabel('Probability')
    axis[1].set_title('(b) CDF of Fidelity Estimation Error', fontsize=12, fontweight="bold",)
    axis[1].set_xlim(0, 0.5)

    sns.ecdfplot(data=df_melted_execs, x='absolute_difference', ax=axis[2], hue='Estimation Method', palette=sns.color_palette("deep"))
    axis[2].set_xlabel('Execution Time Estimation Error [ms]')
    axis[2].set_ylabel('Probability')
    axis[2].set_title('(c) CDF of Execution Time Estimation Error', fontsize=12, fontweight="bold",)
    axis[2].set_xlim(0, 5000)

    plt.tight_layout(w_pad=0.2)
    plt.savefig(
        "plots/resource_estimator_analysis.pdf",
        dpi=600,
        bbox_inches="tight",
    )

plot_resource_estimator()