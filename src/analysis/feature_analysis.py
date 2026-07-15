import logging
import pathlib
import textwrap

from matplotlib import pyplot as plt, gridspec
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.feature_selection import (
    VarianceThreshold,
    SelectKBest,
    f_regression,
    mutual_info_regression,
)
from sklearn.inspection import permutation_importance
from sklearn.model_selection import train_test_split

from src.execution_time.regression_estimator import RegressionEstimator
from src.utils import plot
from src.utils.database import get_jobs_from_database

logger = logging.getLogger(__name__)


def analyze_model_features(plot_folder: pathlib.Path) -> None:
    """
    Analyze execution time estimation regression model features
    :param plot_folder: Folder to save plots
    """
    # Create plot folder
    plot_folder.mkdir(parents=True, exist_ok=True)

    # Create regression estimator
    regression_estimator = RegressionEstimator()

    # Load dataset
    x, y = regression_estimator._load_dataset()
    x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=0)

    # Load model with best parameters
    model_name = "Extra Trees"
    model = regression_estimator._get_available_models()[model_name]

    all_params = regression_estimator.model["model"].get_params()
    grid_params = regression_estimator._get_model_parameter_grids()[model_name]
    model_params = {
        name: value
        for name, value in all_params.items()
        if f"model__{name}" in grid_params
    }
    model["model"].set_params(**model_params)

    # Train and test model
    model.fit(x_train, y_train)
    logger.info("Training score: %s", model.score(x_train, y_train))
    logger.info("Testing score: %s", model.score(x_test, y_test))

    # Plot Mean Decrease in Impurity (MDI)
    feature_importance = model["model"].feature_importances_
    feature_names = np.array(regression_estimator._get_feature_names())
    selected_feature_names = feature_names[model["selection"].get_support()]
    model_importance = pd.Series(
        feature_importance, index=selected_feature_names
    ).sort_values(ascending=True)
    ax = model_importance.plot.barh(
        color=plot.COLORS[0],
        figsize=plot.FIGURE_SIZE,
        edgecolor="black",
        linewidth=1.5,
        zorder=2000,
    )
    ax.set_title("Extra Trees Feature Importance (MDI)")
    ax.set_xlabel("Mean Decrease in Impurity")
    ax.set_ylabel("Feature")
    ax.figure.tight_layout()
    plt.savefig(plot_folder / "mdi.pdf", dpi=600, bbox_inches="tight")

    plt.clf()

    # Analyze permutation importance
    result = permutation_importance(
        model, x_test, y_test, n_repeats=10, random_state=0, n_jobs=-1
    )
    sorted_importance_idx = result.importances_mean.argsort()[
        -len(selected_feature_names) :
    ]

    importance = pd.DataFrame(
        result.importances[sorted_importance_idx].T,
        columns=feature_names[sorted_importance_idx],
    )
    ax = importance.plot.box(
        vert=False,
        whis=10,
        figsize=plot.FIGURE_SIZE,
        zorder=2000,
    )

    ax.set_title("Permutation Importance")
    ax.set_xlabel("Decrease in Accuracy Score")
    ax.set_ylabel("Feature")
    ax.figure.tight_layout()
    plt.savefig(
        plot_folder / "permutation_importance.pdf",
        dpi=600,
        bbox_inches="tight",
    )


def analyze_dataset_features(plot_folder: pathlib.Path) -> None:
    """
    Analyze dataset features
    :param plot_folder: Folder to save plots
    """
    # Create plot folder
    plot_folder.mkdir(parents=True, exist_ok=True)

    # Create regression estimator
    regression_estimator = RegressionEstimator()

    # Load dataset
    x, y = regression_estimator._load_dataset()
    feature_names = np.array(regression_estimator._get_feature_names())
    logger.info("Dataset features: %s", feature_names)

    # Analyze variance of features
    variance_selection = VarianceThreshold()
    x = variance_selection.fit_transform(x)
    selected_features = feature_names[variance_selection.get_support()]
    logger.info("Features with non-zero variance: %s", selected_features)
    discarded_features = feature_names[~variance_selection.get_support()]
    logger.info("Features with zero variance: %s", discarded_features)

    # Plot f-score
    f_score_selection = SelectKBest(score_func=f_regression, k=10)
    f_score_selection.fit(x, y)
    scores = f_score_selection.scores_
    scores = pd.Series(scores, index=selected_features).sort_values(
        ascending=True
    )
    ax = scores.plot.barh(
        color=plot.COLORS[0],
        figsize=plot.FIGURE_SIZE,
        edgecolor="black",
        linewidth=1.5,
        zorder=2000,
    )
    ax.set_title("Feature Univariate Score")
    ax.set_xlabel("F-score")
    ax.set_ylabel("Feature")
    ax.figure.tight_layout()
    plt.savefig(plot_folder / "f_score.pdf", dpi=600, bbox_inches="tight")

    plt.clf()

    # Plot mutual information score
    mutual_info_selection = SelectKBest(
        k=10, score_func=mutual_info_regression
    )
    mutual_info_selection.fit(x, y)
    scores = mutual_info_selection.scores_
    scores = pd.Series(scores, index=selected_features).sort_values(
        ascending=True
    )
    ax = scores.plot.barh(
        color=plot.COLORS[0],
        figsize=plot.FIGURE_SIZE,
        edgecolor="black",
        linewidth=1.5,
        zorder=2000,
    )
    ax.set_title("Feature Univariate Score")
    ax.set_xlabel("Mutual Information Score")
    ax.set_ylabel("Feature")
    ax.figure.tight_layout()
    plt.savefig(
        plot_folder / "mutual_info_score.pdf", dpi=600, bbox_inches="tight"
    )


def load_IBM_jobs():
    return get_jobs_from_database()

def get_jobs_of_size(jobs, size):
    filtered_jobs = []
    for j in jobs:
        if len(j.circuits) == size and j.shots < 8200:
            filtered_jobs.append(j)
    return filtered_jobs

def estimate_execution_time_for_job(estimator, job):
    for c in job.circuits:
        c.load_circuit()
    circuits = [c.quantum_circuit for c in job.circuits]

    return estimator.estimate_execution_time(circuits, shots=job.shots)


def filter_estimations():
    jobs = load_IBM_jobs()
    regression_estimator = RegressionEstimator()

    job_sizes = [1, 3, 5, 14, 20, 30, 40, 43, 60, 80, 100]
    jobs_to_predict = []

    for js in job_sizes:
        j = get_jobs_of_size(jobs, js)
        if len(j) > 0:
            jobs_to_predict.append(j)

    results = []

    for js in jobs_to_predict:
        max_diff = 1000000
        best_pair = None

        for j in js:
            t1 = round(j.taken_time, 3)
            t2 = round(estimate_execution_time_for_job(regression_estimator, j), 3)
            if abs(t1-t2) < max_diff:
                max_diff = abs(t1-t2)
                best_pair = (len(j.circuits), t1, t2)
        results.append(best_pair)
   
    return results


def plot_important_analysis(plot_folder: pathlib.Path) -> None:
    # Create plot folder
    plot_folder.mkdir(parents=True, exist_ok=True)

    # Create regression estimator
    regression_estimator = RegressionEstimator()

    # Load dataset
    x, y = regression_estimator._load_dataset()
    x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=0)

    # Load model with best parameters
    model_name = "Polynomial Regression"
    model = regression_estimator._get_available_models()[model_name]

    all_params = regression_estimator.model["linearregression"].get_params()
    grid_params = regression_estimator._get_model_parameter_grids()[model_name]
    model_params = {
        name: value
        for name, value in all_params.items()
        if f"model__{name}" in grid_params
    }
    model["linearregression"].set_params(**model_params)

    # Train and test model
    model.fit(x_train, y_train)
    logger.info("Training score: %s", model.score(x_train, y_train))
    logger.info("Testing score: %s", model.score(x_test, y_test))

    abbreviation_map = {
        "swap" : "SWAPs",
        "circuit_count": "circs",
        "num_qubits": "width",
        "depth": "depth",
        "shots": "shots"
    }

    variance_selection = VarianceThreshold()
    x = variance_selection.fit_transform(x)    
    feature_names = np.array(regression_estimator._get_feature_names())
    selected_feature_names = feature_names
    selected_features = feature_names[variance_selection.get_support()]
    selected_feature_names = [abbreviation_map[i] if i in abbreviation_map else i for i in selected_feature_names]

    fig = plt.figure(figsize=plot.WIDE_FIGSIZE)
    nrows = 1
    ncols = 3
    gs = gridspec.GridSpec(nrows=nrows, ncols=ncols)
    axis = [fig.add_subplot(gs[i, j]) for i in range(nrows) for j in range(ncols)]
    sns.set_theme(style="whitegrid")

    estimations = filter_estimations()
    df_data = []

    for tup in estimations:
        first_num, second_num, third_num = tup
        df_data.append({'Task Size': first_num, 'Type': 'Taken Time', 'Time': second_num})
        df_data.append({'Task Size': first_num, 'Type': 'Estimated Time', 'Time': third_num})

    prediction_df = pd.DataFrame(df_data)

    sns.lineplot(
        prediction_df, 
        x=prediction_df['Task Size'], 
        y=prediction_df['Time'], 
        ax=axis[0], 
        hue=prediction_df["Type"], 
        palette=sns.color_palette("deep"), 
        style=prediction_df['Type'],
        markers=True
    )

    axis[0].set_title("(a) Estimation Accuracy", fontsize=12, fontweight="bold")
    axis[0].set_xlabel("Task Size (# of Circuits)")
    axis[0].set_ylabel("Time [s]")
    axis[0].set_ylim(0, 210)
    axis[0].text(0.5, 1.18, "Equal is Better", ha="center", va="center", transform=axis[0].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    axis[0].legend()

    # Plot f-score
    f_score_selection = SelectKBest(score_func=f_regression, k=5)
    f_score_selection.fit(x, y)
    scores = f_score_selection.scores_
    scores = pd.Series(scores, index=selected_features).sort_values(
        ascending=False
    )
    scores = scores.rename(abbreviation_map)

    sns.barplot(
        x=scores.values, 
        y=scores.index,
        #orient = 'h',
        color=plot.COLORS[0],
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        ax=axis[1],
    )
    axis[1].set_title("(b) F-test", fontsize=12, fontweight="bold")
    axis[1].set_xlabel("F-score")
    axis[1].set_ylabel("Feature")
    axis[1].text(0.5, 1.18, "Higher is better →", ha="center", va="center", transform=axis[1].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
 
    mutual_info_selection = SelectKBest(
        k=5, score_func=mutual_info_regression
    )
    mutual_info_selection.fit(x, y)
    scores = mutual_info_selection.scores_
    scores = pd.Series(scores, index=selected_features).sort_values(
        ascending=False
    )
    scores = scores.rename(abbreviation_map)
    sns.barplot(
        x=scores.values, 
        y=scores.index,
        #orient = 'h',
        color=plot.COLORS[0],
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        ax=axis[2],
    )
    axis[2].set_title("(c) Feature Univariate Score", fontsize=12, fontweight="bold")
    axis[2].set_xlabel("Mutual Information Score")
    axis[2].set_ylabel("Feature")
    axis[2].text(0.5, 1.18, "Higher is better →", ha="center", va="center", transform=axis[2].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)
    """
    # Analyze permutation importance
    result = permutation_importance(
        model, x_test, y_test, n_repeats=10, random_state=0, n_jobs=-1
    )

    sorted_importance_idx = result.importances_mean.argsort()[
        -len(selected_feature_names) :
    ]

    importance = pd.DataFrame(
        result.importances[sorted_importance_idx].T,
        columns=feature_names[sorted_importance_idx],
    )
    importance = importance[importance.columns[::-1]]
    importance = importance.iloc[:, :6]

    for i, column in enumerate(importance.columns):
        if column in abbreviation_map:
            importance.rename(columns={column: abbreviation_map[column]}, inplace=True)

    melted_df = importance.melt(var_name='Columns', value_name='Values')

    sns.boxplot(
        data=melted_df,
        x='Values', y='Columns',
        color=plot.COLORS[0],
        linecolor="black",
        linewidth=1.5,
        whis=10,
        ax=axis[1]
    )
    axis[1].text(0.5, 1.3, "Higher is better →", ha="center", va="center", transform=axis[1].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    axis[1].set_title("(b) Permutation Importance", fontsize=12, fontweight="bold")
    axis[1].set_xlabel("Decrease in Accuracy Score")
    axis[1].set_ylabel("")
    axis[1].set_xlim(0, 1)

    x, y = regression_estimator._load_dataset()
    feature_names = np.array(regression_estimator._get_feature_names())

    variance_selection = VarianceThreshold()
    x = variance_selection.fit_transform(x)
    selected_features = feature_names[variance_selection.get_support()]

    selected_features = [abbreviation_map[i] if i in abbreviation_map else i for i in selected_features]

    mutual_info_selection = SelectKBest(
        k=10, score_func=mutual_info_regression
    )
    mutual_info_selection.fit(x, y)
    scores = mutual_info_selection.scores_
    scores = pd.Series(scores, index=selected_features).sort_values(
        ascending=False
    )
    to_keep = ['shots', 'depth', 'circs', 'qbits', 'cx', 'sx']
    scores = scores[scores.index.isin(to_keep)]

    sns.barplot(
        x=scores.values, 
        y=scores.index,
        color=plot.COLORS[0],
        edgecolor="black",
        hatch="/",
        linewidth=1.5,
        ax=axis[2],
    )
    axis[2].text(0.5, 1.3, "Higher is better →", ha="center", va="center", transform=axis[2].transAxes, fontweight="bold", color="navy", fontsize=plot.ISBETTER_FONTSIZE)

    axis[2].set_title("(c) Feature Univariate Score", fontsize=12, fontweight="bold")
    axis[2].set_xlabel("Mutual Information Score")
    axis[2].set_ylabel("")
    """

    plt.subplots_adjust(wspace=0.35)
    plt.savefig(plot_folder / "regression_model_analysis.pdf", dpi=600, bbox_inches="tight")

plot_important_analysis(pathlib.Path("plots/regression_features"))
#filter_estimations()