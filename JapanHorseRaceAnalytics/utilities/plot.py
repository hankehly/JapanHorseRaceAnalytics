import japanize_matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.metrics import auc, confusion_matrix, roc_curve


def plot_confusion_matrix(y_test, y_pred, figsize=(15, 5)):
    conf_matrix = confusion_matrix(y_test, y_pred)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize)
    sns.heatmap(conf_matrix, annot=True, fmt="g", cmap="Blues", ax=ax1)
    ax1.set_xlabel("Predicted")
    ax1.set_ylabel("Actual")
    ax1.set_title("Confusion Matrix")
    sns.heatmap(
        conf_matrix / conf_matrix.sum(axis=1)[:, None],
        annot=True,
        fmt=".2%",
        cmap="Blues",
        ax=ax2,
    )
    ax2.set_xlabel("Predicted")
    ax2.set_ylabel("Actual")
    ax2.set_title("Normalized Confusion Matrix")
    plt.tight_layout()
    return fig, (ax1, ax2)


def plot_roc_curve(y_test, y_pred, figsize=(10, 10)):
    fpr, tpr, _ = roc_curve(y_test, y_pred)
    roc_auc = auc(fpr, tpr)
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(
        fpr, tpr, color="darkorange", lw=2, label="ROC curve (area = %0.2f)" % roc_auc
    )
    ax.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.0])
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title("Receiver Operating Characteristic")
    ax.legend(loc="lower right")
    plt.tight_layout()
    return fig, ax


def plot_feature_importances(
    feature_names,
    feature_importances,
    figsize=(10, 12),
    top_n: int = None,
):
    if top_n is None:
        top_n = len(feature_names)
    elif top_n < 1:
        raise ValueError("top_n must be a positive integer")
    feature_importances = zip(feature_names, feature_importances)
    feature_importances_df = (
        pd.DataFrame(data=feature_importances, columns=["feature", "importance"])
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    japanize_matplotlib.japanize()
    fig, ax = plt.subplots(figsize=figsize)
    sns.barplot(
        x="importance", y="feature", data=feature_importances_df.iloc[:top_n], ax=ax
    )
    ax.grid(axis="x")
    ax.set_title("Feature Importances")
    ax.set_xlabel("Importance")
    ax.set_ylabel("Features")
    plt.tight_layout()
    return fig, ax
