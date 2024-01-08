import pandas as pd
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
    roc_curve,
    auc,
)
import matplotlib.pyplot as plt
import seaborn as sns


def calculate_binary_classifier_statistics(
    df: pd.DataFrame,
    group_by=None,
    probability_threshold: float = 0.5,
    payoff_column_name="複勝払戻金",
):
    """
    Calculates statistics for a binary classifier.

    Parameters
    ----------
    df : pd.DataFrame
        A dataframe containing the predictions and actual values.
        Expected columns: "actual", "pred", "pred_proba_false", "pred_proba_true"
    group_by : str, optional
        The name of a column to group by, by default None
    probability_threshold : float, optional
        The probability threshold to use when calculating statistics, by default 0.5

    Returns
    -------
    dict
        A dictionary containing the statistics.

    Examples
    --------
    >>> calculate_binary_classifier_statistics(results, group_by="場所距離芝ダ")
    {'payoff_rate': 0.0,
     'hit_rate': 0.0,
     'precision': 0.0,
     'recall': 0.0,
     'f1_score': 0.0,
     'total_bets': 0,
     'total_hits': 0,
     'bet_rate': 0.0,
     'total_payoff_amount': 0.0,
     'total_bet_amount': 0}
    """
    if probability_threshold < 0 or probability_threshold > 1:
        raise ValueError("probability_threshold must be between 0 and 1")

    bet_amount = 100
    groups = [("*", df)] if group_by is None else df.groupby(group_by, observed=False)
    results = {}
    for name, group in groups:
        if len(group) == 0:
            print(f"Skipping {name} because it has no data")
            continue
        bets = group[group["pred_proba_true"] >= probability_threshold]
        hits = group[
            (group["pred_proba_true"] >= probability_threshold) & group["actual"]
        ]
        total_hits = len(hits)
        total_payoff_amount = hits[payoff_column_name].sum() * (bet_amount / 100)
        total_bets = len(bets)
        total_bet_amount = total_bets * bet_amount
        hit_rate = total_hits / total_bets * 100 if total_bets > 0 else 0
        bet_rate = total_bets / len(group) * 100
        payoff_rate = (
            total_payoff_amount / total_bet_amount * 100 if total_bet_amount > 0 else 0
        )
        precision = precision_score(group["actual"], group["pred"], zero_division=0)
        recall = recall_score(group["actual"], group["pred"], zero_division=0)
        f1 = f1_score(group["actual"], group["pred"], zero_division=0)
        results[name] = {
            "payoff_rate": payoff_rate,
            "hit_rate": hit_rate,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "total_bets": total_bets,
            "total_hits": total_hits,
            "bet_rate": bet_rate,
            "total_payoff_amount": total_payoff_amount,
            "total_bet_amount": total_bet_amount,
        }
    return results


def plot_binary_classifier_metrics(y_true, y_pred):
    # Calculate the metrics
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    # Print the results
    # f"When your model predicts a horse will place, it is correct about {round(precision * 100, 2)}% of the time."
    print("Precision:", precision)
    # f"Your model correctly identifies {round(recall * 100, 2)}% of the horses that actually place."
    print("Recall:", recall)
    print("F1 Score:", f1)

    # Generate the confusion matrix
    conf_matrix = confusion_matrix(y_true, y_pred)

    # Plotting the confusion matrix
    _, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    sns.heatmap(conf_matrix, annot=True, fmt="g", cmap="Blues", ax=ax1)
    ax1.set_xlabel("Predicted")
    ax1.set_ylabel("Actual")
    ax1.set_title("Confusion Matrix")

    # Calculate the ROC curve points
    fpr, tpr, _ = roc_curve(y_true, y_pred)

    # Calculate the Area Under the Curve (AUC)
    roc_auc = auc(fpr, tpr)

    # Plotting
    ax2.plot(
        fpr, tpr, color="darkorange", lw=2, label="ROC curve (area = %0.2f)" % roc_auc
    )
    ax2.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
    ax2.set_xlim([0.0, 1.0])
    ax2.set_ylim([0.0, 1.0])
    ax2.set_xlabel("False Positive Rate")
    ax2.set_ylabel("True Positive Rate")
    ax2.set_title("Receiver Operating Characteristic")
    ax2.legend(loc="lower right")

    plt.show()
