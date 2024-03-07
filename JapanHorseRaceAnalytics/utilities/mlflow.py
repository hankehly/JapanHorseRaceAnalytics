import pandas as pd
from mlflow.types.schema import ColSpec

from JapanHorseRaceAnalytics.utilities.structured_logger import logger


def get_mlflow_col_spec_type_for_pandas_dtype(dtype) -> str:
    dtype_str = str(dtype).lower()
    if dtype_str.startswith("float"):
        return "double"
    elif dtype_str.startswith("int"):
        return "long"
    elif dtype_str == "category":
        return "string"
    else:
        raise ValueError(f"Unexpected dtype: {dtype}")


def get_colspecs(df: pd.DataFrame) -> list[ColSpec]:
    return [
        ColSpec(get_mlflow_col_spec_type_for_pandas_dtype(dtype), col)
        for col, dtype in df.dtypes.items()
    ]


def no_progress_loss(iteration_stop_count=10, absolute_increase=0.0):
    """
    Stop function that will stop after X iteration if the loss doesn't decrease by at least the absolute_increase value.

    Parameters
    ----------
    iteration_stop_count: int
        Search will stop if the loss doesn't improve by at least absolute_increase after this number of iterations.
    absolute_increase: float
        Specifies the minimum absolute improvement required within iteration_stop_count.
        Early stop will be triggered if the loss didn't decrease by more than this value
        after iteration_stop_count rounds.
    """

    def stop_fn(trials, best_loss=None, iteration_no_progress=0):
        # https://github.com/hyperopt/hyperopt/issues/808
        # result = trials.trials[len(trials.trials) - 1]["result"]
        # print(result)
        # if result["status"] == "new":
        #     True
        # [{'state': 1, 'tid': 0, 'spec': None, 'result': {'status': 'new'}, 'misc': {'tid': 0, 'cmd': ('domain_attachment', 'FMinIter_Domain'), 'workdir': None, 'idxs': {'colsample_bytree': [0], 'feature_fraction': [0], 'lambda_l1': [0], 'lambda_l2': [0], 'learning_rate': [0], 'max_depth': [0], 'min_child_samples': [0], 'min_child_weight': [0], 'min_split_gain': [0], 'n_estimators': [0], 'num_leaves': [0], 'reg_alpha': [0], 'reg_lambda': [0], 'subsample': [0]}, 'vals': {'colsample_bytree': [0.9665314987474787], 'feature_fraction': [0.9678649804776035], 'lambda_l1': [3.7653366119924554], 'lambda_l2': [1.5167177238937268], 'learning_rate': [0.5681136261981354], 'max_depth': [8.0], 'min_child_samples': [229.0], 'min_child_weight': [9.144044436736317], 'min_split_gain': [0.7566560264858682], 'n_estimators': [962.0], 'num_leaves': [126.0], 'reg_alpha': [0.9424640347539684], 'reg_lambda': [0.5532468831726944], 'subsample': [0.7255240972894323]}}, 'exp_key': None, 'owner': None, 'version': 0, 'book_time': datetime.datetime(2024, 3, 7, 0, 5, 32, 531000), 'refresh_time': datetime.datetime(2024, 3, 7, 0, 5, 32, 531000)}]
        new_loss = trials.trials[len(trials.trials) - 1]["result"]["loss"]
        if best_loss is None:
            return False, [new_loss, iteration_no_progress + 1]

        # Adjusted to use absolute_increase for threshold calculation
        best_loss_threshold = best_loss - absolute_increase
        if new_loss is None or new_loss < best_loss_threshold:
            best_loss = new_loss
            iteration_no_progress = 0
        else:
            iteration_no_progress += 1
            logger.info(
                f"No progress made: {iteration_no_progress} iteration on {iteration_stop_count}. best_loss={best_loss:.2f}, best_loss_threshold={best_loss_threshold:.2f}, new_loss={new_loss:.2f}"
            )

        return (
            iteration_no_progress >= iteration_stop_count,
            [best_loss, iteration_no_progress],
        )

    return stop_fn
