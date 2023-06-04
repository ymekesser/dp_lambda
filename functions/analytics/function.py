import boto3
import pandas as pd
import numpy as np
import os
import posixpath
import io
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import sklearn.metrics as metrics


def lambda_handler(event, context):
    print("Preparing data")
    feature_set = read_dataframe(
        join_path(
            os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
            os.environ["ANALYTICS_FILE_FEATURE_SET"],
        )
    )

    X = feature_set[
        [
            "storey_median",
            "floor_area_sqm",
            "lease_commence_date",
            "remaining_lease_in_months",
            "distance_to_closest_mrt",
            "distance_to_closest_mall",
            "distance_to_cbd",
        ]
    ].to_numpy()
    y = feature_set[["resale_price"]].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, random_state=1337, test_size=0.25
    )

    print("Fitting model")
    pipe = Pipeline([("scaler", StandardScaler()), ("linreg", RandomForestRegressor())])

    pipe.fit(X_train, y_train.ravel())

    write_pickle(
        pipe, join_path(os.environ["LOCATION_ANALYTICS"], os.environ["MODEL_FILE"])
    )


def _calculate_metrics(
    model: Pipeline,
    X_train,
    y_train,
    X_test,
    y_test,
) -> pd.DataFrame:
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    df_metrics = pd.DataFrame()

    df_metrics["r2_train"] = [metrics.r2_score(y_train, y_train_pred)]  # type: ignore
    df_metrics["r2_test"] = [metrics.r2_score(y_test, y_test_pred)]  # type: ignore
    df_metrics["MAE"] = [metrics.mean_absolute_error(y_test, y_test_pred)]  # type: ignore
    df_metrics["MSE"] = [metrics.mean_squared_error(y_test, y_test_pred)]  # type: ignore
    df_metrics["RMSE"] = [np.sqrt(metrics.mean_squared_error(y_test, y_test_pred))]  # type: ignore
    mape = np.mean(np.abs((y_test - y_test_pred) / np.abs(y_test)))
    df_metrics["MAPE"] = [round(mape * 100, 2)]
    df_metrics["Accuracy"] = [round(100 * (1 - mape), 2)]

    return df_metrics


def _print_metrics(df_metrics: pd.DataFrame):
    formatted_metrics = f"""Result Metrics:
R^2 Score on Training:{df_metrics["r2_train"]}
R^2 Score on Test:{df_metrics["r2_test"]}

Mean Absolute Error (MAE): {df_metrics["MAE"]}
Mean Squared Error (MSE): {df_metrics["MSE"]}
Root Mean Squared Error (RMSE): {df_metrics["RMSE"]}

Mean Absolute Percentage Error (MAPE): {df_metrics["MAPE"]}
Accuracy: {df_metrics["Accuracy"]}
    """

    print(formatted_metrics)


def join_path(*paths) -> str:
    return posixpath.join(*[str(path) for path in paths])


def read_dataframe(src_path: str) -> pd.DataFrame:
    print(f"Read dataframe from {src_path}")

    s3 = boto3.resource("s3")
    obj = s3.Object(os.environ["S3_BUCKET"], src_path)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))

    return df


def write_pickle(obj: any, dst_path: str) -> None:
    print(f"Writing pickled object to {dst_path}")

    bytes = pickle.dumps(obj)

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=bytes)
