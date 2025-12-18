# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Alexander Steiger

# predict_smc.py

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model

def predict_smc_next_day(gdf: pd.DataFrame, rain_df: pd.DataFrame, model_path: str) -> pd.DataFrame:
    """
    Use a trained Neural Network to predict soil moisture content (SMC) for the coming day.
    
    Parameters
    ----------
    gdf : GeoDataFrame or DataFrame
        Must contain columns ['device_id', 'smc', 'time'].
    rain_df : DataFrame
        Must contain columns ['date','rain','temp','humidity','uvi','wind_speed'].
    model_path : str
        Path to the saved Keras model (.h5).
    
    Returns
    -------
    gdf : DataFrame
        Same as input gdf but with a new column 'smc_pred' containing predicted values.
    """

    # --- Prepare the ML dataframe ---
    ml_df = gdf[['device_id','smc','time']].copy()
    ml_df['date'] = pd.to_datetime(ml_df['time'].dt.date)
    ml_df = ml_df.drop('time', axis=1)

    ml_df = ml_df.groupby(['date','device_id'])['smc'].mean().reset_index()
    ml_df['date'] = ml_df['date'].max()
    ml_df = ml_df.sort_values(by='device_id', ascending=True)

    # Remove unwanted devices
    strings_to_delete = ['ru-lse-06', 'ru-lse-12', 'ru-lse-30']
    ml_df = ml_df[~ml_df['device_id'].isin(strings_to_delete)]

    ml_device_ids = ml_df['device_id'].reset_index(drop=True)
    step = len(ml_df)

    # Add next day with empty SMC
    ml_df2 = ml_df.copy()
    ml_df2['date'] = ml_df2['date'] + pd.Timedelta(days=1)
    ml_df2['smc'] = 0
    ml_df = pd.concat([ml_df, ml_df2], ignore_index=True)

    # Merge with weather data
    ml_df = ml_df.merge(rain_df[['date','rain','temp','humidity','uvi','wind_speed']],
                        on='date', how='left')

    if ml_df.isna().any().any():
        print("Attention!!! There are NaN values in ml_df.")

    # Compute mean/std excluding zeros
    mean = ml_df[['smc','temp','humidity']].replace(0, np.nan).mean().values
    std = ml_df[['smc','temp','humidity']].replace(0, np.nan).std().values

    # Prepare feature array
    ml_df = ml_df[['smc','temp','humidity']]
    ml_df.iloc[step:, 0] = np.nan  # blank the smc for prediction rows

    custom_scaler = StandardScaler()
    custom_scaler.mean_ = mean
    custom_scaler.scale_ = std
    ml_array = custom_scaler.transform(ml_df)

    # Load trained model
    loaded_model = load_model(model_path)
    print("Loaded model:", loaded_model.name if hasattr(loaded_model, "name") else "Keras Model")
    print("Input Shape:", loaded_model.layers[0].input_shape)

    # Predict step-by-step for each device for the next day
    for k in range(step, len(ml_array)):
        data_x = np.array([ml_array[k-step:k, 0:ml_array.shape[1]]])
        prediction = loaded_model.predict(data_x, verbose=0)
        ml_array[k, 0] = prediction

    # Inverse transform predictions
    pred_df = custom_scaler.inverse_transform(ml_array)
    pred_df = pd.DataFrame(pred_df).iloc[step:step*2, :]
    pred_df = pred_df.reset_index(drop=True)
    pred_df = pred_df.rename(columns={0: 'smc'})

    # Combine predictions with device IDs
    pred_df = pd.concat([ml_device_ids, pred_df['smc']], axis=1)

    # Merge into gdf
    gdf = gdf.merge(pred_df, on='device_id', how='left', suffixes=('', '_pred'))
    gdf['smc_pred'] = gdf['smc_pred'].fillna(gdf['smc'])

    return gdf

