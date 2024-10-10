import pandas as pd
from datetime import datetime

from processing.pillars.vitality.dataStream.v_hr_types import *

class VHRagg:
    def __init__(self, googleFit_df, *args):
        self.googleFit_df = googleFit_df
        self.user_name = self.googleFit_df['userName'].iloc[0] if 'userName' in self.googleFit_df.columns else 'UnknownUser'
        self.processor_instance = VHeartRate(self.googleFit_df, *args)
        self.processed_df = self.processor_instance.process()
        self.s_name = 'V_HR'

    def process(self):

        processed_df = self.processed_df

        if not processed_df.empty:
            type_value = processed_df['type'].iloc[0] if 'type' in processed_df.columns else None
            unit_value = processed_df['unit'].iloc[0] if 'unit' in processed_df.columns else None
        else:
            type_value = None
            unit_value = None

        processed_df['startDate'] = pd.to_datetime(processed_df['startDate'], errors='coerce')
        processed_df = processed_df.dropna(subset=['startDate'])
        processed_df['value'] = pd.to_numeric(processed_df['value'], errors='coerce')
        processed_df = processed_df.drop_duplicates(subset='startDate')

        for col in ['activity', 'sleep', 'workout', 'resting']:
            if col not in processed_df.columns:
                processed_df[col] = 0

        daily_agg = processed_df.groupby(processed_df['startDate'].dt.date).agg(
            dayMin=('value', 'min'),
            dayMax=('value', 'max'),
            dayAvg=('value', 'mean'),
            activityMin=('value', lambda x: x[processed_df['activity'] == 1].min() if (processed_df['activity'] == 1).any() else pd.NA),
            activityMax=('value', lambda x: x[processed_df['activity'] == 1].max() if (processed_df['activity'] == 1).any() else pd.NA),
            activityAvg=('value', lambda x: x[processed_df['activity'] == 1].mean() if (processed_df['activity'] == 1).any() else pd.NA),
            sleepMin=('value', lambda x: x[processed_df['sleep'] == 1].min() if (processed_df['sleep'] == 1).any() else pd.NA),
            sleepMax=('value', lambda x: x[processed_df['sleep'] == 1].max() if (processed_df['sleep'] == 1).any() else pd.NA),
            sleepAvg=('value', lambda x: x[processed_df['sleep'] == 1].mean() if (processed_df['sleep'] == 1).any() else pd.NA),
            workoutMin=('value', lambda x: x[processed_df['workout'] == 1].min() if (processed_df['workout'] == 1).any() else pd.NA),
            workoutMax=('value', lambda x: x[processed_df['workout'] == 1].max() if (processed_df['workout'] == 1).any() else pd.NA),
            workoutAvg=('value', lambda x: x[processed_df['workout'] == 1].mean() if (processed_df['workout'] == 1).any() else pd.NA),
            restingMin=('value', lambda x: x[processed_df['resting'] == 1].min() if (processed_df['resting'] == 1).any() else pd.NA),
            restingMax=('value', lambda x: x[processed_df['resting'] == 1].max() if (processed_df['resting'] == 1).any() else pd.NA),
            restingAvg=('value', lambda x: x[processed_df['resting'] == 1].mean() if (processed_df['resting'] == 1).any() else pd.NA),
        ).reset_index()

        daily_agg = daily_agg.fillna({'activityMin': pd.NA, 'activityMax': pd.NA, 'activityAvg': pd.NA,
                                      'sleepMin': pd.NA, 'sleepMax': pd.NA, 'sleepAvg': pd.NA,
                                      'workoutMin': pd.NA, 'workoutMax': pd.NA, 'workoutAvg': pd.NA,
                                      'restingMin': pd.NA, 'restingMax': pd.NA, 'restingAvg': pd.NA})

        long_format = daily_agg.melt(id_vars=['startDate'], 
                                     value_vars=['dayAvg', 'dayMin', 'dayMax',
                                                 'activityAvg', 'activityMin', 'activityMax', 
                                                 'sleepAvg', 'sleepMin', 'sleepMax',
                                                 'workoutAvg', 'workoutMin', 'workoutMax',
                                                 'restingAvg', 'restingMin', 'restingMax'],
                                     var_name='valueType', 
                                     value_name='value')

        long_format['type'] = type_value
        long_format['unit'] = unit_value
        long_format['date'] = long_format['startDate']

        final_output = long_format[['date', 'type', 'unit', 'valueType', 'value']]
        final_output['userName'] = self.user_name
        final_output['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_output['value'] = pd.to_numeric(final_output['value']).copy()
        final_output['value'] = final_output['value'].round(1).copy()
        final_output['s_name'] = self.s_name
        final_output = final_output.sort_values(by=['date', 'type'], ascending=False, ignore_index=True)

        columns_order = ['userName', 'valueGeneratedAt', 's_name'] + [col for col in final_output.columns if col not in ['userName', 'valueGeneratedAt', 's_name']]
        final_output = final_output[columns_order]

        return final_output
