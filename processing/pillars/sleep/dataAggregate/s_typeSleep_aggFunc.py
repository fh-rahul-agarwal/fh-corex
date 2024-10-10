import pandas as pd
from processing.pillars.sleep.dataStream.s_typeSleep import SSleepType

class SSleepTypeAgg:
    def __init__(self, google_fit_df, *args):
        self.records_df = google_fit_df
        self.processor = SSleepType(self.records_df, *args)
        self.sleep_data_processor = self.processor.process()
        self.type = self.sleep_data_processor['type'].iloc[0]
        self.s_name = 'S_SleepType'

    def process(self):
        # Process the records using SSleepType to get the relevant data for the *args
        sleep_data_processor = self.sleep_data_processor

        # Ensure that the 'date' column is in date format
        sleep_data_processor.loc[:, 'date'] = pd.to_datetime(sleep_data_processor['modifiedTime']).dt.date

        # Aggregate the durations for each sleep type
        agg_df = sleep_data_processor.groupby(
            ['userName', 'valueGeneratedAt', 'originDataSourceId', 'dataSource', 'date', 'unit', 'value'],
            as_index=False
        )['duration'].sum()

        # Identify the relevant types for total sleep calculation
        total_sleep_types = ['LightSleep', 'DeepSleep', 'REMSleep']

        # Calculate the total sleep duration
        total_sleep_df = sleep_data_processor[sleep_data_processor['value'].isin(total_sleep_types)]
        total_sleep_duration = total_sleep_df.groupby(
            ['userName', 'valueGeneratedAt', 'originDataSourceId', 'dataSource', 'date', 'unit'],
            as_index=False
        )['duration'].sum()

        total_sleep_duration['valueType'] = 'TotalSleepDuration'
        total_sleep_duration['type'] = self.type
        total_sleep_duration.loc[:, 'value'] = total_sleep_duration['duration']

        # Create mapping for the different sleep types and value types
        value_type_mapping = {
            'LightSleep': 'TotalLightSleepDuration',
            'DeepSleep': 'TotalDeepSleepDuration',
            'REMSleep': 'TotalREMSleepDuration',
            'Awake': 'TotalAwakeDuration'
        }

        agg_df.loc[:, 'valueType'] = agg_df['value'].map(value_type_mapping)
        agg_df.loc[:, 'value'] = agg_df['duration']
        agg_df.loc[:, 'type'] = self.type

        # Concatenate the aggregated DataFrame with the total sleep duration DataFrame
        final_df = pd.concat([agg_df, total_sleep_duration], ignore_index=True)

        # Sort the DataFrame by date and type in descending order
        final_df = final_df.sort_values(by=['date', 'type', 'value'], ascending=False, ignore_index=True)
        final_df.loc[:, 's_name'] = self.s_name

        return final_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
