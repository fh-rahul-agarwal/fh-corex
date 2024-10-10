import pandas as pd
from processing.pillars.activity.dataStream.a_stepCount import *

class AStepCountAgg:
    def __init__(self, googleFit_df, *args):
        self.googleFit_df = googleFit_df
        self.processor = AStepCount(self.googleFit_df, *args)
        self.step_count_df = self.processor.process()
        self.type = self.step_count_df['type'].iloc[0]
        self.valueType = 'TotalStepCount'
        self.s_name = 'A_StepCount'

    def process(self):
        # Check for duplicate columns and rows
        self.step_count_df = self.step_count_df.loc[:, ~self.step_count_df.columns.duplicated()]
        self.step_count_df = self.step_count_df.drop_duplicates()

        # Convert 'value' column to numeric and date columns to datetime
        self.step_count_df['value'] = pd.to_numeric(self.step_count_df['value'], errors='coerce')
        self.step_count_df['startDate'] = pd.to_datetime(self.step_count_df['startDate'], errors='coerce').dt.date
        self.step_count_df['endDate'] = pd.to_datetime(self.step_count_df['endDate'], errors='coerce').dt.date

        # Set the date column
        self.step_count_df['date'] = self.step_count_df['startDate']

        # Aggregate data by userName, date, startDate, endDate, and unit
        self.step_count_df = self.step_count_df.groupby(['userName', 'date', 'startDate', 'endDate', 'unit']).agg({'value': 'sum'}).reset_index()

        # Keep rows with the maximum value for each date
        self.step_count_df = self.step_count_df.loc[self.step_count_df.groupby('date')['value'].idxmax()]

        # Add additional columns and metadata
        self.step_count_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.step_count_df['value'] = self.step_count_df['value'].round(1)
        self.step_count_df['type'] = self.type
        self.step_count_df['valueType'] = self.valueType
        self.step_count_df['s_name'] = self.s_name

        # Reorder columns and sort by date
        self.step_count_df = self.step_count_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
        self.step_count_df = self.step_count_df.sort_values(by=['date'], ascending=False).reset_index(drop=True)

        return self.step_count_df
