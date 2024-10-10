import pandas as pd
from processing.pillars.activity.dataStream.a_walkingRunningDistance import *

class AWalkingRunningDistanceAgg:
    def __init__(self, googleFit_df, *args):
        self.googleFit_df = googleFit_df
        self.processor = AWalkingRunningDistance(self.googleFit_df, *args)
        self.walking_running_distance_df = self.processor.process()
        self.type = self.walking_running_distance_df['type'].iloc[0]
        self.valueType = 'TotalWalkingRunningDistance'
        self.s_name = 'A_WalkingRunningDistance'

    def process(self):
        # Check for duplicate columns and rows
        self.walking_running_distance_df = self.walking_running_distance_df.loc[:, ~self.walking_running_distance_df.columns.duplicated()]
        self.walking_running_distance_df = self.walking_running_distance_df.drop_duplicates()

        # Convert 'value' column to numeric and date columns to datetime
        self.walking_running_distance_df['value'] = pd.to_numeric(self.walking_running_distance_df['value'], errors='coerce')
        self.walking_running_distance_df['startDate'] = pd.to_datetime(self.walking_running_distance_df['startDate'], errors='coerce').dt.date
        self.walking_running_distance_df['endDate'] = pd.to_datetime(self.walking_running_distance_df['endDate'], errors='coerce').dt.date

        # Set 'date' column to 'startDate' and group by relevant columns
        self.walking_running_distance_df['date'] = self.walking_running_distance_df['startDate']
        self.walking_running_distance_df = self.walking_running_distance_df.groupby(['userName', 'date', 'startDate', 'endDate', 'unit']).agg({'value': 'sum'}).reset_index()

        # Keep rows with the maximum value for each date
        self.walking_running_distance_df = self.walking_running_distance_df.loc[self.walking_running_distance_df.groupby('date')['value'].idxmax()]

        # Add additional columns and metadata
        self.walking_running_distance_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.walking_running_distance_df['value'] = self.walking_running_distance_df['value'].round(1)
        self.walking_running_distance_df['type'] = self.type
        self.walking_running_distance_df['valueType'] = self.valueType
        self.walking_running_distance_df['s_name'] = self.s_name

        # Reorder columns and sort by date
        self.walking_running_distance_df = self.walking_running_distance_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
        self.walking_running_distance_df = self.walking_running_distance_df.sort_values(by=['date'], ascending=False).reset_index(drop=True)

        return self.walking_running_distance_df
