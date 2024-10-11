import pandas as pd
from datetime import datetime, timedelta
import pytz

class WDuration:

    def __init__(self, googleFit_activitiesData, *args):
        self.googleFit_activitiesData = googleFit_activitiesData
        self.value_generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.unit = 'min'
        self.timezone = pytz.timezone('Asia/Kolkata')

        # Convert columns to datetime
        self.googleFit_activitiesData["Lap.StartTime"] = pd.to_datetime(self.googleFit_activitiesData["Lap.StartTime"])
        self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"] = pd.to_datetime(self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"])

        # Convert to UTC if not already timezone aware
        if self.googleFit_activitiesData["Lap.StartTime"].dt.tz is None:
            self.googleFit_activitiesData["Lap.StartTime"] = self.googleFit_activitiesData["Lap.StartTime"].dt.tz_localize('UTC')

        if self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"].dt.tz is None:
            self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"] = self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"].dt.tz_localize('UTC')

        # Convert to the desired timezone
        self.googleFit_activitiesData["Lap.StartTime"] = self.googleFit_activitiesData["Lap.StartTime"].dt.tz_convert(self.timezone)
        self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"] = self.googleFit_activitiesData["Lap.Track.Trackpoint.Time"].dt.tz_convert(self.timezone)

        # Filter data based on arguments
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_googleFit_activitiesData = self._filter_by_dates_list(self.googleFit_activitiesData, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_googleFit_activitiesData = self._filter_by_single_date(self.googleFit_activitiesData, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_googleFit_activitiesData = self._filter_by_date_range(self.googleFit_activitiesData, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.filtered_googleFit_activitiesData = self._filter_by_offset(self.googleFit_activitiesData, self.start_date, self.days_offset, self.offset_sign)

        if self.filtered_googleFit_activitiesData.empty:
            self._handle_empty_records()

    def _filter_by_single_date(self, df, start_date):
        return self._filter_data(df, start_date)

    def _filter_by_date_range(self, df, start_date, end_date):
        return self._filter_data(df, start_date, end_date)

    def _filter_by_offset(self, df, start_date, days_offset, offset_sign):
        if offset_sign == '+':
            end_date = start_date + timedelta(days=days_offset)
        elif offset_sign == '-':
            end_date = start_date
            start_date = start_date - timedelta(days=days_offset)
        return self._filter_data(df, start_date, end_date)

    def _filter_by_dates_list(self, df, dates_list):
        filtered_df = pd.concat([self._filter_data(df, date) for date in dates_list])
        return filtered_df.drop_duplicates().reset_index(drop=True)

    def _filter_data(self, df, start_date, end_date=None):
        if df.empty:
            return pd.DataFrame(columns=['userName', 'Sport', 'Lap.StartTime', 'Lap.Track.Trackpoint.Time', 'duration'])
        
        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999) if end_date else start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)

        df["Id"] = pd.to_datetime(df["Id"]).dt.tz_localize(None)
        filtered_df = df[(df['Id'] >= start_of_day) & (df['Id'] <= end_of_day)]
        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        print("No workout data available for the specified dates.")

    def process(self):

        # Convert Lap.TotalTimeSeconds and Lap.DistanceMeters to numeric
        self.filtered_googleFit_activitiesData["Lap.TotalTimeSeconds"] = pd.to_numeric(self.filtered_googleFit_activitiesData["Lap.TotalTimeSeconds"], errors='coerce')
        self.filtered_googleFit_activitiesData["Lap.DistanceMeters"] = pd.to_numeric(self.filtered_googleFit_activitiesData["Lap.DistanceMeters"], errors='coerce')

        # Calculate duration in minutes and distance in kilometers
        self.filtered_googleFit_activitiesData["duration"] = self.filtered_googleFit_activitiesData["Lap.TotalTimeSeconds"] / 60

        # Round duration and distance
        self.filtered_googleFit_activitiesData["duration"] = self.filtered_googleFit_activitiesData["duration"].round(3)
        self.filtered_googleFit_activitiesData["distance"] = self.filtered_googleFit_activitiesData["Lap.DistanceMeters"].round(3)


        if self.filtered_googleFit_activitiesData.empty:
            print("No data available for processing.")
            return pd.DataFrame(columns=[
                'userName', 'valueGeneratedAt', 'Sport', 'Lap.StartTime', 'Lap.Track.Trackpoint.Time', 'unit', 'duration', 'distance'
            ])

        self.filtered_googleFit_activitiesData['valueGeneratedAt'] = self.value_generated_at
        self.filtered_googleFit_activitiesData['unit'] = self.unit

        # Create startDate and endDate based on grouping by Lap.StartTime
        grouped_data = self.filtered_googleFit_activitiesData.groupby('Lap.StartTime').agg(
            startDate=('Lap.Track.Trackpoint.Time', 'min'),
            endDate=('Lap.Track.Trackpoint.Time', 'max'),
            duration=('duration', 'first'),  # This should now work
            userName=('userName', 'first'),  # Assuming userName is the same within each group
            Sport=('Sport', 'first'),  # Assuming Sport is the same within each group
            valueGeneratedAt=('valueGeneratedAt', 'first'),
            unit=('unit', 'first'),
            distance=('distance', 'first'),
        ).reset_index()

        # Reorder columns to include Sport before Lap.StartTime
        final_googleFit_activitiesData = grouped_data[['userName', 'valueGeneratedAt', 'Sport', 'Lap.StartTime', 'startDate', 'endDate', 'unit', 'duration', 'distance']]

        # Round value to one decimal place
        final_googleFit_activitiesData['duration'] = final_googleFit_activitiesData['duration'].round(1)

        return final_googleFit_activitiesData

