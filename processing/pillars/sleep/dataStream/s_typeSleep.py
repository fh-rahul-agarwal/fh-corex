import pandas as pd
from datetime import datetime, timedelta

class SSleepType:

    SLEEP_STAGE_MAPPING = {
        1: 'Awake',
        2: 'Sleep',
        3: 'OutOfBed',
        4: 'LightSleep',
        5: 'DeepSleep',
        6: 'REMSleep'
    }
    
    def __init__(self, googleFit_df, *args):
        self.records_df = googleFit_df[googleFit_df['dataSource'] == 'derived:com.google.sleep.segment:com.google.android.gms:merged'].copy()
        
        if self.records_df.empty:
            self._handle_empty_records()
            return
        
        # Handle date filtering based on the arguments
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.records_df = self._filter_by_dates_list(self.records_df, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.records_df = self._filter_by_single_date(self.records_df, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.records_df = self._filter_by_date_range(self.records_df, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.records_df = self._filter_by_offset(self.records_df, self.start_date, self.days_offset, self.offset_sign)

        # Handle case where no data is found after filtering
        if self.records_df.empty:
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
        df.loc[:, 'modifiedTime'] = pd.to_datetime(df['modifiedTime']).dt.tz_localize(None)
        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999) if end_date else start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)
        return df[(df['modifiedTime'] >= start_of_day) & (df['modifiedTime'] <= end_of_day)].reset_index(drop=True)

    def _handle_empty_records(self):
        print("No Sleep data available for the specified dates.")
    
    def _calculate_duration(self, row):
        start = pd.to_datetime(row['startDate'])
        end = pd.to_datetime(row['endDate'])
        return round((end - start).total_seconds() / 60, 1)  # Convert to minutes

    def _map_sleep_stage(self, value):
        return self.SLEEP_STAGE_MAPPING.get(value, 'Unknown')

    def _format_output(self):
        self.records_df['value'] = self.records_df['value'].apply(self._map_sleep_stage)
        self.records_df['duration'] = self.records_df.apply(self._calculate_duration, axis=1)
        self.records_df['unit'] = 'min'
        self.records_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return self.records_df[[ 
            'userName', 'valueGeneratedAt', 'type', 'originDataSourceId', 'dataSource', 
            'modifiedTime', 'startDate', 'endDate', 'value', 'unit', 'duration'
        ]]

    def process(self):
        result_df = self._format_output().copy()
        result_df['dateSorting'] = pd.to_datetime(result_df['modifiedTime'], errors='coerce').dt.date
        return result_df.sort_values(by=['dateSorting', 'type'], ascending=False, ignore_index=True)[[
            'userName', 'valueGeneratedAt', 'type', 'originDataSourceId', 'dataSource', 'value', 
            'modifiedTime', 'startDate', 'endDate', 'unit', 'duration'
        ]]
