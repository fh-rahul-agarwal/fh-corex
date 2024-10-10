import pandas as pd
from datetime import datetime, timedelta

class VTotalCalories:
    def __init__(self, googleFit_df, *args):
        self.records_df = googleFit_df[googleFit_df["dataSource"] == 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended'].copy()
        self.unit = 'kcal'
        self.value_generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_records_df = self._filter_by_dates_list(self.records_df, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_records_df = self._filter_by_single_date(self.records_df, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_records_df = self._filter_by_date_range(self.records_df, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.filtered_records_df = self._filter_by_offset(self.records_df, self.start_date, self.days_offset, self.offset_sign)
        
        if not self.filtered_records_df.empty:
            self.flagged_records_df = self._flag_records()
        else:
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
        print(f'No data available for the given input.')

    def _flag_records(self):
        if self.filtered_records_df.empty:
            return self.filtered_records_df
        
        self.filtered_records_df = self._initialize_flags(self.filtered_records_df)
        self._flag_active_calories()
        self._flag_resting_calories()

        return self.filtered_records_df.reset_index(drop=True)

    def _initialize_flags(self, df):
        for col in ['activeCalories', 'restingCalories']:
            if col not in df.columns:
                df[col] = 0
        return df

    def _flag_active_calories(self):
        non_google_sources = self.filtered_records_df['originDataSourceId'] != 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended'
        self.filtered_records_df.loc[non_google_sources, 'activeCalories'] = 1

    def _flag_resting_calories(self):
        google_sources = self.filtered_records_df['originDataSourceId'] == 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended'
        self.filtered_records_df.loc[google_sources & (self.filtered_records_df['activeCalories'] == 0), 'restingCalories'] = 1

    def process(self):
        calories_df = self.filtered_records_df.copy()
        if calories_df.empty:
            return calories_df[['userName', 'valueGeneratedAt', 'type', 'originDataSourceId', 'dataSource', 'modifiedTime', 'startDate', 'endDate', 'unit', 'value', 'activeCalories', 'restingCalories']].reset_index(drop=True)
        
        calories_df['unit'] = self.unit
        calories_df['valueGeneratedAt'] = self.value_generated_at
        calories_df['value'] = calories_df['value'].astype(float)
        calories_df['startDate'] = pd.to_datetime(calories_df['startDate'])
        calories_df['dateSorting'] = calories_df['startDate'].dt.date
        #calories_df = calories_df.sort_values(by=['dateSorting', 'startDate', 'type'], ascending=(False, True, False), ignore_index=True)

        return calories_df[['userName', 'valueGeneratedAt', 'type', 'originDataSourceId', 'dataSource', 'modifiedTime', 'startDate', 'endDate', 'unit', 'value', 'activeCalories', 'restingCalories']].reset_index(drop=True)
