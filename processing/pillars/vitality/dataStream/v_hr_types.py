import pandas as pd
from datetime import datetime, timedelta

class VHeartRate:
    def __init__(self, googleFit_df, *args):
        self.records_df = googleFit_df.copy()
        self.unit = 'bpm'
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
        if df.empty:
            return pd.DataFrame(columns=['dataTypeName', 'originDataSourceId', 'data_source', 'startDate', 
                                         'endDate', 'value_type', 'fit_value'])

        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()
        else:
            end_of_day = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()

        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        print('No data available for the given input.')

    def _flag_records(self):
        if self.filtered_records_df.empty:
            return self.filtered_records_df
        
        self.filtered_records_df = self._initialize_flags(self.filtered_records_df)
        self._flag_sleep_records()
        self._flag_workout_records() 
        self._flag_activity_records()
        self._flag_resting_records()

        return self.filtered_records_df.reset_index(drop=True)

    def _initialize_flags(self, df):
        for col in ['activity', 'sleep', 'resting', 'workout']:
            if col not in df.columns:
                df[col] = 0
        return df

    def _flag_sleep_records(self):
        sleep_values = ['derived:com.google.sleep.segment:com.google.android.gms:merged']
        for sleep_type in sleep_values:
            sleep_df = self.filtered_records_df[self.filtered_records_df['data_source'] == sleep_type].copy()
            for _, sleep_row in sleep_df.iterrows():
                sleep_start = sleep_row['startDate']
                sleep_end = sleep_row['endDate']
                overlap_mask = (self.filtered_records_df['startDate'] <= sleep_end) & (self.filtered_records_df['endDate'] >= sleep_start)
                self.filtered_records_df.loc[overlap_mask, 'sleep'] = 1

    def _flag_workout_records(self):
        workout_types = [
            'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended'
        ]    
        for workout_type in workout_types:
            workout_df = self.filtered_records_df[(self.filtered_records_df['data_source'] == workout_type) & (self.filtered_records_df['originDataSourceId'] != 
                        'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended')].copy()
            for _, workout_row in workout_df.iterrows():
                workout_start = workout_row['startDate']
                workout_end = workout_row['endDate']
                overlap_mask = (self.filtered_records_df['startDate'] <= workout_end) & (self.filtered_records_df['endDate'] >= workout_start)
                self.filtered_records_df.loc[(overlap_mask) & (self.filtered_records_df['sleep'] == 0), 'workout'] = 1    

    def _flag_activity_records(self):
        activity_types = [
            'derived:com.google.active_minutes:com.google.android.gms:merge_active_minutes',
            'derived:com.google.step_count.delta:com.google.android.gms:estimated_steps'
        ]
        for activity_type in activity_types:
            activity_df = self.filtered_records_df[self.filtered_records_df['data_source'] == activity_type].copy()
            for _, activity_row in activity_df.iterrows():
                activity_start = activity_row['startDate']
                activity_end = activity_row['endDate']
                overlap_mask = (self.filtered_records_df['startDate'] <= activity_end) & (self.filtered_records_df['endDate'] >= activity_start)
                self.filtered_records_df.loc[(overlap_mask) & 
                                             (self.filtered_records_df['sleep'] == 0) & 
                                             (self.filtered_records_df['workout'] == 0), 'activity'] = 1

    def _flag_resting_records(self):
        self.filtered_records_df['resting'] = (
            (self.filtered_records_df['activity'] == 0) & 
            (self.filtered_records_df['workout'] == 0) & 
            (self.filtered_records_df['sleep'] == 0)
        ).astype(int)

    def process(self):
        heart_rate_df = self.filtered_records_df[self.filtered_records_df['data_source'] == 'derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm'].copy()
        if heart_rate_df.empty:
            return heart_rate_df
        
        heart_rate_df['unit'] = self.unit
        heart_rate_df['valueGeneratedAt'] = self.value_generated_at
        heart_rate_df['fit_value'] = heart_rate_df['fit_value'].astype(float)
        heart_rate_df['startDate'] = pd.to_datetime(heart_rate_df['startDate'])
        heart_rate_df['dateSorting'] = heart_rate_df['startDate'].dt.date
        heart_rate_df = heart_rate_df.sort_values(by=['dateSorting', 'startDate', 'dataTypeName'], ascending=(False, True, False), ignore_index=True)
        
        return heart_rate_df[['userName', 'valueGeneratedAt', 'dataTypeName', 'originDataSourceId', 'data_source', 
                               'modifiedTime', 'startDate', 'endDate', 'unit', 'fit_value', 
                               'sleep', 'activity', 'workout', 'resting']].reset_index(drop=True)
