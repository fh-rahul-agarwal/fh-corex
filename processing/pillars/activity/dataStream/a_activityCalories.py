import pandas as pd
from datetime import datetime, timedelta

class AActivityCalories:
    def __init__(self, googleFit_df, *args):
        # Filter for step count records using the specified identifier
        self.records_df = googleFit_df[(googleFit_df['data_source'] == 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended') & (googleFit_df["originDataSourceId"] != 'derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended')].copy()
        self.records_df['fit_value'] = pd.to_numeric(self.records_df['fit_value'], errors='coerce')
        self.records_df.dropna(subset=['fit_value'], inplace=True)
        self.value_generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.unit = 'kcal'

        # Initialize filtered_records_df based on input arguments
        if len(args) == 1 and isinstance(args[0], list):
            dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_records_df = self._filter_by_dates_list(dates_list)
        elif len(args) == 1:
            start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_records_df = self._filter_by_single_date(start_date)
        elif len(args) == 2:
            start_date, end_date = pd.to_datetime(args[0]).tz_localize(None), pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_records_df = self._filter_by_date_range(start_date, end_date)
        elif len(args) == 3:
            start_date, days_offset, offset_sign = pd.to_datetime(args[0]).tz_localize(None), int(args[1]), args[2]
            self.filtered_records_df = self._filter_by_offset(start_date, days_offset, offset_sign)

        # Convert value from meters to kilometers
        if self.filtered_records_df.empty:
            self.filtered_records_df = self._handle_empty_records()

    def _filter_by_single_date(self, start_date):
        return self._filter_data(start_date)

    def _filter_by_date_range(self, start_date, end_date):
        return self._filter_data(start_date, end_date)

    def _filter_by_offset(self, start_date, days_offset, offset_sign):
        if offset_sign == '+':
            end_date = start_date + timedelta(days=days_offset)
        else:
            end_date = start_date
            start_date = start_date - timedelta(days=days_offset)
        return self._filter_data(start_date, end_date)

    def _filter_by_dates_list(self, dates_list):
        filtered_df = pd.concat([self._filter_data(date) for date in dates_list]).drop_duplicates().reset_index(drop=True)
        return filtered_df

    def _filter_data(self, start_date, end_date=None):
        if self.records_df.empty:
            return pd.DataFrame(columns=self.records_df.columns)

        df = self.records_df.copy()
        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)]
        else:
            end_of_day = start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)]

        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        return pd.DataFrame(columns=['userName', 'valueGeneratedAt', 'dataTypeName', 'originDataSourceId', 'data_source', 'modifiedTime', 'startDate', 
                                     'endDate', 'unit', 'fit_value'])

    def process(self):
        filtered_by_type = self.filtered_records_df
        final_df = filtered_by_type.copy()
        final_df['valueGeneratedAt'] = self.value_generated_at
        final_df['unit'] = self.unit
        final_df['dateSorting'] = pd.to_datetime(final_df['startDate']).dt.date
        final_df = final_df.sort_values(by=['dateSorting', 'dataTypeName'], ascending=False, ignore_index=True)

        return final_df[['userName', 'valueGeneratedAt', 'dataTypeName', 'originDataSourceId', 'data_source', 'modifiedTime', 'startDate', 
                        'endDate', 'unit', 'fit_value']].copy()
