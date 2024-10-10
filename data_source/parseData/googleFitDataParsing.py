import json
import pandas as pd
import os

class ParseData:
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def nanos_to_datetime(self, nanos):
        utc_time = pd.to_datetime(nanos, unit='ns')
        ist_time = utc_time.tz_localize('UTC').tz_convert('Asia/Kolkata')
        return ist_time.strftime('%Y-%m-%d %H:%M:%S')

    def millis_to_datetime(self, millis):
        utc_time = pd.to_datetime(millis, unit='ms')
        ist_time = utc_time.tz_localize('UTC').tz_convert('Asia/Kolkata')
        return ist_time.strftime('%Y-%m-%d %H:%M:%S')

    def parse_json(self, file_path):
        """Parse a single JSON file and return it as a DataFrame."""
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        data_source = data.get('Data Source', '')
        data_points = data.get('Data Points', [])
        
        parsed_data = []
        for point in data_points:
            parsed_row = {}
            start_time_nanos = point.get('startTimeNanos')
            end_time_nanos = point.get('endTimeNanos')
            modified_time_millis = point.get('modifiedTimeMillis')

            parsed_row['startDate'] = self.nanos_to_datetime(start_time_nanos) if start_time_nanos else None
            parsed_row['endDate'] = self.nanos_to_datetime(end_time_nanos) if end_time_nanos else None
            parsed_row['modifiedTime'] = self.millis_to_datetime(modified_time_millis) if modified_time_millis else None
            
            for key, value in point.items():
                if key not in ['fitValue', 'modifiedTimeMillis']:  
                    parsed_row[key] = value

            if 'fitValue' in point and point['fitValue']:
                fit_value_entry = point['fitValue'][0].get('value', None)
                if fit_value_entry:
                    for value_type, value in fit_value_entry.items():
                        parsed_row['fit_value_type'] = value_type
                        parsed_row['fit_value'] = value
                else:
                    parsed_row['fit_value_type'] = None
                    parsed_row['fit_value'] = None
            else:
                parsed_row['fit_value_type'] = None
                parsed_row['fit_value'] = None
            
            parsed_row['data_source'] = data_source
            parsed_data.append(parsed_row)
        
        df = pd.DataFrame(parsed_data)
        return df

    def parse_folder(self):
        all_dfs = []
        
        for filename in os.listdir(self.folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(self.folder_path, filename)
                df = self.parse_json(file_path)
                df = df.dropna(axis=1, how='all') 
                all_dfs.append(df)

        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False) if all_dfs else pd.DataFrame()
        
        del all_dfs
        return combined_df

    def process(self):
        combined_df = self.parse_folder()

        columnName_mapping_table = {
            'dataTypeName': 'type',
            'data_source': 'dataSource',
            'fit_value': 'value',
            'fit_value_type' : 'fitValueType'
        }

        combined_df.rename(columns=columnName_mapping_table, inplace=True)
        
        
        return combined_df[['type', 'originDataSourceId', 'dataSource', 'modifiedTime',  'startDate', 'endDate', 'fitValueType', 'value']]
