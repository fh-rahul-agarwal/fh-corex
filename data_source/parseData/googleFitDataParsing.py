import json
import os
import pandas as pd
import xml.etree.ElementTree as ET

class ParseData:
    
    def nanos_to_datetime(self, nanos):
        """Convert nanoseconds to IST datetime."""
        utc_time = pd.to_datetime(nanos, unit='ns')
        ist_time = utc_time.tz_localize('UTC').tz_convert('Asia/Kolkata')
        return ist_time.strftime('%Y-%m-%d %H:%M:%S')

    def millis_to_datetime(self, millis):
        """Convert milliseconds to IST datetime."""
        utc_time = pd.to_datetime(millis, unit='ms')
        ist_time = utc_time.tz_localize('UTC').tz_convert('Asia/Kolkata')
        return ist_time.strftime('%Y-%m-%d %H:%M:%S')

    def parse_json(self, file_path):
        """Parse a single JSON file and return a DataFrame."""
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
        df = df.dropna(axis=1, how='all')
        return df

    def parse_tcx_file(self, file_path):
        """Parse a single TCX file and return a DataFrame."""
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        namespaces = {
            'tcx': 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2',
            'ns2': 'http://www.garmin.com/xmlschemas/UserProfile/v2',
            'ns3': 'http://www.garmin.com/xmlschemas/ActivityExtension/v2',
            'ns4': 'http://www.garmin.com/xmlschemas/ProfileExtension/v1',
            'ns5': 'http://www.garmin.com/xmlschemas/ActivityGoals/v1'
        }

        parsed_data = []

        for activity in root.findall('tcx:Activities/tcx:Activity', namespaces):
            activity_data = {
                'Sport': activity.get('Sport'),
                'Id': activity.find('tcx:Id', namespaces).text
            }

            for lap in activity.findall('tcx:Lap', namespaces):
                lap_data = {
                    'Lap.StartTime': lap.get('StartTime'),
                    'Lap.DistanceMeters': lap.find('tcx:DistanceMeters', namespaces).text,
                    'Lap.TotalTimeSeconds': lap.find('tcx:TotalTimeSeconds', namespaces).text,
                    'Lap.Calories': lap.find('tcx:Calories', namespaces).text,
                    'Lap.AverageHeartRateBpm': lap.find('tcx:AverageHeartRateBpm/tcx:Value', namespaces).text if lap.find('tcx:AverageHeartRateBpm/tcx:Value', namespaces) is not None else pd.NA,
                    'Lap.MaximumHeartRateBpm': lap.find('tcx:MaximumHeartRateBpm/tcx:Value', namespaces).text if lap.find('tcx:MaximumHeartRateBpm/tcx:Value', namespaces) is not None else pd.NA,
                    'Lap.Intensity': lap.find('tcx:Intensity', namespaces).text,
                    'Lap.TriggerMethod': lap.find('tcx:TriggerMethod', namespaces).text,
                }

                trackpoints = []
                for trackpoint in lap.findall('tcx:Track/tcx:Trackpoint', namespaces):
                    trackpoint_data = {
                        'Lap.Track.Trackpoint.DistanceMeters': trackpoint.find('tcx:DistanceMeters', namespaces).text,
                        'Lap.Track.Trackpoint.Time': trackpoint.find('tcx:Time', namespaces).text,
                        'HeartRateBpm': trackpoint.find('tcx:HeartRateBpm/tcx:Value', namespaces).text if trackpoint.find('tcx:HeartRateBpm/tcx:Value', namespaces) is not None else pd.NA,
                    }
                    trackpoints.append(trackpoint_data)

                for trackpoint in trackpoints:
                    parsed_data.append({**activity_data, **lap_data, **trackpoint})

        df = pd.DataFrame(parsed_data)
        df = df.dropna(axis=1, how='all')
        return df

    def parse_csv(self, file_path):
        """Parse the daily activity CSV file."""
        try:
            df = pd.read_csv(file_path)
            return df
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")
        return pd.DataFrame()

    def allData_json(self, folder_path):
        """Process all JSON files in the folder and return a combined DataFrame."""
        all_dfs = [self.parse_json(os.path.join(folder_path, filename)) 
                   for filename in os.listdir(folder_path) if filename.endswith('.json')]
        combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        return combined_df

    def activities_tcx(self, folder_path):
        """Process all TCX files in the folder and return a combined DataFrame."""
        all_dfs = [self.parse_tcx_file(os.path.join(folder_path, filename)) 
                   for filename in os.listdir(folder_path) if filename.endswith('.tcx')]
        combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        return combined_df

    def daily_activity_metrics(self, file_path):
        """Parse the daily activity metrics CSV and return a DataFrame."""
        return self.parse_csv(file_path)
