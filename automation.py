import csv
from datetime import datetime

def determine_shift(time_str):
    """Determine the shift based on the time."""
    hour = int(time_str.split(':')[0])
    
    if 6 <= hour < 14:
        return "A"
    elif 14 <= hour < 22:
        return "B"
    else:
        return "C"

def format_time(time_str):
    """Extract and format the time part from an ISO 8601 datetime string."""
    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    return dt.strftime('%H:%M:%S')

with open("ankita_output.csv", "w", newline='') as fout:
    writer = csv.writer(fout)
    writer.writerow(["Profile", "Date", "Time", "Shift", "Actual event occurred (region)", "Sensor", "Loss Value", "Start Time of Failure", "End Time of Failure"])

    for key, val in region_dict.items():
        print("****** " + key + " ******")
        bucket = "iba_data_10mm"
        profile = "10mm"
        measurement = key + "_anomaly_score_cobble"
        start_time = "2024-02-15T02:20:00.000Z"
        end_time = "2024-02-15T02:30:00.000Z"
        field = "predicted_cobble"

        query_for_anomaly_time = f'''from(bucket: "{bucket}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        |> filter(fn: (r) => r["_field"] == "{field}")
        |> filter(fn: (r) => r["_value"] == 1)
        '''

        try:
            result = query_api.query(org=org, query=query_for_anomaly_time)
        except Exception as ex:
            print(ex)
        else:
            list_of_predicted_cobble_timestamps = []
            for table in result:
                for record in table.records:
                    list_of_predicted_cobble_timestamps.append(record.get_time())

            measurement = key + "_loss_per_signal"
            list_of_sensors = val["list_of_sensors"]

            # Format start and end times
            start_time_of_failure = format_time(start_time)
            end_time_of_failure = format_time(end_time)

            for timestamp in list_of_predicted_cobble_timestamps:
                print(timestamp)
                date_formatted = timestamp.strftime('%d-%m-%Y')  # Format as dd-mm-yyyy
                time_formatted = timestamp.strftime('%H:%M:%S.000Z')
                shift = determine_shift(timestamp.strftime('%H:%M:%S'))  # Determine the shift based on time
                
                query_for_loss_signals = f'''from(bucket: "{bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["_time"] == {timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                |> group()
                |> sort(columns: ["_value"], desc: true)
                |> limit(n:4)
                |> keep(columns: ["_field", "_value"])
                '''

                result_for_loss_signals = query_api.query(org=org, query=query_for_loss_signals)
                
                first_row = True
                
                for table in result_for_loss_signals:
                    for record in table.records:
                        sensor = record.get_field()
                        loss_value = record.get_value()
                        if first_row:
                            writer.writerow([profile, date_formatted, time_formatted, shift, key, sensor, loss_value, start_time_of_failure, end_time_of_failure])
                            first_row = False
                        else:
                            writer.writerow(["", "", "", "", "", sensor, loss_value, "", ""])  # Skip profile, date, time, shift, region, start time, and end time for subsequent rows
                        print(sensor, loss_value)