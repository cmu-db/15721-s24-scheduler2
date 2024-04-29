import os
import json
import pandas as pd
import matplotlib.pyplot as plt


def load_and_process_json_files(directory):
    data = []
    all_submitted_times = []

    for file_name in sorted(os.listdir(directory), key=lambda x: int(x.split('.')[0])):
        if file_name.endswith('.json'):
            with open(os.path.join(directory, file_name), 'r') as file:
                jobs = json.load(file)
                file_index = int(file_name.split('.')[0])
                for job in jobs:
                    job['file_index'] = file_index
                    job['submitted_at'] = pd.to_datetime(job['submitted_at'])
                    job['finished_at'] = pd.to_datetime(job['finished_at'])
                    all_submitted_times.append(job['submitted_at'])
                    data.append(job)

    return data, all_submitted_times


# Directory containing the JSON files
directory = "./executor_logs"

jobs_data, all_submitted_times = load_and_process_json_files(directory)

df_jobs = pd.DataFrame(jobs_data)

# Normalize timestamps by smallest submitted_at timestamp
min_submitted_at = min(all_submitted_times)
df_jobs['normalized_submitted'] = (df_jobs['submitted_at'] - min_submitted_at).dt.total_seconds()
df_jobs['normalized_finished'] = (df_jobs['finished_at'] - min_submitted_at).dt.total_seconds()

# Plotting
plt.figure(figsize=(14, 8))
file_indices = df_jobs['file_index'].unique()
for index in sorted(file_indices):
    df_subset = df_jobs[df_jobs['file_index'] == index]
    for i, row in df_subset.iterrows():
        plt.plot([row['normalized_submitted'], row['normalized_finished']], [index, index], marker='o')

plt.yticks(sorted(file_indices), labels=[f"{index}" for index in sorted(file_indices)])

plt.gca().invert_yaxis()

plt.xlabel('Time (seconds from first query submission)')
plt.ylabel('File Index')

plt.title('Normalized Timeline of Job Events Execution')
plt.tight_layout()
plt.show()
