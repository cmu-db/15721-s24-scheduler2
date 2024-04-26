import json
import pandas as pd
import matplotlib.pyplot as plt

with open('job_summary.json', 'r') as f:
    data = json.load(f)

df = pd.DataFrame(data)

df['query_id'] = df['query_id'].astype(int)

df['submitted_at'] = pd.to_datetime(df['submitted_at'])
df['finished_at'] = pd.to_datetime(df['finished_at'])

df_complete = df[df['finished_at'].notna()]

df_complete = df_complete.sort_values(by='query_id').reset_index(drop=True)

print(df_complete)

# Normalize timestamps by subtracting the earliest submitted_at time
min_submitted_at = df_complete['submitted_at'].min()
df_complete['submitted_at'] = (df_complete['submitted_at'] - min_submitted_at).dt.total_seconds()
df_complete['finished_at'] = (df_complete['finished_at'] - min_submitted_at).dt.total_seconds()

plt.figure(figsize=(14, 8))
for i, row in df_complete.iterrows():
    # Draw a line segment from normalized submitted_at to finished_at for each query
    plt.plot([row['submitted_at'], row['finished_at']], [i, i], marker='o', label=f'Query {row["query_id"]}' if i == 0 else "")

plt.yticks(range(len(df_complete)), [f'Query {id}' for id in df_complete['query_id']])

# Invert y-axis to display the first entry on top
plt.gca().invert_yaxis()

plt.xlabel('Time (seconds from first query submission)')
plt.ylabel('Query ID')

plt.title('Normalized Timeline of SQL Queries Execution')


plt.tight_layout()
plt.show()
