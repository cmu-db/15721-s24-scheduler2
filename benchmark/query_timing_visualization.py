import json
import pandas as pd
import matplotlib.pyplot as plt

with open('path_to_your_file.json', 'r') as f:
    data = json.load(f)

df = pd.DataFrame(data)
df['submitted_at'] = pd.to_datetime(df['submitted_at'])
df['finished_at'] = pd.to_datetime(df['finished_at'])

df_complete = df[df['finished_at'].notna()]

plt.figure(figsize=(14, 8))
for i, row in df_complete.iterrows():
    plt.plot([row['submitted_at'], row['finished_at']], [i, i], marker='o', label=row['sql_string'] if i == 0 else "")

plt.yticks(range(len(df_complete)), df_complete['sql_string'])
plt.gca().invert_yaxis()  # Invert y axis so that the first entry is on top
plt.xlabel('Time')
plt.ylabel('SQL Query')
plt.title('Timeline of SQL Queries Execution')
plt.legend(title='Query Legend', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
