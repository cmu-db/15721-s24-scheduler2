import json
import pandas as pd
import matplotlib.pyplot as plt

# Load the JSON data from the file
with open('path_to_your_file.json', 'r') as f:
    data = json.load(f)

# Convert the data into a pandas DataFrame
df = pd.DataFrame(data)

# Convert timestamp strings to datetime objects
df['submitted_at'] = pd.to_datetime(df['submitted_at'])
df['finished_at'] = pd.to_datetime(df['finished_at'])

# Filter out incomplete entries (where finished_at is not available)
df_complete = df[df['finished_at'].notna()]

# Sort the DataFrame by 'query_id' for plotting
df_complete = df_complete.sort_values(by='query_id')

# Create a figure and axis for the plot
plt.figure(figsize=(14, 8))
for i, row in df_complete.iterrows():
    # Draw a line segment from submitted_at to finished_at for each query
    plt.plot([row['submitted_at'], row['finished_at']], [i, i], marker='o', label=row['sql_string'] if i == 0 else "")

# Set the y-ticks to the SQL string for each query, aligning with sorted order
plt.yticks(range(len(df_complete)), df_complete['sql_string'])

# Invert y-axis to display the first entry on top
plt.gca().invert_yaxis()

# Label the x-axis and y-axis
plt.xlabel('Time')
plt.ylabel('SQL Query')

# Set the title of the plot
plt.title('Timeline of SQL Queries Execution')

# Place the legend outside the plot area
plt.legend(title='Query Legend', bbox_to_anchor=(1.05, 1), loc='upper left')

# Adjust the layout to not cut off labels or legend
plt.tight_layout()

# Display the plot
plt.show()
