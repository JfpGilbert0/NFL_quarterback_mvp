import pandas as pd
import jupyter
from prettytable import PrettyTable

df = pd.read_parquet("data/cleaned_data/mvp_season_stats_with_binary.parquet")

# Columns of interest
columns = [
    'passing_yards', 'passing_tds','passing_first_downs',
    'interceptions', 'pacr', 'sacks', 'sack_yards', 'total_fumbles_lost',
    'rushing_yards', 'rushing_tds', 'rushing_first_downs', 'win_pct','td:int'
]

# Initialize lists to store results
percentile_85th_values = []
mean_list = []
std_list = []
max_mean_list = []
max_std_list = []

# Calculate the 85th percentile, mean, standard deviation, and max mean for each column
for col in columns:
    # Calculate the 85th percentile value for the column across all seasons
    value_85th = df.groupby('season')[col].quantile(0.85)
    mean_list.append(value_85th.mean())
    std_list.append(value_85th.std())
    percentile_85th_value = df[col].quantile(0.85)
    
    max_value = df.groupby('season')[col].max()
    max_mean_list.append(max_value.mean())
    max_std_list.append(max_value.std())

print(percentile_85th_values)
print(mean_list)
print(std_list)
print(max_mean_list)
print(max_std_list)

summary_df = pd.DataFrame({
    'Statistic': columns,
    '85th Pct Mean': mean_list,
    '85th Pct std': std_list,
    'Max Value Mean': max_mean_list,
    'Max Value std': max_std_list
})

# Round the values

print(summary_df.to_markdown(index=False, tablefmt='pipe'))