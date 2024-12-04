import pandas as pd
import pyarrow


df = pd.read_parquet('data/results_data/season_stats_2024_with_results.parquet')


high_columns = ['win_pct','mvp_votes_probability', 'td:int', 'passing_tds_pg', 'rushing_tds_pg', 'passing_yards_pg', 'rushing_yards_pg',
           'passing_first_downs_pg','rushing_first_downs_pg', 'pacr']
low_columns = ['total_fumbles_lost_pg', 'sacks_pg', 'sack_yards_pg', 'interceptions_pg']

df[['win_pct', 'td:int', 'pacr']] = df[['win_pct', 'td:int', 'pacr']].round(2)
df[['passing_yards_pg', 'rushing_yards_pg', 'sack_yards_pg']] = df[['passing_yards_pg', 'rushing_yards_pg', 'sack_yards_pg']].round(1)
df[['passing_tds_pg', 'rushing_tds_pg','interceptions_pg', 'sacks_pg','passing_first_downs_pg','rushing_first_downs_pg', 'total_fumbles_lost_pg']] = df[['passing_tds_pg', 'rushing_tds_pg','interceptions_pg', 'sacks_pg','passing_first_downs_pg','rushing_first_downs_pg', 'total_fumbles_lost_pg']].round(1)

for col in high_columns:
    # Create a new column with ranking for each metric
    df[f'rank_{col}'] = df[col].rank(ascending=False, method='min')
for col in low_columns:
    # Create a new column with ranking for each metric
    df[f'rank_{col}'] = df[col].rank(ascending=True, method='min')

df= df[df['player_display_name'].isin(["Josh Allen", "Lamar Jackson", "Jared Goff", "Joe Burrow", "Patrick Mahomes"])]
df['mvp_votes_probability']= (df['mvp_votes_probability'].astype(float)*100).round(1)
df = df.sort_values(by='mvp_votes_probability', ascending=False)

# Prepare a DataFrame with values and their respective ranks in brackets
columns = high_columns + low_columns
rank_columns = [f'rank_{col}' for col in columns]

df_display = df[['player_display_name']].copy()

for col, rank_col in zip(columns, rank_columns):
    df_display[col] = df.apply(lambda row: f"{row[col]}\n({int(row[rank_col])})", axis=1)

# Display the DataFrame
df_display.rename(columns={
    'player_display_name': 'Player',
    'mvp_votes_probability': 'MVP prop',
    'win_pct': 'Win %',
    'td:int': 'TD:INT',
    'passing_tds_pg': 'Pass TD',
    'rushing_tds_pg': 'Rush TD',
    'passing_yards_pg': 'Pass Yrds',
    'rushing_yards_pg': 'Rush Yrds',
    'passing_first_downs_pg': 'Pass 1sts',
    'rushing_first_downs_pg': 'Rush 1sts',
    'pacr': 'PACR',
    'total_fumbles_lost_pg': 'Fumbles Lost',
    'sacks_pg': 'Sacks',
    'sack_yards_pg': 'Sack Yrds',
    'interceptions_pg': 'Ints'
}, inplace=True)

print(df_display.to_markdown(index=False, tablefmt='grid'))