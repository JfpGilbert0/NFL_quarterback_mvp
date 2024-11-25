import pandas as pd
import pyarrow

import pandas as pd

# SEASON STATS
## gathering season stats for each player in each year (that is a qb)


stats = []
for year in ["00s", "10s", "20s"]:
    file_path = f"data/raw_data/game_stats/player_game_{year}.parquet"
    try:
        df = pd.read_parquet(file_path)
        df = df
        stats.append(df)
    except FileNotFoundError:
        # Skip years where the file does not exist
        continue
stats = pd.concat(stats, ignore_index=True)
print(stats.columns.tolist())
stats = stats[stats['position'] == 'QB']
stats = stats[stats['season_type']=="REG"]
stats['gp'] = 1





aggregate_columns = {
    'completions': 'sum', 'attempts': 'sum', 'passing_yards': 'sum', 'passing_tds': 'sum', 'interceptions': 'sum',
    'sacks': 'sum', 'sack_yards': 'sum', 'sack_fumbles': 'sum', 'sack_fumbles_lost': 'sum', 'passing_air_yards': 'sum',
    'passing_yards_after_catch': 'sum', 'passing_first_downs': 'sum', 'passing_epa': 'sum', 'passing_2pt_conversions': 'sum',
    'pacr': 'mean', 'dakota': 'mean', 'carries': 'sum', 'rushing_yards': 'sum', 'rushing_tds': 'sum','rushing_fumbles': 'sum',
    'rushing_fumbles_lost': 'sum','rushing_first_downs': 'sum','rushing_epa': 'sum', 'rushing_2pt_conversions': 'sum',
    'receptions': 'sum', 'targets': 'sum', 'receiving_yards': 'sum', 'receiving_tds': 'sum', 'receiving_fumbles': 'sum',
    'receiving_fumbles_lost': 'sum', 'receiving_air_yards': 'sum', 'receiving_yards_after_catch': 'sum', 'receiving_first_downs': 'sum',
    'receiving_epa': 'sum', 'receiving_2pt_conversions': 'sum', 'racr': 'mean', 'target_share': 'mean',
    'air_yards_share': 'mean', 'wopr': 'mean', 'special_teams_tds': 'sum',
    'fantasy_points': 'sum','fantasy_points_ppr': 'sum', 'gp': 'sum'}


## Group by player_id, player_name, season and aggregate
season_stats = stats.groupby(['player_id', 'player_display_name', 'season', 'recent_team']).agg(aggregate_columns).reset_index()

# # Filters are put in place n order to only nclude qb's who play a significant number of games in a season
season_stats['total_fumbles'] = season_stats['receiving_fumbles'] + season_stats['rushing_fumbles'] + season_stats['sack_fumbles']
season_stats['total_fumbles'] = season_stats['receiving_fumbles_lost'] + season_stats['rushing_fumbles_lost'] + season_stats['sack_fumbles_lost']
season_stats = season_stats[season_stats['gp'] > 13]
season_stats = season_stats[season_stats['attempts'] > 50]
season_stats['completion_pct'] = (season_stats['completions']/season_stats['attempts'])




season_stats.to_csv("data/cleaned_data/qb_season_stats.csv", index=False)
season_stats.to_parquet("data/cleaned_data/qb_season_stats.parquet", index=False)

per_game_columns = [
    'completion_pct', 'passing_yards', 'passing_tds', 'interceptions', 'passing_air_yards', 'passing_yards_after_catch',
    'sacks', 'total_fumbles', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions',
    'rushing_yards', 'rushing_tds', 'rushing_first_downs', 'rushing_2pt_conversions']

for col in per_game_columns:
    if col in season_stats.columns:
        season_stats[f'{col}_pg' ] = season_stats[col] / season_stats['gp']

print(season_stats.columns.to_list())


season_stats.to_parquet("data/cleaned_data/season_stats_with_per_game.parquet")


# team season records are included to be used as a varable in regresion
team_record = pd.read_parquet("data/cleaned_data/season_records.parquet")

# team season records are included to be used as a varable in regresion
team_record = pd.read_parquet("data/raw_data/game_stats/team_stats.parquet")

# Create a new DataFrame for team records
team_records = []

# Iterate over each game to collect win, loss, draw records for both home and away teams
for _, row in team_record.iterrows():
    season, week = row['season'], row['week']
    
    # Home team record
    home_team = row['home_team']
    home_score = row['home_score']
    away_score = row['away_score']
    
    if home_score > away_score:
        win, loss, draw = 1, 0, 0
    elif home_score < away_score:
        win, loss, draw = 0, 1, 0
    else:
        win, loss, draw = 0, 0, 1
        
    team_records.append({
        'season': season,
        'week': week,
        'team': home_team,
        'win': win,
        'loss': loss,
        'draw': draw,
    })
    
    # Away team record
    away_team = row['away_team']
    if away_score > home_score:
        win, loss, draw = 1, 0, 0
    elif away_score < home_score:
        win, loss, draw = 0, 1, 0
    else:
        win, loss, draw = 0, 0, 1
        
    team_records.append({
        'season': season,
        'week': week,
        'team': away_team,
        'win': win,
        'loss': loss,
        'draw': draw,
        'gp': 1
    })

# Convert team records into a DataFrame
team_records_df = pd.DataFrame(team_records)

print(team_records_df)






mvp_season_stats = season_stats.merge(team_record,
                                               left_on=['season', 'recent_team'],
                                               right_on=['season','team'],
                                               how='inner')

mvp_season_stats.to_parquet("data/cleaned_data/qb_season_stats_wl.parquet")

## Create binary columns for each important stat (per game)



df =  ['mvp_win', 'tds', 'ints', 'wins', 'total_ypg', 'pass_ypg', 'rush_ypg', 'complton ']