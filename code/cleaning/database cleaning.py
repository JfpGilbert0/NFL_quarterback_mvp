import pandas as pd
import pyarrow
import numpy as np

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
# DF currently includs playoff records and non QB records which are not needed for analysis
stats = stats[stats['position'] == 'QB']
stats = stats[stats['season_type']=="REG"]
# gp = game played
stats['gp'] = 1


# Team season records are included to be used as a varable in regresion
team_record = pd.read_parquet("data/raw_data/game_stats/team_stats.parquet")

# Create a column "playoff" initialized to 0
team_record['playoff'] = 0

# Find teams and seasons that have a game_type != 'REG'
playoff_teams = team_record[team_record['game_type'] != 'REG'][['season', 'away_team', 'home_team']]
playoff_teams = (playoff_teams[['season', 'away_team']].rename(columns={'away_team': 'team'})
                          ._append(playoff_teams[['season', 'home_team']].rename(columns={'home_team': 'team'}))
                          .drop_duplicates()
                          .reset_index(drop=True))

team_record = team_record[team_record['game_type'] == "REG"]
# Create a new DataFrame for team records
team_records = []

# Iterate over each game to collect win, loss, draw records for both home and away teams
for _, row in team_record.iterrows():
    season, week = row['season'], row['week']
    home_qb, away_qb = row['home_qb_name'], row['away_qb_name']
    
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
        'qb_name': home_qb,
        'team': home_team,
        'win': win,
        'loss': loss,
        'draw': draw
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
        'qb_name': away_qb,
        'team': away_team,
        'win': win,
        'loss': loss,
        'draw': draw
    })

# Convert team records into a DataFrame
team_records_df = pd.DataFrame(team_records)

print(team_records_df)

stats = stats.merge(team_records_df,
                                    left_on=['season','week', 'player_display_name', 'recent_team'],
                                    right_on=['season', 'week', 'qb_name', 'team'],
                                    how='inner')

print(stats)


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
    'fantasy_points': 'sum','fantasy_points_ppr': 'sum', 'gp': 'sum', 'win': 'sum', 'loss': 'sum', 'draw': 'sum'}


## Group by player_id, player_name, season and aggregate
season_stats = stats.groupby(['player_id', 'player_display_name', 'season', 'recent_team']).agg(aggregate_columns).reset_index()

# Filters are put in place in order to only include qb's who play a significant number of games in a season
# 1989 (joe montana only 10 mvps played less than 14) was the last time a player won MVP playng less than 14 games so this is used as the minimum
season_stats = season_stats[season_stats['gp'] > 13]
# Variable creation:
season_stats['total_fumbles'] = season_stats['receiving_fumbles'] + season_stats['rushing_fumbles'] + season_stats['sack_fumbles']
season_stats['total_fumbles_lost'] = season_stats['receiving_fumbles_lost'] + season_stats['rushing_fumbles_lost'] + season_stats['sack_fumbles_lost']
season_stats['td:int'] = season_stats['passing_tds']/season_stats['interceptions']
season_stats['completion_pct'] = (season_stats['completions']/season_stats['attempts'])
season_stats['win_pct'] = season_stats['win']/season_stats['gp']

print(f"pre merge:{season_stats}")
# creating playoff variable
season_stats['playoff'] = 0
season_stats.loc[season_stats.set_index(['season', 'recent_team']).index.isin(playoff_teams.set_index(['season', 'team']).index), 'playoff'] = 1


season_stats.to_csv("data/cleaned_data/qb_season_stats.csv", index=False)
season_stats.to_parquet("data/cleaned_data/qb_season_stats.parquet", index=False)

# voting data gathered from  https://www.pro-football-reference.com/awards/ap-nfl-mvp-award.htm

mvp_ranks = [
    [2000, 1, 24.0, 'Marshall Faulk'],
    [2000, 2, 11.0, 'Donovan McNabb'],
    [2000, 3, 8.0, 'Eddie George'],
    [2000, 4, 5.0, 'Rich Gannon'],
    [2000, 5, 1.0, 'Ray Lewis'],
    [2000, 6, 1.0, 'Peyton Manning'],
    [2001, 1, 21.5, 'Kurt Warner'],
    [2001, 2, 17.5, 'Marshall Faulk'],
    [2001, 3, 5.0, 'Brett Favre'],
    [2001, 4, 4.0, 'Kordell Stewart'],
    [2001, 5, 2.0, 'Brian Urlacher'],
    [2002, 1, 19.0, 'Rich Gannon'],
    [2002, 2, 15.0, 'Brett Favre'],
    [2002, 3, 11.0, 'Steve McNair'],
    [2002, 4, 1.0, 'Derrick Brooks'],
    [2002, 5, 1.0, 'Priest Holmes'],
    [2002, 6, 1.0, 'Michael Vick'],
    [2003, 1, 16.0, 'Peyton Manning'],
    [2003, 2, 16.0, 'Steve McNair'],
    [2003, 3, 8.0, 'Tom Brady'],
    [2003, 4, 5.0, 'Jamal Lewis'],
    [2003, 5, 3.0, 'Priest Holmes'],
    [2003, 6, 2.0, 'Ray Lewis'],
    [2004, 1, 47.0, 'Peyton Manning'],
    [2004, 2, 1.0, 'Michael Vick'],
    [2005, 1, 19.0, 'Shaun Alexander'],
    [2005, 2, 13.0, 'Peyton Manning'],
    [2005, 3, 10.0, 'Tom Brady'],
    [2005, 4, 6.0, 'Tiki Barber'],
    [2005, 5, 2.0, 'Carson Palmer'],
    [2006, 1, 44.0, 'LaDainian Tomlinson'],
    [2006, 2, 4.0, 'Drew Brees'],
    [2006, 3, 2.0, 'Peyton Manning'],
    [2007, 1, 49.0, 'Tom Brady'],
    [2007, 2, 1.0, 'Brett Favre'],
    [2008, 1, 32.0, 'Peyton Manning'],
    [2008, 2, 4.0, 'Chad Pennington'],
    [2008, 3, 4.0, 'Michael Turner'],
    [2008, 4, 3.0, 'James Harrison'],
    [2008, 5, 3.0, 'Adrian Peterson'],
    [2008, 6, 2.0, 'Philip Rivers'],
    [2008, 7, 1.0, 'Chris Johnson'],
    [2008, 8, 1.0, 'Kurt Warner'],
    [2009, 1, 39.5, 'Peyton Manning'],
    [2009, 2, 7.5, 'Drew Brees'],
    [2009, 3, 2.0, 'Philip Rivers'],
    [2009, 4, 1.0, 'Brett Favre'],
    [2010, 1, 50.0, 'Tom Brady'],
    [2011, 1, 48.0, 'Aaron Rodgers'],
    [2011, 2, 2.0, 'Drew Brees'],
    [2012, 1, 30.5, 'Adrian Peterson'],
    [2012, 2, 19.5, 'Peyton Manning'],
    [2013, 1, 49.0, 'Peyton Manning'],
    [2013, 2, 1.0, 'Tom Brady'],
    [2014, 1, 31.0, 'Aaron Rodgers'],
    [2014, 2, 13.0, 'J.J. Watt'],
    [2014, 3, 2.0, 'DeMarco Murray'],
    [2014, 4, 2.0, 'Tony Romo'],
    [2014, 5, 1.0, 'Tom Brady'],
    [2014, 6, 1.0, 'Bobby Wagner'],
    [2015, 1, 48.0, 'Cam Newton'],
    [2015, 2, 1.0, 'Tom Brady'],
    [2015, 3, 1.0, 'Carson Palmer'],
    [2016, 1, 25.0, 'Matt Ryan'],
    [2016, 2, 10.0, 'Tom Brady'],
    [2016, 3, 6.0, 'Derek Carr'],
    [2016, 4, 6.0, 'Ezekiel Elliott'],
    [2016, 5, 2.0, 'Aaron Rodgers'],
    [2016, 6, 1.0, 'Dak Prescott'],
    [2017, 1, 40.0, 'Tom Brady'],
    [2017, 2, 8.0, 'Todd Gurley'],
    [2017, 3, 2.0, 'Carson Wentz'],
    [2018, 1, 41.0, 'Patrick Mahomes'],
    [2018, 2, 9.0, 'Drew Brees'],
    [2019, 1, 50.0, 'Lamar Jackson'],
    [2020, 1, 44.0, 'Aaron Rodgers'],
    [2020, 2, 4.0, 'Josh Allen'],
    [2020, 3, 2.0, 'Patrick Mahomes'],
    [2021, 1, 39.0, 'Aaron Rodgers'],
    [2021, 2, 10.0, 'Tom Brady'],
    [2021, 3, 1.0, 'Cooper Kupp'],
    [2022, 1, 48.0, 'Patrick Mahomes'],
    [2022, 2, 1.0, 'Jalen Hurts'],
    [2022, 3, 1.0, 'Josh Allen'],
    [2023, 1, 49, 'Lamar Jackson'],
    [2023, 2, 1.0, 'Josh Allen']
    ]

columns = ['season', 'rank', 'votes', 'player']
mvp_df = pd.DataFrame(mvp_ranks, columns=columns)
#print(mvp_df.head())

mvp_season_stats = season_stats.merge(mvp_df[['season', 'player', 'rank', 'votes']], 
                                               left_on=['season', 'player_display_name'], 
                                               right_on=['season', 'player'], 
                                               how='left')

mvp_season_stats['votes'] = mvp_season_stats['votes'].fillna(0)
# Binary values added for mvp winners
mvp_season_stats['if_mvp'] = (mvp_season_stats['votes'] >= 16).astype(int)
mvp_season_stats['if_mvp_votes'] = (mvp_season_stats['votes'] > 0).astype(int)

# Preview before saving
#print(mvp_season_stats.sort_values(by='season', ascending=True))
mvp_season_stats.to_parquet("data/cleaned_data/mvp_season_stats.parquet")




# Adding per game value columns:

per_game_columns = [
    'completion_pct', 'passing_yards', 'passing_tds', 'interceptions', 'passing_air_yards', 'passing_yards_after_catch',
    'sacks', 'total_fumbles', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions',
    'rushing_yards', 'rushing_tds', 'rushing_first_downs', 'rushing_2pt_conversions']

pg_season_stats = mvp_season_stats

for col in per_game_columns:
    if col in pg_season_stats.columns:
        pg_season_stats[f'{col}_pg' ] = pg_season_stats[col] / pg_season_stats['gp']

#print(pg_season_stats.columns.to_list())

pg_season_stats.to_parquet("data/cleaned_data/mvp_season_stats_with_per_game.parquet")


## Create binary columns for each important stat


def add_binary_stats(mvp_season_stats):
    high_stats = ['passing_tds', 'rushing_tds',  'passing_yards', 'passing_first_downs', 'pacr', 'rushing_yards', 'rushing_first_downs']
    low_stats = ['total_fumbles_lost', 'sacks', 'sack_yards','interceptions']
    for stat in high_stats:
        if stat in mvp_season_stats.columns:
            mvp_season_stats[f'{stat}_best'] = mvp_season_stats.groupby('season')[stat].transform(
                lambda x: np.where(x == x.max(), 1, 0))
            mvp_season_stats[f'{stat}_{int(90)}pctile'] = mvp_season_stats.groupby('season')[stat].transform(
                    lambda x: np.where(x >= x.quantile(0.9),1,0))
            for q in [0.5, 0.75]:
                mvp_season_stats[f'{stat}_{int(q*100)}pctile'] = mvp_season_stats.groupby('season')[stat].transform(
                    lambda x: np.where((x >= x.quantile(q)) & (x < x.quantile(q + 0.25)), 1, 0)
                )
            
    for stat in low_stats:
        if stat in mvp_season_stats.columns:
            mvp_season_stats[f'{stat}_best'] = mvp_season_stats.groupby('season')[stat].transform(
                lambda x: np.where(x == x.min(), 1, 0))
            mvp_season_stats[f'{stat}_{int(90)}pctile'] = mvp_season_stats.groupby('season')[stat].transform(
                    lambda x: np.where(x <= x.quantile(0.1),1,0))
            for q in [0.5, 0.25]:
                mvp_season_stats[f'{stat}_{int((1-q)*100)}pctile'] = mvp_season_stats.groupby('season')[stat].transform(
                    lambda x: np.where((x > x.quantile(q-0.25)) & (x <= x.quantile(q)), 1, 0)
                )
    return mvp_season_stats    
mvp_season_stats = add_binary_stats(mvp_season_stats)

#print(mvp_season_stats.columns.to_list())
pg_season_stats.to_parquet("data/cleaned_data/mvp_season_stats_with_binary.parquet")


# get current year stats
stats_2024 = pd.read_parquet("data/raw_data/game_stats/player_game_current.parquet")

stats_2024 = stats_2024[stats_2024['position'] == "QB"]
stats_2024 = stats_2024[stats_2024['season_type']=="REG"]
# gp = game played
stats_2024['gp'] = 1


stats_2024 = stats_2024.merge(team_records_df,
                                    left_on=['season','week', 'player_display_name', 'recent_team'],
                                    right_on=['season', 'week', 'qb_name', 'team'],
                                    how='inner')

#print(stats_2024)


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
    'fantasy_points': 'sum','fantasy_points_ppr': 'sum', 'gp': 'sum', 'win': 'sum', 'loss': 'sum', 'draw': 'sum'}


## Group by player_id, player_name, season and aggregate
season_stats_2024 = stats_2024.groupby(['player_id', 'player_display_name', 'season', 'recent_team']).agg(aggregate_columns).reset_index()
season_stats_2024= season_stats_2024[season_stats_2024['gp']>5]
# Filters are put in place in order to only include qb's who play a significant number of games in a season
# 1989 (joe montana only 10 mvps played less than 14) was the last time a player won MVP playng less than 14 games so this is used as the minimum
# Variable creation:
season_stats_2024['total_fumbles'] = season_stats_2024['receiving_fumbles'] + season_stats_2024['rushing_fumbles'] + season_stats_2024['sack_fumbles']
season_stats_2024['total_fumbles_lost'] = season_stats_2024['receiving_fumbles_lost'] + season_stats_2024['rushing_fumbles_lost'] + season_stats_2024['sack_fumbles_lost']
season_stats_2024['td:int'] = season_stats_2024['passing_tds']/season_stats_2024['interceptions']
season_stats_2024['completion_pct'] = (season_stats_2024['completions']/season_stats_2024['attempts'])
season_stats_2024['win_pct'] = season_stats_2024['win']/season_stats_2024['gp']

season_stats_2024 = add_binary_stats(season_stats_2024)
#print(season_stats_2024.columns.to_list)

season_stats_2024.to_parquet("data/cleaned_data/season_stats_2024.parquet")