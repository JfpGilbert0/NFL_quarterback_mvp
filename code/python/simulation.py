import pandas as pd
import numpy as np
import random

# Define constants
SEASONS = list(range(2006, 2024))
PLAYERS = ["Tom Brady", "Peyton Manning", "Aaron Rodgers", "Drew Brees", "Patrick Mahomes", "Ben Roethlisberger", "Matt Ryan", "Eli Manning", "Philip Rivers", "Russell Wilson", "Cam Newton", "Kirk Cousins", "Jared Goff", "Josh Allen", "Lamar Jackson", "Brett Favre", "Joe Burrow", "Justin Herbert", "Tua Tagovailoa", "Dak Prescott"]

# Generate the dataset
def generate_dataset(num_records=1000):
    data = []

    for _ in range(num_records):
        season = random.choice(SEASONS)
        player = random.choice(PLAYERS)
        if_mvp_votes = random.randint(0, 1)  # Binary
        votes = random.randint(0, 32)  # Assuming a max of 32 votes

        # Random win percentage between 0 and 1
        win_pct = round(np.random.uniform(0, 1), 3)

        # Random td:int ratio between 0.1 and 10
        td_int = round(np.random.uniform(0.1, 10), 2)

        data.append({
            'season': season,
            'player': player,
            'if_mvp_votes': if_mvp_votes,
            'votes': votes,
            'win_pct': win_pct,
            'td:int': td_int,
            'passing_tds_85pctile': 0,
            'rushing_tds_85pctile': 0,
            'passing_yards_85pctile': 0,
            'passing_first_downs_85pctile': 0,
            'pacr_85pctile': 0,
            'rushing_yards_85pctile': 0,
            'rushing_first_downs_85pctile': 0,
            'total_fumbles_lost_85pctile': 0,
            'sacks_85pctile': 0,
            'sack_yards_85pctile': 0,
            'interceptions_85pctile': 0
        })

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Assign approximately 15% values as 1 for each '_85pctile' column for each season
    columns_85pctile = [
        'passing_tds_85pctile', 'rushing_tds_85pctile', 'passing_yards_85pctile', 
        'passing_first_downs_85pctile', 'pacr_85pctile', 'rushing_yards_85pctile',
        'rushing_first_downs_85pctile', 'total_fumbles_lost_85pctile', 'sacks_85pctile',
        'sack_yards_85pctile', 'interceptions_85pctile'
    ]

    for season in SEASONS:
        season_data = df[df['season'] == season]
        num_players = len(season_data)

        for column in columns_85pctile:
            num_to_set = int(num_players * 0.15)
            if num_to_set > 0:
                indices_to_set = season_data.sample(n=num_to_set).index
                df.loc[indices_to_set, column] = 1

    return df

# Generate the dataset and save to a CSV file
df_simulated = generate_dataset(1000)
df_simulated.to_parquet('data/results_data/simulated_mvp_data.parquet', index=False)

# Display the first few rows of the generated dataset
print(df_simulated.head())