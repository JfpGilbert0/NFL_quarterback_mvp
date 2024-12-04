import pandas as pd
import pyarrow

def run_tests():
    """
    Function to run tests on the nfl_player_stats table to verify its integrity.
    """

    print("Running tests on the NFL Player Stats dfbase...\n")
    
    # Load df into a dfFrame
    df = pd.read_parquet("data/cleaned_data/mvp_season_stats_with_binary.parquet")

    # Test 1: Ensure no negative values for essential metrics
    essential_metrics = [
        'completions', 'attempts', 'passing_yards', 'passing_tds',
        'interceptions', 'sacks', 'sack_yards', 'carries', 'rushing_yards',
        'rushing_tds', 'total_fumbles', 'votes'
    ]
    
    for metric in essential_metrics:
        if (df[metric] < 0).any():
            print(f"Test Failed: Column '{metric}' contains negative values.")
        else:
            print(f"Test Passed: Column '{metric}' contains no negative values.")

    # Test 2: Check that no essential fields are null
    essential_fields = [
        'player_id', 'player_display_name', 'season', 'recent_team', 'completions',
        'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks',
        'sack_yards', 'carries', 'rushing_yards', 'rushing_tds', 'total_fumbles',
        'completion_pct', 'win_pct', 'playoff', 'votes'
    ]
    for field in essential_fields:
        if df[field].isnull().any():
            print(f"Test Failed: Column '{field}' contains null values.")
        else:
            print(f"Test Passed: Column '{field}' contains no null values.")
    
    # Test 3: Verify correct types of columns (integers, floats, strings)
    expected_dtypes = {
        'player_id': 'object', 'season': 'int64', 'recent_team': 'object', 'completions': 'int64',
        'attempts': 'int64', 'passing_yards': 'int64', 'passing_tds': 'int64', 'interceptions': 'int64',
        'sacks': 'int64', 'sack_yards': 'int64', 'carries': 'int64', 'rushing_yards': 'int64',
        'rushing_tds': 'int64', 'total_fumbles': 'int64', 'completion_pct': 'float64', 'win_pct': 'float64',
        'playoff': 'int64', 'votes': 'int64'
    }

    for field, expected_dtype in expected_dtypes.items():
        if df[field].dtype != expected_dtype:
            print(f"Test Failed: Column '{field}' has incorrect dtype. Expected: {expected_dtype}, Found: {df[field].dtype}")
        else:
            print(f"Test Passed: Column '{field}' has correct dtype '{expected_dtype}'.")
    
    # Test 4: Check that `win_pct` is between 0 and 1
    if df['win_pct'].between(0, 1).all():
        print("Test Passed: Column 'win_pct' values are between 0 and 1.")
    else:
        print("Test Failed: Column 'win_pct' contains values outside the range [0, 1].")

    # Test 5: Ensure the `td:int` ratio is non-negative
    if (df['td:int'] >= 0).all():
        print("Test Passed: Column 'td:int' has non-negative values.")
    else:
        print("Test Failed: Column 'td:int' contains negative values.")

    # Test 6: Check player seasons to ensure that `season` is a valid integer year
    if df['season'].between(2000, 2024).all():
        print("Test Passed: Column 'season' contains valid years between 2000 and 2024.")
    else:
        print("Test Failed: Column 'season' contains invalid years.")

    # Test 7: Check that `completion_pct` is between 0 and 1
    if df['completion_pct'].between(0, 1).all():
        print("Test Passed: Column 'completion_pct' values are between 0 and 1.")
    else:
        print("Test Failed: Column 'completion_pct' contains values outside the range [0, 1].")

    # Test 8: Ensure that no player has more than 16 games played in a regular season
    if (df['gp'] <= 17).all():
        print("Test Passed: No player has more than 16 games played in a regular season.")
    else:
        print("Test Failed: Some players have more than 16 games played in a regular season.")
    
    columns_85 = ['passing_tds_85pctile', 'rushing_tds_85pctile',
        'passing_yards_85pctile', 'passing_first_downs_85pctile', 'pacr_85pctile', 'rushing_yards_85pctile',
        'rushing_first_downs_85pctile',
        'total_fumbles_lost_85pctile',
        'sacks_85pctile','sack_yards_85pctile', 'interceptions_85pctile']
    columns = [col for col in columns_85 if col.endswith('_85pctile')]
    for column in columns:
        assert df[column].isin([0, 1]).all(), f"Column '{column}' contains values other than 0 or 1."

# Run the tests
if __name__ == "__main__":
    run_tests()

# Close the connection
