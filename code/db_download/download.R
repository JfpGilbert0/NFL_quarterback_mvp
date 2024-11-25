# Install nflverse if not already installed
if (!requireNamespace("nflverse", quietly = TRUE)) {
  install.packages("nflverse")
}
library(nflverse)
if (!requireNamespace("arrow", quietly = TRUE)) {
  install.packages("arrow")
}
library(arrow)
if (!requireNamespace("dplyr", quietly = TRUE)) {
  install.packages("dplyr")
}
library(dplyr)

# Download player statistics for all available seasons
## must be broken up into decades so all data is collected
## 2000's
player_stats_2000 <- load_player_stats(seasons = 2000:2009)
write_parquet(player_stats_2000, "data/raw_data/game_stats/player_game_00s.parquet") # nolint: line_length_linter.
## 2010's
player_stats_2010 <- load_player_stats(seasons = 2010:2019)
write_parquet(player_stats_2010, "data/raw_data/game_stats/player_game_10s.parquet") # nolint: line_length_linter.
## 2020-2023
player_stats_2020 <- load_player_stats(seasons = 2020:2023)
write_parquet(player_stats_2020, "data/raw_data/game_stats/player_game_20s.parquet") # nolint: line_length_linter.

## Also downloading player game data for players in the current season
player_stats_2024 <- load_player_stats(seasons = 2024)
write_parquet(player_stats_2024, "data/raw_data/game_stats/player_game_current.parquet") # nolint: line_length_linter.

# Download team stats for each season

#download qbr stats 2006-2024
qbr_to_2023 <- load_espn_qbr(league = "nfl", seasons = 2006:2023)
write_parquet(qbr_to_2023, "data/raw_data/advanced_stats/qbr_06_23.parquet") # nolint: line_length_linter.

qbr_2024 <- load_espn_qbr(league = "nfl", seasons = 2024)
write_parquet(qbr_2024, "data/raw_data/advanced_stats/qbr_2024.parquet")

nextgen_16_24 <- load_nextgen_stats(seasons = 2016:2024)
write_parquet(nextgen_16_24, "data/raw_data/advanced_stats/next_gen.parquet")

# pbp data

for (year in 2000:2024) {
  tryCatch({
    # Download play-by-play data for the year
    pbp_data <- load_pbp(year)
    # Define the output file name
    output_file <- file.path("data/raw_data/pbp", paste0("playbyplay_", year, ".parquet"))
    # Save the data as a Parquet file
    write_parquet(pbp_data, output_file)
    # Message indicating success
    message("Successfully saved play-by-play data for ", year, " to ", output_file)
  }, error = function(e) {
    # Handle any errors
    message("Error downloading or saving data for ", year, ": ", e$message)
  })
}

teams <- load_schedules(seasons = 2000:2023)
teams <- teams %>%
  filter(game_type == "REG")
write_parquet(teams, "data/raw_data/game_stats/team_stats.parquet")
