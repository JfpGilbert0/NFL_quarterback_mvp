# Predicting This Year’s NFL MVP: Insights from Historical Data

**Author**: Jacob Gilbert  
**Date**: December 02, 2024

## Overview

This repository contains the analysis, data, and code related to the research paper titled **"Predicting This Year’s NFL MVP: Insights from Historical Data"**. The project aims to predict the NFL MVP of the current 2024 season using historical data, focusing specifically on quarterbacks and their performance metrics over the years. Leveraging the powerful machine learning model XGBoost, the paper explores which player metrics have the strongest influence on MVP selection, considering aspects like passing yards, touchdowns, team success, and efficiency ratios.

**Access the Full Paper**: [Link to Paper in PDF Format](https://github.com/JfpGilbert0/NFL\papers\Predicting_NFL_MVP.pdf)

## Abstract

The NFL MVP is the most prestigious individual award in American football, highlighting a player's excellence both individually and as a key contributor to their team. This research utilizes historical data from the NFL to predict the likely MVP for the 2024 season, focusing on statistical performance metrics for quarterbacks. By utilizing an XGBoost classifier model, we aim to understand which specific player statistics correlate most strongly with MVP candidacy. The findings show that metrics such as team win percentage, passing touchdowns, and efficiency ratios are significant determinants. This approach helps stakeholders understand what makes an MVP quarterback, providing insights not only into individual player performance but also the broader dynamics that influence MVP voting.

## Repository Structure

The repository consists of the following files and folders:

- **`data/`**: Contains the cleaned dataset in Parquet format, which was used for model training and analysis.
  - `cleaned_data`: Includes data that has gone through cleaning and filtering through database cleaning file in `code`
    - `mvp_season_stats_with_binary.parquet`: Dataset with key metrics used to train and evaluate the model.
  - `result_data`: includes final results from the prediction and regression model. 
  
- **`code/`**: Includes all Python scripts used in the project.
  - `data_cleaning.py`: Code used to clean and preprocess the data.
  - `xg_boost.py`: Script for training the XGBoost model and generating predictions.
  - `fig_x.py`: Contains code to generate the figures used in the paper.

- **`Predicting_NFL_MVP.qmd`**: Quarto document that contains the complete analysis, including all code, tables, and LaTeX mathematical representations.
  
- **`Predicting_NFL_MVP.pdf`**: The compiled paper with analysis and findings.

- **`references.bib`**: BibTeX file containing the references used in the paper.

- **`requirements.txt`**: Lists all required Python packages for the project.

- **`README.md`**: This file, providing an overview of the project.

## Key Features

1. **Data Collection and Cleaning**:
   - Data sourced from NFLverse package in R, supplemented by other publicly available sources.
   - Focus on NFL player performance data since 2006, concentrating on key metrics for quarterbacks.

2. **Modeling Approach**:
   - XGBoost Classifier is used to predict MVP voting outcomes, capturing complex relationships between player statistics.
   - Training data is split into training (90%) and testing (10%) sets to evaluate model performance on unseen data.

3. **Results and Evaluation**:
   - **Accuracy Score**: The model achieved an accuracy score of 97% in predicting which quarterbacks are likely to receive MVP votes.
   - Feature importance analysis reveals that metrics such as win percentage, passing touchdowns, and efficiency are critical factors.
   - Three quarterbacks are expected to receive MVP votes in 2024, with Josh Allen leading the conversation.

4. **Appendix**:
   - A detailed exploration of survey methodologies, sampling, and observational data considerations for understanding subjective MVP candidacy factors.

## Setup Instructions

### Prerequisites

- **Python**: Version 3.7 or above
- **R**: For data collection with the `nflverse` package.
- **Python Libraries**: Install required libraries by running the following command:


  pip install -r requirements.txt
Ensure requirements.txt contains:

- pandas
- pyarrow
- numpy
- seaborn
- matplotlib
- scikit-learn
- xgboost
- jupyter

### Running the Analysis
#### Clone the repository:

`git clone https://github.com/JfpGilbert0/NFL.git`
`cd NFL`

#### Data Preprocessing:
Rload the raw data by running  code\db_download\download.R

Run the data cleaning script located in code/python/data_cleaning.py to ensure the dataset is preprocessed properly.
#### Model Training:

Use the code/python/xg_boost.py script to train the XGBoost model.
Generating Results:

To generate figures and visualizations, run code/python/fig_{1/5}.py
Quarto Document:

To view the paper or update it, modify Predicting_NFL_MVP.qmd and recompile using Quarto.
bash
Copy code
quarto render Predicting_NFL_MVP.qmd
Results Summary
The XGBoost model identified team win percentage as the most significant predictor of MVP candidacy, followed by passing touchdowns and efficiency metrics. The analysis also highlighted that while individual performance is crucial, team success remains a major determinant. In the ongoing 2024 NFL season, the model predicts that Josh Allen, Jared Goff, and Lamar Jackson are most likely to receive MVP votes, with Allen standing out as the leading candidate.

## Future Directions
Survey and Sentiment Analysis: The next step could involve incorporating fan and media perception data to understand how subjective factors influence MVP voting.
More Robust Models: Exploring other machine learning models, such as Random Forests or Bayesian networks, to compare prediction performance.
Contributing
Contributions to this project are welcome. If you have any suggestions or would like to contribute, please fork the repository and submit a pull request.

## License
This project is licensed under the MIT License.

## Contact
For questions, suggestions, or further discussions:

Jacob Gilbert: [GitHub Profile](https://github.com/JfpGilbert0)
