import pandas as pd
import pyarrow
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import xgboost as xgb

# Load the dataset
df = pd.read_parquet("data/cleaned_data/mvp_season_stats_with_binary.parquet")
print(df.columns.to_list())

# Check the basic statistics for target and features
print(df['if_mvp_votes'].describe())
print(df['win_pct'].describe())

# Check the seasons available in the dataset
print(df['season'].value_counts())

# Check additional statistics for key features
print(df['completion_pct'].describe())
print(df['passing_tds'].describe())
print(df['interceptions'].describe())

# Define predictor variables (X) and target variable (y)
vars = ['win_pct','td:int', 'passing_tds_90pctile', 'rushing_tds_90pctile',
        'passing_yards_90pctile', 'passing_first_downs_90pctile', 'pacr_90pctile', 'rushing_yards_90pctile',
        'rushing_first_downs_90pctile',
        'total_fumbles_lost_90pctile',
        'sacks_90pctile','sack_yards_90pctile', 'interceptions_90pctile']
# Print the correlation matrix of the variables
correlation_matrix = df[vars].corr()
plt.figure(figsize=(15, 10))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
plt.title('Correlation Matrix of Variables')
#plt.show()


X = df[vars]
y = df['if_mvp_votes']

# Standardize the predictor variables
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=23)

# Fit the XGBoost model
xgb_model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=23)
xgb_model.fit(X_train, y_train)

# Show the feature importances for the variables
feature_importances = pd.DataFrame({'Variable': vars, 'Importance': xgb_model.feature_importances_})
print("Feature importances for the XGBoost model:\n", feature_importances)

# Predict on the test set
y_pred = xgb_model.predict(X_test)

# Evaluate the model
print("Accuracy Score:", accuracy_score(y_test, y_pred))
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
print("Classification Report:\n", classification_report(y_test, y_pred))

# Load the 2024 season data for prediction
df_2024 = pd.read_parquet("data/cleaned_data/season_stats_2024.parquet")
X_current = df_2024[vars]
X_current_scaled = scaler.transform(X_current)

# Predict MVP likelihood for 2024 season
predictions = xgb_model.predict_proba(X_current_scaled)[:,1]
print("MVP Predictions for 2024:\n", predictions)

# Add predictions to the 2024 dataframe
df_2024['mvp_prediction'] = predictions
print(df_2024[['player_display_name','mvp_prediction']])

# Save the predictions to a new file
df_2024.to_parquet("data/cleaned_data/season_stats_2024_with_xg_predictions.parquet", index=False)
