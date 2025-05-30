import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import StandardScaler

# Load the CSV file from the URL
url = "https://raw.githubusercontent.com/sytong12/SEEM3650project/refs/heads/main/aggregated_hourly_data.csv"
try:
    df = pd.read_csv(url)
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit()

# Convert 'Date' to datetime and extract weekday
df['Date'] = pd.to_datetime(df['Date'])
df['Weekday'] = df['Date'].dt.day_name()

# Aggregate data by road, date, hour, and weekday
road_grouped = df.groupby(['Road', 'Date', 'Hour', 'Weekday']).agg({
    'Average_Speed': 'mean',
    'Average_Occupancy': 'mean',
    'Total_Volume': 'sum'
}).reset_index()

# Function to prepare data for a target road
def prepare_data_for_road(target_road):
    # Extract target road data as the target (Y)
    target_data = road_grouped[road_grouped['Road'] == target_road].copy()
    
    target_data['traffic_jam'] = target_data['Average_Speed'] <= 50
    
    # Extract other roads data as features (X)
    nearby_roads = road_grouped[road_grouped['Road'] != target_road]
    
    # Pivot nearby roads data into wide format
    nearby_features = nearby_roads.pivot_table(
        index=['Date', 'Hour', 'Weekday'],
        columns='Road',
        values=['Average_Speed', 'Average_Occupancy', 'Total_Volume'],
        aggfunc='first'
    ).reset_index()
    
    # Flatten the MultiIndex columns
    def flatten_columns(col):
        if isinstance(col, tuple):
            if col[0] in ['Date', 'Hour', 'Weekday'] and col[1] == '':
                return col[0]
            return '_'.join(col).strip()
        return col
    
    nearby_features.columns = [flatten_columns(col) for col in nearby_features.columns]
    
    # Merge target road data with nearby road features
    target_data = target_data[['Date', 'Hour', 'Weekday', 'traffic_jam']]
    data = pd.merge(nearby_features, target_data, on=['Date', 'Hour', 'Weekday'], how='inner')
    
    # One-hot encode Weekday
    weekday_dummies = pd.get_dummies(data['Weekday'], prefix='is')
    data = pd.concat([data, weekday_dummies], axis=1)
    
    # Prepare features (X) and target (Y), keeping 'Hour' and weekday dummies in X
    X = data.drop(columns=['Date', 'Weekday', 'traffic_jam'])  # Keep 'Hour' and weekday dummies
    Y = data['traffic_jam']
    
    # Handle missing values by filling with column means
    X = X.fillna(X.mean())
    
    return X, Y, data, X.columns.tolist()

# Train and evaluate model for a target road
def train_and_evaluate_model(X, Y, target_road):
    # Scale the features to improve convergence
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    X_train, X_test, Y_train, Y_test = train_test_split(X_scaled, Y, test_size=0.2, random_state=123)
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, Y_train)
    
    # Make predictions and evaluate
    Y_pred = model.predict(X_test)
    accuracy = accuracy_score(Y_test, Y_pred)
    report = classification_report(Y_test, Y_pred)
    
    # Print simplified results
    print(f"\nResults for {target_road}:")
    print(f"Accuracy: {accuracy:.4f}")
    print("Classification Report:")
    print(report)
    
    # Output logistic regression equation
    print(f"Logistic Regression Equation for {target_road}:")
    equation = f"log(P(traffic_jam) / (1 - P(traffic_jam))) = {model.intercept_[0]:.4f}"
    for coef, feature in zip(model.coef_[0], X.columns):
        equation += f" + ({coef:.4f}) * {feature}"
    print(equation)
    
    return model, X.columns.tolist(), scaler

# User input for prediction
def predict_new_data(model, feature_names, target_road, scaler):
    print(f"\nEnter new data for predicting traffic jam on {target_road}:")
    input_data = []
    
    # Prompt for weekday
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    while True:
        weekday = input("Enter weekday (Monday, Tuesday, ..., Sunday): ").strip().capitalize()
        if weekday in weekdays:
            break
        print("Invalid weekday. Please enter a valid day (Monday, Tuesday, ..., Sunday).")
    
    # Create weekday dummy variables
    weekday_dummies = [1 if w == weekday else 0 for w in weekdays]
    
    # Prompt for other features
    for feature in feature_names:
        if feature.startswith('is_'):  # Skip weekday dummies, handled above
            continue
        while True:
            try:
                value = float(input(f"Enter value for {feature}: "))
                if feature == 'Hour' and (value < 0 or value > 23):
                    print("Hour must be between 0 and 23.")
                    continue
                input_data.append(value)
                break
            except ValueError:
                print("Please enter a valid number.")
    
    # Append weekday dummies to input data
    for i, feature in enumerate(feature_names):
        if feature.startswith('is_'):
            input_data.append(weekday_dummies[weekdays.index(feature.replace('is_', ''))])
    
    # Convert input to numpy array, scale, and predict
    input_array = np.array([input_data])
    input_scaled = scaler.transform(input_array)
    prediction = model.predict(input_scaled)[0]
    probability = model.predict_proba(input_scaled)[0]
    
    # Print prediction
    print(f"\nPrediction for {target_road}:")
    print("Traffic Jam" if prediction else "No Traffic Jam")
    print(f"Probability of Traffic Jam: {probability[1]:.4f}")
    print(f"Probability of No Traffic Jam: {probability[0]:.4f}")

# Process both directions
roads = ['Kwun Tong Road Westbound', 'Kwun Tong Road Eastbound']
models = {}
feature_names_dict = {}
scalers = {}

for road in roads:
    X, Y, data, feature_columns = prepare_data_for_road(road)
    if X is not None and Y is not None:
        model, feature_names, scaler = train_and_evaluate_model(X, Y, road)
        models[road] = model
        feature_names_dict[road] = feature_names
        scalers[road] = scaler
    else:
        print(f"Skipping {road} due to data issues.")

# Allow user to predict with new data
while True:
    print("\nWould you like to predict traffic jam for new data? (yes/no)")
    choice = input().strip().lower()
    if choice != 'yes':
        break
    
    print("\nSelect road direction:")
    print("1. Kwun Tong Road Westbound")
    print("2. Kwun Tong Road Eastbound")
    road_choice = input("Enter 1 or 2: ").strip()
    
    if road_choice == '1':
        road = 'Kwun Tong Road Westbound'
    elif road_choice == '2':
        road = 'Kwun Tong Road Eastbound'
    else:
        print("Invalid choice. Skipping prediction.")
        continue
    
    if road in models:
        predict_new_data(models[road], feature_names_dict[road], road, scalers[road])
    else:
        print(f"No model available for {road}.")
