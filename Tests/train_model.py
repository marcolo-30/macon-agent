import pandas as pd
import joblib
import json
import os
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
MODEL_FILE = os.path.join(SCRIPT_DIR, "model.pkl")
QOS_DEGRADATION_THRESHOLD = 95.0 # Consider QoS below this as a "failure" state for training

def train_model():
    """
    Loads profiling data, trains a logistic regression model, and saves it.
    """
    print("--- Starting Model Training ---")

    # --- 1. Load and Prepare Data ---
    # Find the latest profiling results file
    try:
        profiling_files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("profiling_results_")]
        if not profiling_files:
            print(f"❌ ERROR: No profiling data found in '{RESULTS_DIR}'. Run profiler_agent.py first.")
            return
        latest_profiling_file = max(profiling_files, key=lambda f: os.path.getmtime(os.path.join(RESULTS_DIR, f)))
        data_path = os.path.join(RESULTS_DIR, latest_profiling_file)
        print(f"Loading data from '{data_path}'...")
        with open(data_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ ERROR: Could not load or find profiling data. {e}")
        return

    # Convert to a pandas DataFrame for easier manipulation
    df = pd.DataFrame(data)
    if df.empty:
        print("❌ ERROR: Profiling data is empty.")
        return

    # --- 2. Feature Engineering ---
    # Our goal is to predict if the QoS will be bad based on CPU usage.
    # X = Feature (the input to the model)
    # y = Target (what we want to predict)
    
    # The feature is the average CPU percentage on the node
    X = df[['avg_cpu_percent']] 
    
    # The target is a binary flag: 1 if QoS was degraded, 0 otherwise.
    df['is_degraded'] = (df['avg_qos'] < QOS_DEGRADATION_THRESHOLD).astype(int)
    y = df['is_degraded']

    if len(df['is_degraded'].unique()) < 2:
        print("❌ ERROR: Training data has only one class (all runs were good or all were bad).")
        print("Model needs examples of both degraded and non-degraded states to learn.")
        return

    print("Training data prepared:")
    print(df[['node', 'avg_cpu_percent', 'avg_qos', 'is_degraded']])

    # --- 3. Model Training ---
    # Split data for basic validation (optional but good practice)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # Initialize and train the Logistic Regression model
    model = LogisticRegression()
    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    print(f"\nModel training complete. Accuracy on test data: {accuracy:.2f}")

    # --- 4. Save Model ---
    # We save the trained model so our agent can load and use it.
    joblib.dump(model, MODEL_FILE)
    print(f"✅ Model saved to '{MODEL_FILE}'")


if __name__ == "__main__":
    # Make sure you have installed scikit-learn, joblib, and pandas:
    # pip install scikit-learn joblib pandas
    train_model()
