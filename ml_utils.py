# ml_utils.py
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from pathlib import Path
import joblib
import os

MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

def get_model_path(workspace_id: int) -> Path:
    """Restituisce il percorso del file del modello per un dato workspace."""
    return MODEL_DIR / f"ws_{workspace_id}_category_classifier.joblib"

def train_model(workspace_id, data):
    """
    Allena un modello di classificazione del testo per uno specifico workspace.
    'data' deve essere una lista di tuple (descrizione, categoria).
    """
    model_path = get_model_path(workspace_id)
    
    if not data or len(data) < 2: # Abbassato il limite per testare più facilmente
        return False, "Dati insufficienti per l'allenamento. Aggiungi più movimenti con descrizioni."

    df = pd.DataFrame(data, columns=['description', 'category'])
    
    df = df.dropna(subset=['description'])
    df = df[df['description'].str.strip() != '']

    if df.empty or len(df['category'].unique()) < 2:
        return False, "Servono descrizioni valide e almeno due categorie diverse per l'allenamento."
    
    X = df['description']
    y = df['category']

    model = Pipeline([
        ('vectorizer', TfidfVectorizer(stop_words=None)),
        ('classifier', MultinomialNB())
    ])

    model.fit(X, y)

    joblib.dump(model, model_path)
    return True, "Modello allenato e salvato con successo!"

def predict_category(workspace_id, descriptions):
    """
    Prevede le categorie per una lista di descrizioni usando il modello del workspace.
    """
    model_path = get_model_path(workspace_id)
    if not model_path.exists():
        return None

    try:
        model = joblib.load(model_path)
        predictions = model.predict(descriptions)
        return predictions
    except Exception as e:
        print(f"Errore durante la predizione: {e}")
        return None

def predict_single(workspace_id, description):
    """
    Prevede la categoria per una singola descrizione usando il modello del workspace.
    """
    model_path = get_model_path(workspace_id)
    if not model_path.exists():
        return None
    
    try:
        model = joblib.load(model_path)
        prediction = model.predict([description])
        return prediction[0] if prediction is not None else "Da categorizzare"
    except Exception as e:
        print(f"Errore durante la predizione singola: {e}")
        return "Da categorizzare"
