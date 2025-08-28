# ml_utils.py
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from pathlib import Path
import joblib

# Percorso dove salvare il modello allenato
MODEL_PATH = Path("category_classifier.joblib")

def train_model(data):
    """
    Allena un modello di classificazione del testo.
    'data' deve essere una lista di tuple (descrizione, categoria).
    """
    if not data or len(data) < 10: # Richiede un minimo di dati per allenarsi
        return False, "Dati insufficienti per l'allenamento. Aggiungi più movimenti con descrizioni."

    df = pd.DataFrame(data, columns=['description', 'category'])
    
    df = df.dropna(subset=['description'])
    df = df[df['description'].str.strip() != '']

    if df.empty:
        return False, "Nessuna descrizione valida trovata per l'allenamento."
    
    X = df['description']
    y = df['category']

    model = Pipeline([
        ('vectorizer', TfidfVectorizer(stop_words=None)),
        ('classifier', MultinomialNB())
    ])

    model.fit(X, y)

    joblib.dump(model, MODEL_PATH)
    return True, "Modello allenato e salvato con successo!"

def predict_category(descriptions):
    """
    Prevede le categorie per una lista di descrizioni.
    """
    if not MODEL_PATH.exists():
        return None

    model = joblib.load(MODEL_PATH)
    predictions = model.predict(descriptions)
    return predictions

def predict_single(description):
    """
    Prevede la categoria per una singola descrizione.
    """
    if not MODEL_PATH.exists():
        return None
    
    model = joblib.load(MODEL_PATH)
    # Il modello si aspetta una lista, quindi gli passiamo una lista con un solo elemento
    prediction = model.predict([description])
    return prediction[0] if prediction is not None else "Sconosciuta"