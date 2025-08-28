# auth.py
import streamlit as st
import bcrypt
import sqlite3
from pathlib import Path

DB_PATH = Path("cashflow.db")

# --- DATABASE CONNECTION ---
def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute("PRAGMA journal_mode=WAL;")
    return c

# --- SCHEMA INITIALIZATION ---
def create_auth_schema():
    """Crea la tabella 'users' se non esiste, aggiungendo il campo 'role'."""
    with conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                security_question TEXT NOT NULL,
                security_answer_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user'
            );
        """)
        # Aggiunge la colonna 'role' se non esiste (per compatibilità con vecchi DB)
        try:
            c.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user';")
        except sqlite3.OperationalError:
            pass # La colonna esiste già

# --- UTILITY FUNCTIONS ---
def hash_value(value: str) -> str:
    """Esegue l'hashing di un valore (password o risposta di sicurezza)."""
    return bcrypt.hashpw(value.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_value(plain_value: str, hashed_value: str) -> bool:
    """Verifica un valore in chiaro con il suo corrispondente hash."""
    if not plain_value or not hashed_value:
        return False
    try:
        return bcrypt.checkpw(plain_value.encode('utf-8'), hashed_value.encode('utf-8'))
    except (ValueError, TypeError):
        return False

# --- CORE AUTHENTICATION LOGIC ---
def user_exists(username: str) -> bool:
    """Controlla se un nome utente esiste già nel database."""
    with conn() as c:
        return c.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone() is not None

def get_user_count() -> int:
    """Restituisce il numero totale di utenti registrati."""
    with conn() as c:
        count = c.execute("SELECT COUNT(id) FROM users").fetchone()
        return count[0] if count else 0

def create_user(username, password, question, answer):
    """Crea un nuovo utente. Il primo utente diventa admin."""
    if not (username and password and question and answer):
        return False, "Tutti i campi sono obbligatori."
    if user_exists(username):
        return False, "Questo nome utente è già stato preso."
    if len(password) < 8:
        return False, "La password deve essere di almeno 8 caratteri."

    password_hash = hash_value(password)
    answer_hash = hash_value(answer.lower().strip())
    
    # Il primo utente registrato è admin
    role = 'admin' if get_user_count() == 0 else 'user'
    
    with conn() as c:
        c.execute(
            "INSERT INTO users (username, password_hash, security_question, security_answer_hash, role) VALUES (?, ?, ?, ?, ?)",
            (username.strip(), password_hash, question, answer_hash, role)
        )
    return True, "Utente creato con successo! Ora puoi effettuare il login."

def authenticate_user(username, password):
    """Autentica un utente tramite username e password."""
    with conn() as c:
        user_data = c.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
    
    if user_data and verify_value(password, user_data[0]):
        return True
    return False

# --- ADMIN AND USER MANAGEMENT ---
def is_admin(username: str) -> bool:
    """Verifica se un utente ha il ruolo di admin."""
    with conn() as c:
        result = c.execute("SELECT role FROM users WHERE username = ?", (username,)).fetchone()
    return result and result[0] == 'admin'

def get_all_users():
    """Restituisce una lista di tutti gli utenti (username e ruolo)."""
    with conn() as c:
        return c.execute("SELECT username, role FROM users ORDER BY username").fetchall()

def delete_user(username: str):
    """Elimina un utente dal database."""
    if get_user_count() <= 1:
        return False, "Non puoi eliminare l'unico utente rimasto."
    with conn() as c:
        c.execute("DELETE FROM users WHERE username = ?", (username,))
    return True, f"Utente '{username}' eliminato con successo."


# --- PASSWORD RECOVERY LOGIC ---
def get_security_question(username):
    """Recupera la domanda di sicurezza per un dato utente."""
    if not user_exists(username):
        return None
    with conn() as c:
        result = c.execute("SELECT security_question FROM users WHERE username = ?", (username,)).fetchone()
    return result[0] if result else None

def verify_security_answer(username, answer):
    """Verifica la risposta di sicurezza fornita dall'utente."""
    with conn() as c:
        result = c.execute("SELECT security_answer_hash FROM users WHERE username = ?", (username,)).fetchone()
    
    if result and verify_value(answer.lower().strip(), result[0]):
        return True
    return False

def reset_password(username, new_password):
    """Aggiorna la password per un dato utente."""
    if len(new_password) < 8:
        return False, "La nuova password deve essere di almeno 8 caratteri."
    
    new_password_hash = hash_value(new_password)
    with conn() as c:
        c.execute("UPDATE users SET password_hash = ? WHERE username = ?", (new_password_hash, username))
    return True, "Password aggiornata con successo. Ora puoi effettuare il login."
