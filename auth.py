# auth.py
import streamlit as st
import bcrypt
import sqlite3
from pathlib import Path

DB_PATH = Path("cashflow.db")

# --- DATABASE CONNECTION ---
def conn():
    """Connessione unificata al database principale."""
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute("PRAGMA journal_mode=WAL;")
    return c

# --- SCHEMA INITIALIZATION ---
def create_auth_schema():
    """
    Crea le tabelle 'users', 'workspaces', e 'workspace_members' se non esistono.
    Queste tabelle gestiscono l'autenticazione e i permessi.
    """
    with conn() as c:
        # Tabella Utenti (la colonna 'role' globale non è più usata per i permessi)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                security_question TEXT NOT NULL,
                security_answer_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user' -- Mantenuta per retrocompatibilità ma non usata per logica admin
            );
        """)
        # Tabella Spazi di Lavoro (Workspace)
        c.execute("""
            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                owner_user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)
        # Tabella Membri del Workspace (collega Utenti e Workspace con un ruolo)
        c.execute("""
            CREATE TABLE IF NOT EXISTS workspace_members (
                workspace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('owner', 'editor', 'viewer')),
                PRIMARY KEY (workspace_id, user_id),
                FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)

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

def get_user_id(username: str) -> int:
    """Recupera l'ID di un utente dal suo username."""
    with conn() as c:
        result = c.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        return result[0] if result else None

def get_user_count() -> int:
    """Restituisce il numero totale di utenti registrati."""
    with conn() as c:
        count = c.execute("SELECT COUNT(id) FROM users").fetchone()
        return count[0] if count else 0

def create_user(username, password, question, answer):
    """Crea un nuovo utente e il suo workspace personale."""
    if not (username and password and question and answer):
        return False, "Tutti i campi sono obbligatori."
    if user_exists(username):
        return False, "Questo nome utente è già stato preso."
    if len(password) < 8:
        return False, "La password deve essere di almeno 8 caratteri."

    password_hash = hash_value(password)
    answer_hash = hash_value(answer.lower().strip())
    
    # MODIFICA: Ogni utente è 'user'. Il ruolo di admin globale è rimosso.
    role = 'user'
    
    try:
        with conn() as c:
            cursor = c.execute(
                "INSERT INTO users (username, password_hash, security_question, security_answer_hash, role) VALUES (?, ?, ?, ?, ?)",
                (username.strip(), password_hash, question, answer_hash, role)
            )
            new_user_id = cursor.lastrowid
            workspace_name = f"Workspace di {username.strip()}"
            create_workspace(new_user_id, workspace_name, c)
            
        return True, "Utente creato con successo! Ora puoi effettuare il login."
    except sqlite3.Error as e:
        return False, f"Errore del database: {e}"


def authenticate_user(username, password):
    """Autentica un utente tramite username e password."""
    with conn() as c:
        user_data = c.execute("SELECT password_hash FROM users WHERE username = ?", (username,)).fetchone()
    
    if user_data and verify_value(password, user_data[0]):
        return True
    return False

# --- WORKSPACE MANAGEMENT ---
def create_workspace(user_id: int, workspace_name: str, db_connection=None):
    c = db_connection if db_connection else conn()
    try:
        cursor = c.execute("INSERT INTO workspaces (name, owner_user_id) VALUES (?, ?)", (workspace_name, user_id))
        workspace_id = cursor.lastrowid
        c.execute("INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (?, ?, ?)", (workspace_id, user_id, 'owner'))
        if not db_connection: c.commit()
        return workspace_id
    finally:
        if not db_connection: c.close()

def get_user_workspaces(user_id: int) -> list:
    query = "SELECT w.id, w.name, m.role FROM workspaces w JOIN workspace_members m ON w.id = m.workspace_id WHERE m.user_id = ? ORDER BY w.name;"
    with conn() as c:
        return c.execute(query, (user_id,)).fetchall()

# NUOVE FUNZIONI DI GESTIONE PERMESSI
def get_workspace_members(workspace_id: int):
    """Recupera tutti i membri di un workspace."""
    query = "SELECT u.id, u.username, m.role FROM users u JOIN workspace_members m ON u.id = m.user_id WHERE m.workspace_id = ?"
    with conn() as c:
        return c.execute(query, (workspace_id,)).fetchall()

def add_user_to_workspace(workspace_id: int, username_to_add: str, role: str):
    """Aggiunge un utente esistente a un workspace."""
    user_id_to_add = get_user_id(username_to_add)
    if not user_id_to_add:
        return False, f"Utente '{username_to_add}' non trovato."
    if role not in ['editor', 'viewer']:
        return False, "Ruolo non valido."
    
    with conn() as c:
        try:
            c.execute("INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (?, ?, ?)", (workspace_id, user_id_to_add, role))
            return True, f"Utente '{username_to_add}' aggiunto al workspace."
        except sqlite3.IntegrityError:
            return False, f"L'utente '{username_to_add}' è già membro di questo workspace."

def remove_user_from_workspace(workspace_id: int, user_id_to_remove: int):
    """Rimuove un utente da un workspace."""
    with conn() as c:
        # Assicurati di non poter rimuovere il proprietario
        owner_check = c.execute("SELECT owner_user_id FROM workspaces WHERE id = ?", (workspace_id,)).fetchone()
        if owner_check and owner_check[0] == user_id_to_remove:
            return False, "Non è possibile rimuovere il proprietario del workspace."
        
        c.execute("DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?", (workspace_id, user_id_to_remove))
        return True, "Utente rimosso dal workspace."

# --- USER MANAGEMENT (NON PIU' GLOBALE) ---
def get_all_users_for_invite():
    """Restituisce tutti gli utenti per i menu a tendina degli inviti."""
    with conn() as c:
        return c.execute("SELECT username FROM users ORDER BY username").fetchall()

def delete_user(username: str):
    """Elimina un utente dal database. Vengono eliminati a cascata anche i workspace di sua proprietà."""
    if get_user_count() <= 1:
        return False, "Non puoi eliminare l'unico utente rimasto."
    with conn() as c:
        user_id_to_delete = get_user_id(username)
        if user_id_to_delete:
            c.execute("DELETE FROM users WHERE id = ?", (user_id_to_delete,))
    return True, f"Utente '{username}' eliminato con successo."

# --- PASSWORD RECOVERY LOGIC ---
def get_security_question(username):
    if not user_exists(username): return None
    with conn() as c:
        result = c.execute("SELECT security_question FROM users WHERE username = ?", (username,)).fetchone()
    return result[0] if result else None

def verify_security_answer(username, answer):
    with conn() as c:
        result = c.execute("SELECT security_answer_hash FROM users WHERE username = ?", (username,)).fetchone()
    if result and verify_value(answer.lower().strip(), result[0]):
        return True
    return False

def reset_password(username, new_password):
    if len(new_password) < 8:
        return False, "La nuova password deve essere di almeno 8 caratteri."
    new_password_hash = hash_value(new_password)
    with conn() as c:
        c.execute("UPDATE users SET password_hash = ? WHERE username = ?", (new_password_hash, username))
    return True, "Password aggiornata con successo. Ora puoi effettuare il login."
