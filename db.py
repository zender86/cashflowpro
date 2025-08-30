# db.py
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

DB_PATH = Path("cashflow.db")

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    name TEXT NOT NULL, 
    opening_balance REAL NOT NULL DEFAULT 0,
    type TEXT NOT NULL DEFAULT 'standard',
    credit_limit REAL,
    statement_day INTEGER,
    UNIQUE(workspace_id, name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('income','expense','transfer')),
    UNIQUE(workspace_id, name),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    tx_date TEXT NOT NULL,
    amount REAL NOT NULL,
    account_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS recurring (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    start_date TEXT NOT NULL,
    interval TEXT NOT NULL CHECK(interval IN ('daily','weekly','monthly')),
    amount REAL NOT NULL,
    account_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    description TEXT,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS budgets (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
    category_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    account_id INTEGER,
    UNIQUE(workspace_id, year, month, category_id, account_id),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE,
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS debts (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    person TEXT NOT NULL,
    amount REAL NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('lent', 'borrowed')),
    due_date TEXT,
    status TEXT NOT NULL DEFAULT 'outstanding',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    keyword TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    UNIQUE(workspace_id, keyword),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS planned_transactions (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    plan_date TEXT NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    category_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned',
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE,
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS goals (
    id INTEGER PRIMARY KEY,
    workspace_id INTEGER NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    priority INTEGER DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS settings (
    workspace_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY(workspace_id, key),
    FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
"""

DEFAULT_CATEGORIES = [("Stipendio", "income"), ("Interessi", "income"),("Spesa alimentare", "expense"), ("Ristorante e bar", "expense"),("Benzina", "expense"), ("Trasporti", "expense"), ("Bolletta luce", "expense"),("Affitto", "expense"), ("Telefonia/Internet", "expense"),("Tempo libero", "expense"), ("Tasse", "expense"), ("Trasferimento", "transfer"), ("Restituzione Prestito", "income"), ("Pagamento Debito", "expense"), ("Da categorizzare", "expense")]

def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute("PRAGMA journal_mode=WAL;")
    return c

def init_db():
    with conn() as c:
        c.executescript(SCHEMA)
        c.commit()

def populate_new_workspace(workspace_id):
    with conn() as c:
        setup_done = c.execute("SELECT value FROM settings WHERE workspace_id = ? AND key = 'initial_setup_done'", (workspace_id,)).fetchone()
        if not setup_done:
            categories_with_ws = [(workspace_id, name, type) for name, type in DEFAULT_CATEGORIES]
            c.executemany("INSERT OR IGNORE INTO categories(workspace_id, name, type) VALUES(?, ?, ?)", categories_with_ws)
            c.execute("INSERT OR REPLACE INTO settings (workspace_id, key, value) VALUES (?, 'initial_setup_done', 'true')", (workspace_id,))
            print(f"Workspace {workspace_id} initialized with default categories.")
            c.commit()

def reset_db():
    with conn() as c:
        cursor = c.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF;")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table_name in tables:
            if table_name[0] not in ["sqlite_sequence", "users", "workspaces", "workspace_members"]:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name[0]};")
        cursor.execute("PRAGMA foreign_keys=ON;")
        c.commit()
    from auth import create_auth_schema
    create_auth_schema()
    init_db()

def get_db_data(query, params=()):
    with conn() as c: return c.execute(query, params).fetchall()

def parse_date(d):
    if isinstance(d, datetime): return d.date()
    if isinstance(d, date): return d
    try: return datetime.strptime(d, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        try: return datetime.strptime(d, "%d/%m/%Y").date()
        except (TypeError, ValueError): return None

def get_or_create(c, table, workspace_id, name, type=None):
    ALLOWED_TABLES = ['accounts', 'categories'];
    if table not in ALLOWED_TABLES: raise ValueError(f"Tabella non consentita: {table}")
    query_select = f"SELECT id FROM {table} WHERE name = ? AND workspace_id = ?";
    params_select = (name, workspace_id)
    if table == 'categories' and type:
        query_select += " AND type = ?"; params_select = (name, workspace_id, type)
    cur = c.execute(query_select, params_select)
    row = cur.fetchone()
    if row: return row[0]
    else:
        if table == 'accounts': return None
        query_insert = f"INSERT INTO {table} (workspace_id, name) VALUES (?, ?)"; params_insert = (workspace_id, name)
        if table == 'categories' and type:
            query_insert = f"INSERT INTO {table} (workspace_id, name, type) VALUES (?, ?, ?)"; params_insert = (workspace_id, name, type)
        cur = c.execute(query_insert, params_insert)
        return cur.lastrowid

# --- TRANSACTIONS ---
def add_tx(workspace_id, tx_date, account_name, category_name, amount, desc=None):
    tx_date_obj = parse_date(tx_date);
    if not tx_date_obj: return
    with conn() as c:
        cur = c.execute("SELECT type FROM categories WHERE name = ? AND workspace_id = ?", (category_name, workspace_id)); cat_type = cur.fetchone()
        acc_id = get_or_create(c, 'accounts', workspace_id, account_name)
        cat_id = get_or_create(c, 'categories', workspace_id, category_name, type=cat_type[0] if cat_type else 'expense')
        c.execute("INSERT INTO transactions(workspace_id, tx_date, amount, account_id, category_id, description) VALUES(?,?,?,?,?,?)", (workspace_id, tx_date_obj.isoformat(), amount, acc_id, cat_id, desc))

def update_tx(workspace_id, tx_id, new_date, new_account, new_category, new_amount, new_description):
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', workspace_id, new_account)
        cat_id = get_or_create(c, 'categories', workspace_id, new_category)
        c.execute("UPDATE transactions SET tx_date=?, account_id=?, category_id=?, amount=?, description=? WHERE id=? AND workspace_id=?", (parse_date(new_date).isoformat(), acc_id, cat_id, new_amount, new_description, tx_id, workspace_id))

def bulk_update_transactions(workspace_id, transaction_ids, new_category_name=None, new_account_name=None):
    if not transaction_ids or (new_category_name is None and new_account_name is None): return
    with conn() as c:
        set_clauses, params = [], []
        if new_category_name:
            cat_id_result = c.execute("SELECT id FROM categories WHERE name = ? AND workspace_id = ?", (new_category_name, workspace_id)).fetchone()
            if cat_id_result:
                set_clauses.append("category_id = ?"); params.append(cat_id_result[0])
        if new_account_name:
            acc_id_result = c.execute("SELECT id FROM accounts WHERE name = ? AND workspace_id = ?", (new_account_name, workspace_id)).fetchone()
            if acc_id_result:
                set_clauses.append("account_id = ?"); params.append(acc_id_result[0])
        if not set_clauses: return
        query = f"UPDATE transactions SET {', '.join(set_clauses)} WHERE id IN ({','.join('?' for _ in transaction_ids)}) AND workspace_id = ?"
        params.extend(transaction_ids)
        params.append(workspace_id)
        c.execute(query, tuple(params))

def delete_tx(workspace_id, tx_id):
    with conn() as c: c.execute("DELETE FROM transactions WHERE id=? AND workspace_id=?", (tx_id, workspace_id))

def bulk_delete_transactions(workspace_id, transaction_ids):
    if not transaction_ids: return 0
    with conn() as c:
        placeholders = ','.join('?' for _ in transaction_ids)
        query = f"DELETE FROM transactions WHERE id IN ({placeholders}) AND workspace_id = ?"
        params = transaction_ids + [workspace_id]
        cursor = c.execute(query, params)
        return cursor.rowcount

def get_all_transactions_raw(workspace_id):
    query = "SELECT t.id, t.tx_date, a.name AS account, c.name AS category, t.amount, COALESCE(t.description,'') as description FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN categories c ON c.id = t.category_id WHERE t.workspace_id = ? ORDER BY t.tx_date DESC, t.id DESC"
    return get_db_data(query, (workspace_id,))

def get_transaction_by_id(workspace_id, tx_id):
    query = "SELECT t.id, t.tx_date, a.name AS account, c.name AS category, t.amount, COALESCE(t.description,'') as description FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN categories c ON c.id = t.category_id WHERE t.id = ? AND t.workspace_id = ?"
    result = get_db_data(query, (tx_id, workspace_id))
    return result[0] if result else None

def get_transactions_in_range(workspace_id, start_date, end_date, account_name=None):
    tx_q = "SELECT tx_date, amount FROM transactions t JOIN accounts a ON a.id = t.account_id WHERE t.workspace_id = ? AND tx_date BETWEEN ? AND ?"
    tx_params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    if account_name:
        tx_q += " AND a.name = ?"; tx_params.append(account_name)
    return get_db_data(tx_q, tuple(tx_params))

def get_transactions_for_training(workspace_id):
    query = "SELECT T.description, C.name as category FROM transactions T JOIN categories C on T.category_id = C.id WHERE T.workspace_id = ? AND T.description IS NOT NULL AND T.description != ''"
    return get_db_data(query, (workspace_id,))

# --- ACCOUNTS ---
def add_account(workspace_id, name, balance=0.0, acc_type='standard', limit=None, day=None):
    with conn() as c:
        c.execute("INSERT INTO accounts(workspace_id, name, opening_balance, type, credit_limit, statement_day) VALUES(?, ?, ?, ?, ?, ?)",
                  (workspace_id, name, balance, acc_type, limit, day))

def update_account(workspace_id, old_name, new_name, new_balance, new_type, new_limit, new_day):
    with conn() as c:
        c.execute("UPDATE accounts SET name=?, opening_balance=?, type=?, credit_limit=?, statement_day=? WHERE name=? AND workspace_id = ?",
                  (new_name, new_balance, new_type, new_limit, new_day, old_name, workspace_id))

def delete_account(workspace_id, name):
    with conn() as c: c.execute("DELETE FROM accounts WHERE name=? AND workspace_id = ?", (name, workspace_id))

def get_all_accounts(workspace_id, with_details=False):
    if with_details:
        return get_db_data("SELECT id, name, type FROM accounts WHERE workspace_id = ? ORDER BY name ASC", (workspace_id,))
    return [row[0] for row in get_db_data("SELECT name FROM accounts WHERE workspace_id = ? ORDER BY name ASC", (workspace_id,))]

def get_account_details_by_name(workspace_id, name):
    query = "SELECT name, opening_balance, type, credit_limit, statement_day FROM accounts WHERE name = ? AND workspace_id = ?"
    result = get_db_data(query, (name, workspace_id))
    return result[0] if result else None

def get_accounts_with_balance(workspace_id):
    query = """
    SELECT 
        a.name, a.type, a.credit_limit,
        CASE 
            WHEN a.type = 'credit_card' THEN a.credit_limit + COALESCE(SUM(t.amount), 0)
            ELSE a.opening_balance + COALESCE(SUM(t.amount), 0) 
        END as display_balance,
        CASE
            WHEN a.type = 'credit_card' THEN COALESCE(SUM(t.amount), 0)
            ELSE NULL
        END as amount_due
    FROM accounts a 
    LEFT JOIN transactions t ON a.id = t.account_id AND t.workspace_id = a.workspace_id
    WHERE a.workspace_id = ?
    GROUP BY a.id, a.name, a.type, a.credit_limit, a.opening_balance 
    ORDER BY a.name
    """
    return get_db_data(query, (workspace_id,))

# --- CATEGORIES ---
def get_all_categories(workspace_id):
    return [row[0] for row in get_db_data("SELECT name FROM categories WHERE workspace_id = ? ORDER BY name ASC", (workspace_id,))]

def get_all_categories_with_types(workspace_id):
    return get_db_data("SELECT id, name, type FROM categories WHERE workspace_id = ? ORDER BY type, name ASC", (workspace_id,))

def add_category(workspace_id, name, type):
    with conn() as c:
        try: c.execute("INSERT INTO categories (workspace_id, name, type) VALUES (?, ?, ?)", (workspace_id, name, type)); return True, None
        except sqlite3.IntegrityError: return False, "Una categoria con questo nome esiste già."

def bulk_add_categories(workspace_id, categories_to_add):
    with_ws_id = [(workspace_id, name, type) for name, type in categories_to_add]
    with conn() as c: c.executemany("INSERT OR IGNORE INTO categories (workspace_id, name, type) VALUES (?, ?, ?)", with_ws_id)

def update_category(workspace_id, category_id, new_name, new_type):
    with conn() as c:
        try: c.execute("UPDATE categories SET name = ?, type = ? WHERE id = ? AND workspace_id = ?", (new_name, new_type, category_id, workspace_id)); return True, None
        except sqlite3.IntegrityError: return False, "Una categoria con questo nome esiste già."

def delete_category(workspace_id, category_id):
    with conn() as c:
        try: c.execute("DELETE FROM categories WHERE id = ? AND workspace_id = ?", (category_id, workspace_id)); return True, None
        except sqlite3.IntegrityError: return False, "La categoria è utilizzata da uno o più movimenti e non può essere eliminata."
        
def delete_unused_categories(workspace_id):
    with conn() as c:
        cursor = c.execute("DELETE FROM categories WHERE workspace_id = ? AND id NOT IN (SELECT DISTINCT category_id FROM transactions WHERE workspace_id = ?)", (workspace_id, workspace_id))
        return cursor.rowcount

# --- SUMMARY & ANALYSIS ---
def get_summary_by_category(workspace_id, start_date, end_date, account_name=None):
    params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    query = "SELECT c.name, SUM(ABS(t.amount)) AS total FROM transactions t JOIN categories c ON c.id = t.category_id JOIN accounts a ON a.id = t.account_id WHERE t.workspace_id = ? AND t.amount < 0 AND t.tx_date BETWEEN ? AND ?"
    if account_name:
        query += " AND a.name = ?"; params.append(account_name)
    query += " GROUP BY c.name ORDER BY total DESC"
    return get_db_data(query, tuple(params))

# --- MODIFICA CHIAVE FINALE: Logica ibrida e robusta per il grafico ---
def get_monthly_summary(workspace_id, start_date, end_date, account_name=None):
    params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    query = """
        SELECT 
            strftime('%Y-%m', t.tx_date) AS month, 
            SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS income, 
            SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END) AS expense 
        FROM transactions t 
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.workspace_id = ? AND t.tx_date BETWEEN ? AND ? AND (c.type IS NULL OR c.type != 'transfer')
    """
    if account_name:
        account_filter_query = " AND t.account_id IN (SELECT id FROM accounts WHERE name = ? AND workspace_id = ?)"
        query += account_filter_query
        params.extend([account_name, workspace_id])

    query += " GROUP BY month ORDER BY month"
    return get_db_data(query, tuple(params))


def get_balance_before_date(workspace_id, start_date, account_name=None):
    acc_bal_q = "SELECT COALESCE(SUM(opening_balance), 0) FROM accounts WHERE workspace_id = ? AND type = 'standard'"
    acc_bal_params = [workspace_id]
    if account_name:
        acc_bal_q += " AND name = ?"; acc_bal_params.append(account_name)
    initial_balance_acc = get_db_data(acc_bal_q, tuple(acc_bal_params))[0][0]
    
    bal_q = "SELECT COALESCE(SUM(t.amount), 0) FROM transactions t JOIN accounts a ON a.id = t.account_id WHERE t.workspace_id = ? AND t.tx_date < ? AND a.type = 'standard'"
    bal_params = [workspace_id, start_date.isoformat()]
    if account_name:
        bal_q += " AND a.name = ?"; bal_params.append(account_name)
    balance_tx = get_db_data(bal_q, tuple(bal_params))[0][0]
    return initial_balance_acc + balance_tx

def get_data_for_sankey(workspace_id, start_date, end_date, account_name=None):
    query = "SELECT c.name as category, SUM(t.amount) as amount FROM transactions t JOIN categories c ON c.id = t.category_id JOIN accounts a ON a.id = t.account_id WHERE t.workspace_id = ? AND t.tx_date BETWEEN ? AND ? AND c.type != 'transfer' "
    params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    if account_name and account_name != "Tutti":
        query += " AND a.name = ? "; params.append(account_name)
    query += " GROUP BY c.name"
    return get_db_data(query, tuple(params))
    
def get_net_worth(workspace_id):
    accounts_data = get_accounts_with_balance(workspace_id)
    total_liquidity = sum(row[3] for row in accounts_data if row[1] == 'standard')
    total_cc_debt = sum(row[4] for row in accounts_data if row[1] == 'credit_card' and row[4] is not None)
    total_borrowed_data = get_db_data("SELECT COALESCE(SUM(amount), 0) FROM debts WHERE workspace_id = ? AND type = 'borrowed' AND status = 'outstanding'", (workspace_id,))
    total_borrowed = total_borrowed_data[0][0] if total_borrowed_data else 0
    return total_liquidity + total_cc_debt - total_borrowed

def get_category_trend(workspace_id, category_name, start_date, end_date):
    query = "SELECT strftime('%Y-%m', t.tx_date) as month, SUM(ABS(t.amount)) FROM transactions t JOIN categories c ON t.category_id = c.id WHERE t.workspace_id = ? AND c.name = ? AND t.amount < 0 AND t.tx_date BETWEEN ? AND ? GROUP BY month ORDER BY month ASC"
    params = (workspace_id, category_name, start_date.isoformat(), end_date.isoformat())
    return get_db_data(query, params)

# --- RECURRING & PLANNED ---
def get_recurring_transactions(workspace_id):
    query = "SELECT r.id, r.name, r.start_date, r.interval, r.amount, a.name, c.name, COALESCE(r.description,'') FROM recurring r JOIN accounts a ON a.id = r.account_id JOIN categories c ON c.id = r.category_id WHERE r.workspace_id = ? ORDER BY r.start_date DESC"
    return get_db_data(query, (workspace_id,))

def add_recurring(workspace_id, name, start_date, interval, amount, account_name, category_name, description):
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', workspace_id, account_name)
        cat_id = get_or_create(c, 'categories', workspace_id, category_name)
        c.execute("INSERT INTO recurring (workspace_id, name, start_date, interval, amount, account_id, category_id, description) VALUES (?,?,?,?,?,?,?,?)",
                  (workspace_id, name, parse_date(start_date).isoformat(), interval, amount, acc_id, cat_id, description))

def delete_recurring(workspace_id, recurring_id):
    with conn() as c: c.execute("DELETE FROM recurring WHERE id = ? AND workspace_id = ?", (recurring_id, workspace_id))

def add_planned_tx(workspace_id, plan_date, description, amount, category_name, account_name):
    plan_date_obj = parse_date(plan_date)
    if not plan_date_obj: return
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', workspace_id, account_name)
        cat_id = get_or_create(c, 'categories', workspace_id, category_name)
        c.execute("INSERT INTO planned_transactions(workspace_id, plan_date, description, amount, account_id, category_id) VALUES(?,?,?,?,?,?)", (workspace_id, plan_date_obj.isoformat(), description, amount, acc_id, cat_id))

def get_all_planned_tx(workspace_id):
    query = "SELECT p.id, p.plan_date, p.description, p.amount, c.name as category, a.name as account FROM planned_transactions p JOIN categories c ON p.category_id = c.id JOIN accounts a ON p.account_id = a.id WHERE p.workspace_id = ? AND p.status = 'planned' ORDER BY p.plan_date ASC"
    return get_db_data(query, (workspace_id,))

def delete_planned_tx(workspace_id, planned_tx_id):
    with conn() as c: c.execute("DELETE FROM planned_transactions WHERE id = ? AND workspace_id = ?", (planned_tx_id, workspace_id))

def get_future_events(workspace_id, start_date, end_date, account_name=None):
    events = []
    
    # Eventi da OGGI in poi
    real_tx_query = """
        SELECT t.tx_date, t.description, t.amount, t.category_id, c.name as category_name
        FROM transactions t 
        JOIN accounts a ON t.account_id = a.id
        JOIN categories c ON t.category_id = c.id
        WHERE t.workspace_id = ? AND t.tx_date >= ? AND t.tx_date <= ? AND a.type = 'standard'
    """
    params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    if account_name:
        real_tx_query += " AND a.name = ?"; params.append(account_name)
    for date_str, desc, amount, cat_id, cat_name in get_db_data(real_tx_query, tuple(params)):
        events.append({'date': parse_date(date_str), 'description': f"(Reale) {desc}", 'amount': amount, 'category_id': cat_id, 'category': cat_name})

    planned_query = """
        SELECT p.plan_date, p.description, p.amount, p.category_id, c.name as category_name
        FROM planned_transactions p
        JOIN accounts a ON p.account_id = a.id
        JOIN categories c ON p.category_id = c.id
        WHERE p.workspace_id = ? AND p.plan_date >= ? AND p.plan_date <= ? AND a.type = 'standard'
    """
    params = [workspace_id, start_date.isoformat(), end_date.isoformat()]
    if account_name:
        planned_query += " AND a.name = ?"; params.append(account_name)
    for date_str, desc, amount, cat_id, cat_name in get_db_data(planned_query, tuple(params)):
        events.append({'date': parse_date(date_str), 'description': f"(Pianificato) {desc}", 'amount': amount, 'category_id': cat_id, 'category': cat_name})

    real_and_planned_lookup = {(e['date'], e['category_id']) for e in events}

    rec_query = """
        SELECT r.start_date, r.interval, r.amount, r.name, r.category_id, c.name as category_name
        FROM recurring r
        JOIN accounts a ON a.id = r.account_id
        JOIN categories c ON r.category_id = c.id
        WHERE r.workspace_id = ? AND a.type = 'standard'
    """
    rec_params = [workspace_id]
    if account_name:
        rec_query += " AND a.name = ?"; rec_params.append(account_name)
    
    for r_start_date_str, interval, amount, name, cat_id, cat_name in get_db_data(rec_query, tuple(rec_params)):
        curr_date = parse_date(r_start_date_str)
        
        if curr_date < start_date:
            if interval == 'daily':
                curr_date = start_date
            elif interval == 'weekly':
                days_diff = (start_date.weekday() - curr_date.weekday() + 7) % 7
                curr_date = start_date + timedelta(days=days_diff)
            elif interval == 'monthly':
                try:
                    next_occurrence = start_date.replace(day=curr_date.day)
                except ValueError:
                    _, last_day = calendar.monthrange(start_date.year, start_date.month)
                    next_occurrence = start_date.replace(day=last_day)
                if next_occurrence < start_date:
                    next_occurrence += relativedelta(months=1)
                curr_date = next_occurrence
        
        while curr_date <= end_date:
            if (curr_date, cat_id) not in real_and_planned_lookup:
                events.append({'date': curr_date, 'description': f"(Ricorrente) {name}", 'amount': amount, 'category_id': cat_id, 'category': cat_name})
            
            if interval == "daily": curr_date += relativedelta(days=1)
            elif interval == "weekly": curr_date += relativedelta(weeks=1)
            elif interval == "monthly": curr_date += relativedelta(months=1)
            else: break

    return sorted(events, key=lambda x: x['date'])

# --- BUDGETS, DEBTS, RULES, GOALS, ETC. ---
def find_recurring_suggestions(workspace_id):
    tx_query = """
        SELECT t.tx_date, t.amount, COALESCE(t.description, '') as description, 
               c.name as category_name, a.name as account_name, c.type as category_type
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        JOIN accounts a ON t.account_id = a.id
        WHERE t.workspace_id = ? AND t.amount != 0
    """
    df = pd.DataFrame(get_db_data(tx_query, (workspace_id,)), columns=['date', 'amount', 'description', 'category_name', 'account_name', 'category_type'])
    if df.empty: return []

    df['date'] = pd.to_datetime(df['date']); df.sort_values('date', inplace=True)
    df['normalized_desc'] = df['description'].str.lower().str.strip()
    df['grouping_desc'] = np.where(df['category_type'] == 'income', '---income_group---', df['normalized_desc'])
    
    df['amount_group'] = ((df['amount'] / 5).round()).astype(int)

    rec_data = get_recurring_transactions(workspace_id)
    existing_recurring = set()
    if rec_data:
        for _, name, _, interval, _, account, category, _ in rec_data:
            existing_recurring.add((name.lower().strip(), interval, category, account))
            
    suggestions, grouped = [], df.groupby(['category_name', 'account_name', 'grouping_desc', 'amount_group'])

    for name_keys, group in grouped:
        if len(group) < 3: continue
        group = group.copy(); group['interval'] = group['date'].diff().dt.days
        category_name, account_name, grouping_desc, _ = name_keys
        
        interval_type = None
        if group['interval'].between(28, 32).sum() / (len(group) - 1) >= 0.8: interval_type = 'monthly'
        elif group['interval'].between(6, 8).sum() / (len(group) - 1) >= 0.8: interval_type = 'weekly'

        if interval_type:
            avg_amount = group['amount'].mean()
            first_date = group['date'].min()
            
            display_desc = category_name if grouping_desc == '---income_group---' else (group['normalized_desc'].iloc[0].capitalize() if group['normalized_desc'].iloc[0] else 'Movimento')
            key = (display_desc.lower().strip(), interval_type, category_name, account_name)
            if key not in existing_recurring:
                suggestions.append((display_desc, avg_amount, interval_type, category_name, account_name, first_date.strftime('%Y-%m-%d')))
    return suggestions

def get_budgets_by_year(workspace_id, year):
    query = "SELECT b.id, b.month, c.name, COALESCE(a.name, 'Tutti i conti') as account_name, b.amount FROM budgets b JOIN categories c ON c.id = b.category_id LEFT JOIN accounts a ON a.id = b.account_id WHERE b.workspace_id = ? AND b.year=? ORDER BY b.month, c.name"
    return get_db_data(query, (workspace_id, year))

def add_budget(workspace_id, year, month, category_name, account_name, amount):
    with conn() as c:
        cat_id = get_or_create(c, 'categories', workspace_id, category_name, 'expense')
        acc_id = get_or_create(c, 'accounts', workspace_id, account_name) if account_name != 'Tutti i conti' else None
        query = "INSERT INTO budgets (workspace_id, year, month, category_id, account_id, amount) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(workspace_id, year, month, category_id, account_id) DO UPDATE SET amount = excluded.amount;"
        c.execute(query, (workspace_id, year, month, cat_id, acc_id, amount))

def delete_budget(workspace_id, budget_id):
    with conn() as c: c.execute("DELETE FROM budgets WHERE id = ? AND workspace_id = ?", (budget_id, workspace_id))

def get_actual_expenses_by_year(workspace_id, year):
    query = "SELECT CAST(strftime('%m', t.tx_date) AS INTEGER) as month, c.name as category_name, a.name as account_name, SUM(t.amount) as total_spent FROM transactions t JOIN categories c ON t.category_id = c.id JOIN accounts a ON t.account_id = a.id WHERE t.workspace_id = ? AND STRFTIME('%Y', t.tx_date) = ? AND t.amount < 0 GROUP BY month, category_name, account_name"
    data = get_db_data(query, (workspace_id, str(year)))
    actuals, totals_by_category = {}, {}
    for month, category, account, total in data: actuals[(month, category, account)] = abs(total)
    for (month, category, _), total in actuals.items(): totals_by_category[(month, category)] = totals_by_category.get((month, category), 0) + total
    for (month, category), total in totals_by_category.items(): actuals[(month, category, "Tutti i conti")] = total
    return actuals

def add_debt(workspace_id, person, amount, type, due_date):
    with conn() as c: c.execute("INSERT INTO debts (workspace_id, person, amount, type, due_date) VALUES (?, ?, ?, ?, ?)", (workspace_id, person, amount, type, parse_date(due_date).isoformat()))

def get_debts(workspace_id, status='outstanding'):
    return get_db_data("SELECT * FROM debts WHERE workspace_id = ? AND status = ? ORDER BY due_date ASC", (workspace_id, status))

def settle_debt(workspace_id, debt_id, account_name):
    with conn() as c:
        debt = c.execute("SELECT person, amount, type FROM debts WHERE id = ? AND workspace_id = ?", (debt_id, workspace_id)).fetchone()
        if not debt: return
        person, amount, type = debt
        tx_amount, cat_name, desc = (amount, "Restituzione Prestito", f"Restituzione da {person}") if type == 'lent' else (-amount, "Pagamento Debito", f"Pagamento a {person}")
        add_tx(workspace_id, date.today(), account_name, cat_name, tx_amount, desc)
        c.execute("UPDATE debts SET status = 'settled' WHERE id = ? AND workspace_id = ?", (debt_id, workspace_id))

def delete_debt(workspace_id, debt_id):
    with conn() as c:
        c.execute("DELETE FROM debts WHERE id = ? AND workspace_id = ?", (debt_id, workspace_id))

def add_rule(workspace_id, keyword, category_name):
    with conn() as c:
        cat_id = get_or_create(c, 'categories', workspace_id, category_name, 'expense')
        c.execute("INSERT OR REPLACE INTO rules (workspace_id, keyword, category_id) VALUES (?, ?, ?)", (workspace_id, keyword.lower(), cat_id))

def delete_rule(workspace_id, rule_id):
    with conn() as c: c.execute("DELETE FROM rules WHERE id = ? AND workspace_id = ?", (rule_id, workspace_id))

def get_rules(workspace_id):
    query = "SELECT r.id, r.keyword, c.name FROM rules r JOIN categories c ON r.category_id = c.id WHERE r.workspace_id = ? ORDER BY r.keyword"
    return get_db_data(query, (workspace_id,))

def apply_rules(workspace_id, description):
    if not description: return "Da categorizzare"
    with conn() as c:
        rules = c.execute("SELECT keyword, category_id FROM rules WHERE workspace_id = ? ORDER BY length(keyword) DESC", (workspace_id,)).fetchall()
        for keyword, category_id in rules:
            if keyword in description.lower():
                category_name = c.execute("SELECT name FROM categories WHERE id = ? AND workspace_id = ?", (category_id, workspace_id)).fetchone()
                return category_name[0] if category_name else "Da categorizzare"
    return "Da categorizzare"

def find_best_matching_planned_tx(workspace_id, tx_date, tx_amount, tolerance_days=7, tolerance_percent=0.15):
    tx_date_obj = parse_date(tx_date)
    if not tx_date_obj: return None
    date_minus, date_plus = (tx_date_obj - timedelta(days=tolerance_days)).isoformat(), (tx_date_obj + timedelta(days=tolerance_days)).isoformat()
    amount_tolerance = abs(tx_amount * tolerance_percent)
    amount_min, amount_max = tx_amount - amount_tolerance, tx_amount + amount_tolerance
    query = "SELECT id, plan_date, description, amount FROM planned_transactions WHERE workspace_id = ? AND status = 'planned' AND plan_date BETWEEN ? AND ? AND amount BETWEEN ? AND ? ORDER BY ABS(amount - ?) ASC, ABS(julianday(plan_date) - julianday(?)) ASC LIMIT 1"
    params = (workspace_id, date_minus, date_plus, min(amount_min, amount_max), max(amount_min, amount_max), tx_amount, tx_date_obj.isoformat())
    match = get_db_data(query, params)
    if match: return {"id": match[0][0], "plan_date": match[0][1], "description": match[0][2], "amount": match[0][3]}
    return None

def reconcile_tx(workspace_id, planned_tx_id, new_tx_data):
    with conn() as c:
        try:
            tx_date_obj = parse_date(new_tx_data['date'])
            if not tx_date_obj: return
            cur = c.execute("SELECT type FROM categories WHERE name = ? AND workspace_id = ?", (new_tx_data['category'], workspace_id)); cat_type = cur.fetchone()
            acc_id = get_or_create(c, 'accounts', workspace_id, new_tx_data['account'])
            cat_id = get_or_create(c, 'categories', workspace_id, new_tx_data['category'], type=cat_type[0] if cat_type else 'expense')
            c.execute("INSERT INTO transactions(workspace_id, tx_date, amount, account_id, category_id, description) VALUES(?,?,?,?,?,?)", (workspace_id, tx_date_obj.isoformat(), new_tx_data['amount'], acc_id, cat_id, new_tx_data['description']))
            c.execute("DELETE FROM planned_transactions WHERE id = ? AND workspace_id = ?", (planned_tx_id, workspace_id))
            c.commit()
        except Exception as e:
            c.rollback(); print(f"Errore during la riconciliazione: {e}")

def add_goal(workspace_id, description, amount):
    with conn() as c: c.execute("INSERT INTO goals (workspace_id, description, amount) VALUES (?, ?, ?)", (workspace_id, description, -abs(amount)))

def get_goals(workspace_id, status='pending'):
    return get_db_data("SELECT id, description, amount FROM goals WHERE workspace_id = ? AND status = ? ORDER BY amount ASC", (workspace_id, status))

def delete_goal(workspace_id, goal_id):
    with conn() as c: c.execute("DELETE FROM goals WHERE id = ? AND workspace_id = ?", (goal_id, workspace_id))
