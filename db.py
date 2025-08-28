# db.py
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

DB_PATH = Path("cashflow.db")

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY, 
    name TEXT UNIQUE NOT NULL, 
    opening_balance REAL NOT NULL DEFAULT 0,
    type TEXT NOT NULL DEFAULT 'standard',
    credit_limit REAL,
    statement_day INTEGER
);
CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, type TEXT NOT NULL CHECK(type IN ('income','expense','transfer')));
CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, tx_date TEXT NOT NULL, amount REAL NOT NULL, account_id INTEGER NOT NULL, category_id INTEGER NOT NULL, description TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')), FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE, FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE RESTRICT);
CREATE TABLE IF NOT EXISTS recurring (id INTEGER PRIMARY KEY, name TEXT NOT NULL, start_date TEXT NOT NULL, interval TEXT NOT NULL CHECK(interval IN ('daily','weekly','monthly')), amount REAL NOT NULL, account_id INTEGER NOT NULL, category_id INTEGER NOT NULL, description TEXT, FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE, FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE RESTRICT);
CREATE TABLE IF NOT EXISTS budgets (id INTEGER PRIMARY KEY, year INTEGER NOT NULL, month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12), category_id INTEGER NOT NULL, amount REAL NOT NULL, account_id INTEGER, UNIQUE(year, month, category_id, account_id), FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE, FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS debts (id INTEGER PRIMARY KEY, person TEXT NOT NULL, amount REAL NOT NULL, type TEXT NOT NULL CHECK(type IN ('lent', 'borrowed')), due_date TEXT, status TEXT NOT NULL DEFAULT 'outstanding', created_at TEXT NOT NULL DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS rules (id INTEGER PRIMARY KEY, keyword TEXT UNIQUE NOT NULL, category_id INTEGER NOT NULL, FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS planned_transactions (
    id INTEGER PRIMARY KEY,
    plan_date TEXT NOT NULL,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    category_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned',
    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE,
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS goals (
    id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    amount REAL NOT NULL,
    priority INTEGER DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
"""
DEFAULT_CATEGORIES = [("Stipendio", "income"), ("Interessi", "income"),("Spesa alimentare", "expense"), ("Ristorante e bar", "expense"),("Benzina", "expense"), ("Trasporti", "expense"), ("Bolletta luce", "expense"),("Affitto", "expense"), ("Telefonia/Internet", "expense"),("Tempo libero", "expense"), ("Tasse", "expense"), ("Trasferimento", "transfer"), ("Restituzione Prestito", "income"), ("Pagamento Debito", "expense"), ("Da categorizzare", "expense")]

def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute("PRAGMA journal_mode=WAL;")
    return c

def upgrade_db_for_credit_cards():
    """Aggiunge le colonne necessarie per la gestione delle carte di credito."""
    with conn() as c:
        try:
            # Usiamo PRAGMA per verificare se le colonne esistono già
            cursor = c.execute("PRAGMA table_info(accounts);")
            columns = [row[1] for row in cursor.fetchall()]
            if 'type' not in columns:
                c.execute("ALTER TABLE accounts ADD COLUMN type TEXT NOT NULL DEFAULT 'standard';")
            if 'credit_limit' not in columns:
                c.execute("ALTER TABLE accounts ADD COLUMN credit_limit REAL;")
            if 'statement_day' not in columns:
                c.execute("ALTER TABLE accounts ADD COLUMN statement_day INTEGER;")
            c.commit()
        except sqlite3.OperationalError as e:
            # Questo blocco previene errori se si esegue la funzione più volte
            print(f"Le colonne potrebbero esistere già: {e}")

def init_db():
    with conn() as c:
        c.executescript(SCHEMA)
        # Controlla se l'inizializzazione è già stata fatta
        setup_done = c.execute("SELECT value FROM settings WHERE key = 'initial_setup_done'").fetchone()
        if not setup_done:
            c.executemany("INSERT OR IGNORE INTO categories(name,type) VALUES(?,?)", DEFAULT_CATEGORIES)
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('initial_setup_done', 'true')")
            print("Database initialized with default categories.")
        c.commit()
    # Esegui l'upgrade dello schema per le carte di credito
    upgrade_db_for_credit_cards()

def reset_db():
    with conn() as c:
        cursor = c.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF;")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table_name in tables:
            if table_name[0] not in ["sqlite_sequence", "users", "password_reset_tokens"]:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name[0]};")
        cursor.execute("PRAGMA foreign_keys=ON;")
        c.commit()
    # Dopo il reset, forza la re-inizializzazione
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

def get_or_create(c, table, name, type=None):
    ALLOWED_TABLES = ['accounts', 'categories'];
    if table not in ALLOWED_TABLES: raise ValueError(f"Tabella non consentita: {table}")
    query_select = f"SELECT id FROM {table} WHERE name = ?"; params_select = (name,)
    if table == 'categories' and type:
        query_select += " AND type = ?"; params_select = (name, type)
    cur = c.execute(query_select, params_select)
    row = cur.fetchone()
    if row: return row[0]
    else:
        # Per i conti, non possiamo creare al volo senza tipo, quindi restituisce None
        if table == 'accounts': return None
        query_insert = f"INSERT INTO {table} (name) VALUES (?)"; params_insert = (name,)
        if table == 'categories' and type:
            query_insert = f"INSERT INTO {table} (name, type) VALUES (?, ?)"; params_insert = (name, type)
        cur = c.execute(query_insert, params_insert)
        return cur.lastrowid

# --- TRANSACTIONS ---
def add_tx(tx_date, account_name, category_name, amount, desc=None):
    tx_date_obj = parse_date(tx_date);
    if not tx_date_obj: return
    with conn() as c:
        cur = c.execute("SELECT type FROM categories WHERE name = ?", (category_name,)); cat_type = cur.fetchone()
        acc_id = get_or_create(c, 'accounts', account_name)
        cat_id = get_or_create(c, 'categories', category_name, type=cat_type[0] if cat_type else 'expense')
        c.execute("INSERT INTO transactions(tx_date, amount, account_id, category_id, description) VALUES(?,?,?,?,?)", (tx_date_obj.isoformat(), amount, acc_id, cat_id, desc))

def update_tx(tx_id, new_date, new_account, new_category, new_amount, new_description):
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', new_account)
        cat_id = get_or_create(c, 'categories', new_category)
        c.execute("UPDATE transactions SET tx_date=?, account_id=?, category_id=?, amount=?, description=? WHERE id=?", (parse_date(new_date).isoformat(), acc_id, cat_id, new_amount, new_description, tx_id))

def bulk_update_transactions(transaction_ids, new_category_name=None, new_account_name=None):
    if not transaction_ids or (new_category_name is None and new_account_name is None): return
    with conn() as c:
        set_clauses, params = [], []
        if new_category_name:
            cat_id_result = c.execute("SELECT id FROM categories WHERE name = ?", (new_category_name,)).fetchone()
            if cat_id_result:
                set_clauses.append("category_id = ?"); params.append(cat_id_result[0])
        if new_account_name:
            acc_id_result = c.execute("SELECT id FROM accounts WHERE name = ?", (new_account_name,)).fetchone()
            if acc_id_result:
                set_clauses.append("account_id = ?"); params.append(acc_id_result[0])
        if not set_clauses: return
        query = f"UPDATE transactions SET {', '.join(set_clauses)} WHERE id IN ({','.join('?' for _ in transaction_ids)})"
        params.extend(transaction_ids)
        c.execute(query, tuple(params))

def delete_tx(tx_id):
    with conn() as c: c.execute("DELETE FROM transactions WHERE id=?", (tx_id,))

def bulk_delete_transactions(transaction_ids):
    """Elimina una lista di transazioni in base ai loro ID."""
    if not transaction_ids:
        return 0
    with conn() as c:
        placeholders = ','.join('?' for _ in transaction_ids)
        query = f"DELETE FROM transactions WHERE id IN ({placeholders})"
        cursor = c.execute(query, transaction_ids)
        return cursor.rowcount

def get_all_transactions_raw():
    query = "SELECT t.id, t.tx_date, a.name AS account, c.name AS category, t.amount, COALESCE(t.description,'') as description FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN categories c ON c.id = t.category_id ORDER BY t.tx_date DESC, t.id DESC"
    return get_db_data(query)

def get_transaction_by_id(tx_id):
    query = "SELECT t.id, t.tx_date, a.name AS account, c.name AS category, t.amount, COALESCE(t.description,'') as description FROM transactions t JOIN accounts a ON a.id = t.account_id JOIN categories c ON c.id = t.category_id WHERE t.id = ?"
    result = get_db_data(query, (tx_id,))
    return result[0] if result else None

def get_transactions_in_range(start_date, end_date, account_name=None):
    tx_q = "SELECT tx_date, amount FROM transactions t JOIN accounts a ON a.id = t.account_id WHERE tx_date BETWEEN ? AND ?"
    tx_params = [start_date.isoformat(), end_date.isoformat()]
    if account_name:
        tx_q += " AND a.name = ?"; tx_params.append(account_name)
    return get_db_data(tx_q, tuple(tx_params))

def get_transactions_for_training():
    query = "SELECT T.description, C.name as category FROM transactions T JOIN categories C on T.category_id = C.id WHERE T.description IS NOT NULL AND T.description != ''"
    return get_db_data(query)

# --- ACCOUNTS ---
def add_account(name, balance=0.0, acc_type='standard', limit=None, day=None):
    with conn() as c:
        c.execute("INSERT INTO accounts(name, opening_balance, type, credit_limit, statement_day) VALUES(?, ?, ?, ?, ?)",
                  (name, balance, acc_type, limit, day))

def update_account(old_name, new_name, new_balance, new_type, new_limit, new_day):
    with conn() as c:
        c.execute("UPDATE accounts SET name=?, opening_balance=?, type=?, credit_limit=?, statement_day=? WHERE name=?",
                  (new_name, new_balance, new_type, new_limit, new_day, old_name))

def delete_account(name):
    with conn() as c: c.execute("DELETE FROM accounts WHERE name=?", (name,))

def get_all_accounts(with_details=False):
    if with_details:
        return get_db_data("SELECT id, name, type FROM accounts ORDER BY name ASC")
    return [row[0] for row in get_db_data("SELECT name FROM accounts ORDER BY name ASC")]

def get_account_details_by_name(name):
    query = "SELECT name, opening_balance, type, credit_limit, statement_day FROM accounts WHERE name = ?"
    result = get_db_data(query, (name,))
    return result[0] if result else None

def get_accounts_with_balance():
    query = """
    SELECT 
        a.name,
        a.type,
        a.credit_limit,
        CASE 
            WHEN a.type = 'credit_card' THEN a.credit_limit + COALESCE(SUM(t.amount), 0)
            ELSE a.opening_balance + COALESCE(SUM(t.amount), 0) 
        END as display_balance, -- Saldo per conti standard, Credito residuo per CC
        CASE
            WHEN a.type = 'credit_card' THEN COALESCE(SUM(t.amount), 0)
            ELSE NULL
        END as amount_due -- Saldo da pagare per CC
    FROM accounts a 
    LEFT JOIN transactions t ON a.id = t.account_id 
    GROUP BY a.id, a.name, a.type, a.credit_limit, a.opening_balance 
    ORDER BY a.name
    """
    return get_db_data(query)

# --- CATEGORIES ---
def get_all_categories():
    return [row[0] for row in get_db_data("SELECT name FROM categories ORDER BY name ASC")]

def get_all_categories_with_types():
    return get_db_data("SELECT id, name, type FROM categories ORDER BY type, name ASC")

def add_category(name, type):
    with conn() as c:
        try: c.execute("INSERT INTO categories (name, type) VALUES (?, ?)", (name, type)); return True, None
        except sqlite3.IntegrityError: return False, "Una categoria con questo nome esiste già."

def bulk_add_categories(categories_to_add):
    with conn() as c: c.executemany("INSERT OR IGNORE INTO categories (name, type) VALUES (?, ?)", categories_to_add)

def update_category(category_id, new_name, new_type):
    with conn() as c:
        try: c.execute("UPDATE categories SET name = ?, type = ? WHERE id = ?", (new_name, new_type, category_id)); return True, None
        except sqlite3.IntegrityError: return False, "Una categoria con questo nome esiste già."

def delete_category(category_id):
    with conn() as c:
        try: c.execute("DELETE FROM categories WHERE id = ?", (category_id,)); return True, None
        except sqlite3.IntegrityError: return False, "La categoria è utilizzata da uno o più movimenti e non può essere eliminata."
        
def delete_unused_categories():
    with conn() as c:
        cursor = c.execute("DELETE FROM categories WHERE id NOT IN (SELECT DISTINCT category_id FROM transactions)")
        return cursor.rowcount

# --- SUMMARY & ANALYSIS ---
def get_summary_by_category(start_date, end_date, account_name=None):
    params = [start_date.isoformat(), end_date.isoformat()]
    query = "SELECT c.name, SUM(ABS(t.amount)) AS total FROM transactions t JOIN categories c ON c.id = t.category_id JOIN accounts a ON a.id = t.account_id WHERE t.amount < 0 AND t.tx_date BETWEEN ? AND ?"
    if account_name:
        query += " AND a.name = ?"; params.append(account_name)
    query += " GROUP BY c.name ORDER BY total DESC"
    return get_db_data(query, tuple(params))

def get_monthly_summary(start_date, end_date, account_name=None):
    params = [start_date.isoformat(), end_date.isoformat()]
    query = """
        SELECT strftime('%Y-%m', tx_date) AS month, 
               SUM(CASE WHEN c.type = 'income' THEN t.amount ELSE 0 END) AS income, 
               SUM(CASE WHEN c.type = 'expense' THEN t.amount ELSE 0 END) AS expense 
        FROM transactions t 
        JOIN accounts a ON a.id = t.account_id 
        JOIN categories c ON c.id = t.category_id
        WHERE t.tx_date BETWEEN ? AND ?
    """
    if account_name:
        query += " AND a.name = ?"; params.append(account_name)
    query += " GROUP BY month ORDER BY month"
    return get_db_data(query, tuple(params))

def get_balance_before_date(start_date, account_name=None):
    acc_bal_q = "SELECT COALESCE(SUM(opening_balance), 0) FROM accounts WHERE type = 'standard'"
    acc_bal_params = []
    if account_name:
        acc_bal_q += " AND name = ?"; acc_bal_params.append(account_name)
    initial_balance_acc = get_db_data(acc_bal_q, tuple(acc_bal_params))[0][0]
    
    bal_q = "SELECT COALESCE(SUM(t.amount), 0) FROM transactions t JOIN accounts a ON a.id = t.account_id WHERE t.tx_date < ? AND a.type = 'standard'"
    bal_params = [start_date.isoformat()]
    if account_name:
        bal_q += " AND a.name = ?"; bal_params.append(account_name)
    balance_tx = get_db_data(bal_q, tuple(bal_params))[0][0]
    return initial_balance_acc + balance_tx

def get_data_for_sankey(start_date, end_date, account_name=None):
    query = "SELECT c.name as category, SUM(t.amount) as amount FROM transactions t JOIN categories c ON c.id = t.category_id JOIN accounts a ON a.id = t.account_id WHERE t.tx_date BETWEEN ? AND ? AND c.type != 'transfer' "
    params = [start_date.isoformat(), end_date.isoformat()]
    if account_name and account_name != "Tutti":
        query += " AND a.name = ? "; params.append(account_name)
    query += " GROUP BY c.name"
    return get_db_data(query, tuple(params))
    
def get_net_worth():
    accounts_data = get_accounts_with_balance()
    total_liquidity = sum(row[3] for row in accounts_data if row[1] == 'standard')
    total_cc_debt = sum(row[4] for row in accounts_data if row[1] == 'credit_card')
    total_borrowed = get_db_data("SELECT COALESCE(SUM(amount), 0) FROM debts WHERE type = 'borrowed' AND status = 'outstanding'")[0][0]
    return total_liquidity + total_cc_debt - total_borrowed # total_cc_debt è già negativo

def get_category_trend(category_name, start_date, end_date):
    query = "SELECT strftime('%Y-%m', t.tx_date) as month, SUM(ABS(t.amount)) FROM transactions t JOIN categories c ON t.category_id = c.id WHERE c.name = ? AND t.amount < 0 AND t.tx_date BETWEEN ? AND ? GROUP BY month ORDER BY month ASC"
    params = (category_name, start_date.isoformat(), end_date.isoformat())
    return get_db_data(query, params)

# --- RECURRING & PLANNED ---
def get_recurring_transactions():
    query = "SELECT r.id, r.name, r.start_date, r.interval, r.amount, a.name, c.name, COALESCE(r.description,'') FROM recurring r JOIN accounts a ON a.id = r.account_id JOIN categories c ON c.id = r.category_id ORDER BY r.start_date DESC"
    return get_db_data(query)

def add_recurring(name, start_date, interval, amount, account_name, category_name, description):
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', account_name)
        cat_id = get_or_create(c, 'categories', category_name)
        c.execute("INSERT INTO recurring (name, start_date, interval, amount, account_id, category_id, description) VALUES (?,?,?,?,?,?,?)",
                  (name, parse_date(start_date).isoformat(), interval, amount, acc_id, cat_id, description))

def delete_recurring(recurring_id):
    with conn() as c: c.execute("DELETE FROM recurring WHERE id = ?", (recurring_id,))

def add_planned_tx(plan_date, description, amount, category_name, account_name):
    plan_date_obj = parse_date(plan_date)
    if not plan_date_obj: return
    with conn() as c:
        acc_id = get_or_create(c, 'accounts', account_name)
        cat_id = get_or_create(c, 'categories', category_name)
        c.execute("INSERT INTO planned_transactions(plan_date, description, amount, account_id, category_id) VALUES(?,?,?,?,?)", (plan_date_obj.isoformat(), description, amount, acc_id, cat_id))

def get_all_planned_tx():
    query = "SELECT p.id, p.plan_date, p.description, p.amount, c.name as category, a.name as account FROM planned_transactions p JOIN categories c ON p.category_id = c.id JOIN accounts a ON p.account_id = a.id WHERE p.status = 'planned' ORDER BY p.plan_date ASC"
    return get_db_data(query)

def delete_planned_tx(planned_tx_id):
    with conn() as c: c.execute("DELETE FROM planned_transactions WHERE id = ?", (planned_tx_id,))

def get_future_events(start_date, end_date, account_name=None):
    events = []
    # Eventi pianificati
    planned_query = "SELECT p.plan_date, p.description, p.amount FROM planned_transactions p JOIN accounts a ON p.account_id = a.id WHERE p.plan_date BETWEEN ? AND ? AND a.type = 'standard'"
    params = [start_date.isoformat(), end_date.isoformat()]
    if account_name:
        planned_query += " AND a.name = ?"; params.append(account_name)
    for p_date, desc, amount in get_db_data(planned_query, tuple(params)):
        events.append({'date': parse_date(p_date), 'description': desc, 'amount': amount})

    # Eventi ricorrenti
    rec_query = "SELECT r.start_date, r.interval, r.amount, r.name, r.category_id FROM recurring r JOIN accounts a ON a.id = r.account_id WHERE a.type = 'standard'"
    rec_params = []
    if account_name:
        rec_query += " AND a.name = ?"; rec_params.append(account_name)
    
    with conn() as c:
        for r_start_date_str, interval, amount, name, cat_id in get_db_data(rec_query, tuple(rec_params)):
            curr_date = parse_date(r_start_date_str)
            while curr_date <= end_date:
                if curr_date >= start_date:
                    month_str, tolerance = curr_date.strftime('%Y-%m'), abs(amount * 0.10)
                    existing = c.execute("SELECT 1 FROM transactions WHERE category_id = ? AND strftime('%Y-%m', tx_date) = ? AND amount BETWEEN ? AND ? LIMIT 1",
                                         (cat_id, month_str, amount - tolerance, amount + tolerance)).fetchone()
                    if not existing:
                        events.append({'date': curr_date, 'description': name, 'amount': amount})
                if interval == "daily": curr_date += relativedelta(days=1)
                elif interval == "weekly": curr_date += relativedelta(weeks=1)
                elif interval == "monthly": curr_date += relativedelta(months=1)
                else: break 
    return sorted(events, key=lambda x: x['date'])

# --- BUDGETS, DEBTS, RULES, GOALS, ETC. (invariate) ---
def find_recurring_suggestions():
    tx_query = """
        SELECT t.tx_date, t.amount, COALESCE(t.description, '') as description, 
               c.name as category_name, a.name as account_name, c.type as category_type
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        JOIN accounts a ON t.account_id = a.id
        WHERE t.amount != 0
    """
    df = pd.DataFrame(get_db_data(tx_query), columns=['date', 'amount', 'description', 'category_name', 'account_name', 'category_type'])
    if df.empty: return []

    df['date'] = pd.to_datetime(df['date']); df.sort_values('date', inplace=True)
    
    df['normalized_desc'] = df['description'].str.lower().str.strip()
    df['grouping_desc'] = np.where(df['category_type'] == 'income', '---income_group---', df['normalized_desc'])
    
    income_bucket_divisor = 50
    expense_bucket_divisor = 5
    df['amount_group'] = np.where(
        df['category_type'] == 'income',
        (df['amount'] / income_bucket_divisor).round(),
        (df['amount'] / expense_bucket_divisor).round()
    ).astype(int)

    rec_data = get_recurring_transactions()
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
            
            if grouping_desc == '---income_group---':
                display_desc = category_name
            else:
                desc = group['normalized_desc'].iloc[0]
                display_desc = (desc[0].upper() + desc[1:]) if desc else 'Movimento senza descrizione'

            key = (display_desc.lower().strip(), interval_type, category_name, account_name)
            if key not in existing_recurring:
                suggestions.append((display_desc, avg_amount, interval_type, category_name, account_name, first_date.strftime('%Y-%m-%d')))
    return suggestions

def get_budgets_by_year(year):
    query = "SELECT b.id, b.month, c.name, COALESCE(a.name, 'Tutti i conti') as account_name, b.amount FROM budgets b JOIN categories c ON c.id = b.category_id LEFT JOIN accounts a ON a.id = b.account_id WHERE b.year=? ORDER BY b.month, c.name"
    return get_db_data(query, (year,))

def add_budget(year, month, category_name, account_name, amount):
    with conn() as c:
        cat_id = get_or_create(c, 'categories', category_name, 'expense')
        acc_id = get_or_create(c, 'accounts', account_name) if account_name != 'Tutti i conti' else None
        query = "INSERT INTO budgets (year, month, category_id, account_id, amount) VALUES (?, ?, ?, ?, ?) ON CONFLICT(year, month, category_id, account_id) DO UPDATE SET amount = excluded.amount;"
        c.execute(query, (year, month, cat_id, acc_id, amount))

def delete_budget(budget_id):
    with conn() as c: c.execute("DELETE FROM budgets WHERE id = ?", (budget_id,))

def get_actual_expenses_by_year(year):
    query = "SELECT CAST(strftime('%m', t.tx_date) AS INTEGER) as month, c.name as category_name, a.name as account_name, SUM(t.amount) as total_spent FROM transactions t JOIN categories c ON t.category_id = c.id JOIN accounts a ON t.account_id = a.id WHERE STRFTIME('%Y', t.tx_date) = ? AND t.amount < 0 GROUP BY month, category_name, account_name"
    data = get_db_data(query, (str(year),))
    actuals, totals_by_category = {}, {}
    for month, category, account, total in data: actuals[(month, category, account)] = abs(total)
    for (month, category, _), total in actuals.items(): totals_by_category[(month, category)] = totals_by_category.get((month, category), 0) + total
    for (month, category), total in totals_by_category.items(): actuals[(month, category, "Tutti i conti")] = total
    return actuals

def add_debt(person, amount, type, due_date):
    with conn() as c: c.execute("INSERT INTO debts (person, amount, type, due_date) VALUES (?, ?, ?, ?)", (person, amount, type, parse_date(due_date).isoformat()))

def get_debts(status='outstanding'):
    return get_db_data("SELECT * FROM debts WHERE status = ? ORDER BY due_date ASC", (status,))

def settle_debt(debt_id, account_name):
    with conn() as c:
        debt = c.execute("SELECT person, amount, type FROM debts WHERE id = ?", (debt_id,)).fetchone()
        if not debt: return
        person, amount, type = debt
        tx_amount, cat_name, desc = (amount, "Restituzione Prestito", f"Restituzione da {person}") if type == 'lent' else (-amount, "Pagamento Debito", f"Pagamento a {person}")
        add_tx(date.today(), account_name, cat_name, tx_amount, desc)
        c.execute("UPDATE debts SET status = 'settled' WHERE id = ?", (debt_id,))

def add_rule(keyword, category_name):
    with conn() as c:
        cat_id = get_or_create(c, 'categories', category_name, 'expense')
        c.execute("INSERT OR REPLACE INTO rules (keyword, category_id) VALUES (?, ?)", (keyword.lower(), cat_id))

def delete_rule(rule_id):
    with conn() as c: c.execute("DELETE FROM rules WHERE id = ?", (rule_id,))

def get_rules():
    query = "SELECT r.id, r.keyword, c.name FROM rules r JOIN categories c ON r.category_id = c.id ORDER BY r.keyword"
    return get_db_data(query)

def apply_rules(description):
    if not description: return "Da categorizzare"
    with conn() as c:
        rules = c.execute("SELECT keyword, category_id FROM rules ORDER BY length(keyword) DESC").fetchall()
        for keyword, category_id in rules:
            if keyword in description.lower():
                category_name = c.execute("SELECT name FROM categories WHERE id = ?", (category_id,)).fetchone()
                return category_name[0] if category_name else "Da categorizzare"
    return "Da categorizzare"

def find_best_matching_planned_tx(tx_date, tx_amount, tolerance_days=7, tolerance_percent=0.15):
    tx_date_obj = parse_date(tx_date)
    if not tx_date_obj: return None
    date_minus, date_plus = (tx_date_obj - timedelta(days=tolerance_days)).isoformat(), (tx_date_obj + timedelta(days=tolerance_days)).isoformat()
    amount_tolerance = abs(tx_amount * tolerance_percent)
    amount_min, amount_max = tx_amount - amount_tolerance, tx_amount + amount_tolerance
    query = "SELECT id, plan_date, description, amount FROM planned_transactions WHERE status = 'planned' AND plan_date BETWEEN ? AND ? AND amount BETWEEN ? AND ? ORDER BY ABS(amount - ?) ASC, ABS(julianday(plan_date) - julianday(?)) ASC LIMIT 1"
    params = (date_minus, date_plus, min(amount_min, amount_max), max(amount_min, amount_max), tx_amount, tx_date_obj.isoformat())
    match = get_db_data(query, params)
    if match: return {"id": match[0][0], "plan_date": match[0][1], "description": match[0][2], "amount": match[0][3]}
    return None

def reconcile_tx(planned_tx_id, new_tx_data):
    with conn() as c:
        try:
            tx_date_obj = parse_date(new_tx_data['date'])
            if not tx_date_obj: return
            cur = c.execute("SELECT type FROM categories WHERE name = ?", (new_tx_data['category'],)); cat_type = cur.fetchone()
            acc_id = get_or_create(c, 'accounts', new_tx_data['account'])
            cat_id = get_or_create(c, 'categories', new_tx_data['category'], type=cat_type[0] if cat_type else 'expense')
            c.execute("INSERT INTO transactions(tx_date, amount, account_id, category_id, description) VALUES(?,?,?,?,?)", (tx_date_obj.isoformat(), new_tx_data['amount'], acc_id, cat_id, new_tx_data['description']))
            c.execute("DELETE FROM planned_transactions WHERE id = ?", (planned_tx_id,))
            c.commit()
        except Exception as e:
            c.rollback(); print(f"Errore durante la riconciliazione: {e}")

def add_goal(description, amount):
    with conn() as c: c.execute("INSERT INTO goals (description, amount) VALUES (?, ?)", (description, -abs(amount)))

def get_goals(status='pending'):
    return get_db_data("SELECT id, description, amount FROM goals WHERE status = ? ORDER BY amount ASC", (status,))

def delete_goal(goal_id):
    with conn() as c: c.execute("DELETE FROM goals WHERE id = ?", (goal_id,))
