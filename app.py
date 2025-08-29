# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta
import calendar
import numpy as np
import os # <-- 1. IMPORTATO OS
from pathlib import Path

# Importa le funzioni dal file db.py
try:
    from db import (
        init_db, conn, add_tx, parse_date, DB_PATH, reset_db,
        add_account, delete_account, update_account, get_account_details_by_name,
        get_db_data,
        delete_tx, update_tx, get_all_categories, get_all_accounts,
        get_accounts_with_balance, get_all_transactions_raw, get_transaction_by_id,
        get_summary_by_category, get_monthly_summary,
        get_recurring_transactions, get_budgets_by_year, add_recurring, delete_recurring,
        add_budget, delete_budget, get_actual_expenses_by_year,
        get_balance_before_date, get_transactions_in_range,
        add_debt, get_debts, settle_debt,
        add_rule, delete_rule, get_rules, apply_rules,
        get_data_for_sankey,
        get_all_categories_with_types, add_category, update_category, delete_category,
        get_net_worth, get_category_trend, get_transactions_for_training,
        bulk_update_transactions, bulk_add_categories, delete_unused_categories,
        add_planned_tx, get_all_planned_tx, delete_planned_tx,
        get_future_events, find_recurring_suggestions,
        find_best_matching_planned_tx, reconcile_tx,
        add_goal, get_goals, delete_goal, bulk_delete_transactions
    )
    from ml_utils import train_model, predict_category, predict_single
    import auth
except ImportError as e:
    st.error(f"ERRORE CRITICO: Assicurati di avere tutti i file necessari ('db.py', 'ml_utils.py', 'auth.py') e le librerie installate. Dettaglio: {e}")
    st.stop()

# --- DEFINIZIONE PERCORSO CSS ---
# 2. Definisce il percorso del CSS in modo ancora più robusto per Streamlit Cloud
# Usa un percorso assoluto partendo dalla directory dello script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CSS_FILE = os.path.join(SCRIPT_DIR, "styles", "main.css")


# --- FUNZIONE PER CARICARE IL CSS ---
def load_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"ATTENZIONE: Il file CSS non è stato trovato al percorso: '{file_name}'. La grafica potrebbe non essere corretta.")
        st.warning("Controlla che la struttura del repository su GitHub sia: /styles/main.css")


# --- INIZIALIZZAZIONI SESSION STATE ---
def init_session_state():
    defaults = {
        'tx_to_edit': None,
        'suggested_category_index': 0,
        'df_import_review': None,
        'uploaded_file_id': None,
        'planner_results': None,
        'authenticated': False,
        'username': None,
        'is_admin': False,
        'login_page': 'Login',
        'reset_step': 1,
        'reset_username': ''
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# --- DIALOGO DI MODIFICA ---
@st.dialog("Gestisci Movimento", width="large")
def manage_transaction_dialog():
    tx_id = st.session_state.tx_to_edit
    tx_data = get_transaction_by_id(tx_id)
    if not tx_data:
        st.error("Movimento non trovato."); return

    st.subheader("Modifica Dati")
    with st.form("edit_dialog_form"):
        all_accounts = get_all_accounts(); all_categories = get_all_categories()
        _, tx_date_str, tx_account, tx_category, tx_amount, tx_description = tx_data
        
        c1, c2 = st.columns(2)
        edit_date = c1.date_input("Data", value=parse_date(tx_date_str), format="DD/MM/YYYY")
        edit_amount = c2.number_input("Importo", value=float(tx_amount))
        
        edit_description = st.text_input("Descrizione", value=tx_description)
        
        c3, c4 = st.columns(2)
        acc_index = all_accounts.index(tx_account) if tx_account in all_accounts else 0
        cat_index = all_categories.index(tx_category) if tx_category in all_categories else 0
        edit_account = c3.selectbox("Conto", options=all_accounts, index=acc_index)
        edit_category = c4.selectbox("Categoria", options=all_categories, index=cat_index)

        if st.form_submit_button("Salva Modifiche", use_container_width=True, type="primary"):
            update_tx(tx_id, edit_date, edit_account, edit_category, edit_amount, edit_description)
            st.toast("Movimento aggiornato!", icon="✅")
            st.session_state.tx_to_edit = None
            st.cache_data.clear()
            st.rerun()

    st.markdown("---")
    c_del, c_ann = st.columns(2)
    with c_del:
        if st.button("Elimina Definitivamente", use_container_width=True):
            delete_tx(tx_id)
            st.toast("Movimento eliminato!", icon="🗑️")
            st.session_state.tx_to_edit = None
            st.cache_data.clear()
            st.rerun()
    with c_ann:
        if st.button("Annulla", use_container_width=True):
            st.session_state.tx_to_edit = None
            st.rerun()

# --- VISTA PRINCIPALE ---
def show_main_dashboard():
    st.title("💸 Cashflow Pro")
    st.markdown(f"Benvenuto, **{st.session_state.username}**! Ecco la panoramica delle tue finanze.")
    
    if st.session_state.get('tx_to_edit'):
        manage_transaction_dialog()

    tabs = st.tabs([
        "📊 Dashboard", "📄 Movimenti", "🗓️ Pianificati", "💡 Pianificatore", 
        "🏦 Conti", "🤝 Debiti", "🔁 Ricorrenze", "💸 Budget", 
        "📈 Forecast", "⚙️ Impostazioni"
    ])
    
    with tabs[0]: # Dashboard
        
        accounts_data = get_accounts_with_balance()
        df_balances = pd.DataFrame(accounts_data, columns=["Nome", "Tipo", "Plafond", "Saldo/Residuo", "Da Pagare"])

        st.subheader("Panoramica Attuale")
        m1, m2 = st.columns(2)
        with m1:
            total_liquidity = df_balances[df_balances['Tipo'] == 'standard']["Saldo/Residuo"].sum()
            st.metric(label="Liquidità Totale (Conti Standard)", value=f"€ {total_liquidity:,.2f}")

        with m2:
            net_worth = get_net_worth()
            st.metric(label="Patrimonio Netto", value=f"€ {net_worth:,.2f}", help="Liquidità + Crediti - Debiti (incluse carte di credito)")

        st.markdown("---")
        
        st.subheader("Dettaglio Saldi per Conto")
        if not df_balances.empty:
            num_accounts = len(df_balances)
            max_cols = 4
            num_cols = min(num_accounts, max_cols)
            
            cols = st.columns(num_cols)
            for i, row in df_balances.iterrows():
                col_index = i % max_cols
                with cols[col_index]:
                    if row['Tipo'] == 'credit_card':
                        st.metric(label=f"💳 {row['Nome']} (Da pagare)", value=f"€ {abs(row['Da Pagare']):,.2f}")
                        st.caption(f"Credito Residuo: € {row['Saldo/Residuo']:,.2f}")
                    else:
                        st.metric(label=f"🏦 {row['Nome']}", value=f"€ {row['Saldo/Residuo']:,.2f}")
        else:
            st.info("Nessun conto trovato. Aggiungine uno nel tab 'Conti' per iniziare.")
        
        st.markdown("---")

        st.subheader("Analisi Visiva")
        
        with st.container(border=True):
            c_filter1, c_filter2 = st.columns([1,2])
            with c_filter1:
                accounts_list = ["Tutti"] + get_all_accounts()
                selected_account = st.selectbox("Filtra per Conto", accounts_list)
            with c_filter2:
                date_range = st.date_input("Filtra per Intervallo di Date", value=(date.today() - timedelta(days=30), date.today()), format="DD/MM/YYYY")
            
            if len(date_range) != 2: st.stop()
            start_date, end_date = date_range

            account_param = None if selected_account == "Tutti" else selected_account
        
        adv_tabs = st.tabs(["Flusso Mensile", "Diagramma di Sankey", "Andamento Cumulativo", "Treemap Spese", "Andamento Categorie"])

        with adv_tabs[0]:
            monthly_data = get_monthly_summary(start_date, end_date, account_param)
            if monthly_data:
                df_monthly = pd.DataFrame(monthly_data, columns=["Mese", "Entrate", "Uscite"]); df_monthly['Uscite'] = df_monthly['Uscite'].abs()
                fig_bar = px.bar(df_monthly, x="Mese", y=["Entrate", "Uscite"], barmode='group', title="Entrate vs Uscite nel Periodo")
                st.plotly_chart(fig_bar, use_container_width=True)
            else: st.info("Nessun dato di flusso cassa per il periodo selezionato.")

        with adv_tabs[1]:
            sankey_data = get_data_for_sankey(start_date, end_date, account_param)
            
            if sankey_data:
                df_sankey = pd.DataFrame(sankey_data, columns=["category", "amount"])
                income_df = df_sankey[df_sankey['amount'] > 0]
                expense_df = df_sankey[df_sankey['amount'] < 0].copy()
                expense_df['amount'] = expense_df['amount'].abs()

                if not income_df.empty and not expense_df.empty:
                    sources = list(income_df['category'])
                    targets = list(expense_df['category'])
                    all_nodes = list(set(sources + ["Patrimonio"] + targets))
                    
                    color_palette = px.colors.qualitative.Plotly
                    color_map = {node: color_palette[i % len(color_palette)] for i, node in enumerate(all_nodes)}

                    truncated_labels = [label if len(label) < 25 else label[:22] + '...' for label in all_nodes]
                    
                    links, link_colors = [], []
                    
                    for _, row in income_df.iterrows():
                        source_index = all_nodes.index(row['category'])
                        target_index = all_nodes.index("Patrimonio")
                        links.append({"source": source_index, "target": target_index, "value": row['amount']})
                        link_colors.append(color_map[row['category']].replace('rgb', 'rgba').replace(')', ', 0.4)'))

                    for _, row in expense_df.iterrows():
                        source_index = all_nodes.index("Patrimonio")
                        target_index = all_nodes.index(row['category'])
                        links.append({"source": source_index, "target": target_index, "value": row['amount']})
                        link_colors.append(color_map[row['category']].replace('rgb', 'rgba').replace(')', ', 0.4)'))

                    fig_sankey = go.Figure(data=[go.Sankey(
                        node = dict(pad=15, thickness=20, line=dict(color="black", width=0.5), label=truncated_labels, color=[color_map[node] for node in all_nodes], customdata=all_nodes, hovertemplate='%{customdata} <extra></extra>'),
                        link = dict(source=[l['source'] for l in links], target=[l['target'] for l in links], value=[l['value'] for l in links], color=link_colors),
                        textfont=dict(color="white", size=12)
                    )])

                    fig_sankey.update_layout(title_text="Flusso dalle Entrate alle Uscite", font_size=12, height=600, margin=dict(l=20, r=20, t=50, b=20))
                    st.plotly_chart(fig_sankey, use_container_width=True)
                else:
                    st.info("Dati insufficienti (entrate e/o uscite mancanti) per generare il diagramma di Sankey.")
            else:
                st.info("Nessun dato per generare il diagramma di Sankey.")

        with adv_tabs[2]:
            initial_balance = get_balance_before_date(start_date, account_param)
            tx_in_range = get_transactions_in_range(start_date, end_date, account_param)
            if tx_in_range:
                df_cum = pd.DataFrame(tx_in_range, columns=["Data", "Importo"])
                df_cum["Data"] = pd.to_datetime(df_cum["Data"])
                df_cum = df_cum.sort_values(by="Data")
                df_cum['Saldo'] = df_cum['Importo'].cumsum() + initial_balance
                fig_cum = px.line(df_cum, x="Data", y="Saldo", title="Evoluzione del Saldo nel Tempo", markers=True)
                st.plotly_chart(fig_cum, use_container_width=True)
            else:
                st.info("Nessun movimento nel periodo per calcolare l'andamento.")

        with adv_tabs[3]:
            category_data = get_summary_by_category(start_date, end_date, account_param)
            if category_data:
                df_category = pd.DataFrame(category_data, columns=["Categoria", "Totale Spese"])
                df_category = df_category[df_category['Totale Spese'] > 0]
                fig_tree = px.treemap(df_category, path=[px.Constant("Tutte le Spese"), 'Categoria'], values='Totale Spese', title="Ripartizione Spese per Categoria")
                st.plotly_chart(fig_tree, use_container_width=True)
            else:
                st.info("Nessuna spesa nel periodo selezionato.")

        with adv_tabs[4]:
            st.subheader("Analisi Andamento per Categoria di Spesa")
            
            all_cats_with_types = get_all_categories_with_types()
            expense_categories = [cat[1] for cat in all_cats_with_types if cat[2] == 'expense']

            if not expense_categories:
                st.info("Nessuna categoria di spesa trovata.")
            else:
                selected_cat = st.selectbox("Seleziona una categoria da analizzare", options=expense_categories)
                
                trend_data = get_category_trend(selected_cat, start_date, end_date)
                
                if trend_data:
                    df_trend = pd.DataFrame(trend_data, columns=["Mese", "Spesa"])
                    fig_trend = px.bar(df_trend, x="Mese", y="Spesa", title=f"Andamento Spesa per '{selected_cat}'", text_auto='.2s')
                    fig_trend.update_traces(textangle=0, textposition="outside")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.info(f"Nessuna spesa registrata per la categoria '{selected_cat}' nel periodo selezionato.")
    
    with tabs[1]: # Movimenti
        st.header("Gestione Movimenti")
        with st.expander("➕ Aggiungi un nuovo movimento"):
            with st.form("add_tx_form"):
                accounts = get_all_accounts(); categories = get_all_categories()
                
                c1,c2 = st.columns([3,1])
                tx_description = c1.text_input("Descrizione", key='add_desc')
                
                if c2.form_submit_button("Suggerisci Categoria 💡"):
                    suggested_category = apply_rules(tx_description)
                    st.session_state.suggested_category_index = categories.index(suggested_category) if suggested_category in categories else 0
                    st.rerun()

                c3,c4,c5 = st.columns(3)
                with c3:
                    tx_date = st.date_input("Data", date.today(), key='add_date', format="DD/MM/YYYY")
                    tx_account = st.selectbox("Conto", accounts, key='add_acc')
                
                with c4:
                    tx_amount = st.number_input("Importo", value=0.00, key='add_amount', step=0.01, format="%.2f", min_value=0.0)
                    tx_category = st.selectbox("Categoria", categories, key='add_cat', index=st.session_state.suggested_category_index)
                
                with c5:
                    tx_type = st.radio("Tipo", ["Uscita", "Entrata"], key='add_type', horizontal=True)

                if st.form_submit_button("Salva Movimento", type="primary"):
                    if not accounts or not categories:
                        st.error("Prima di aggiungere un movimento, crea almeno un conto e una categoria nelle Impostazioni.")
                    elif tx_account and tx_category:
                        final_amount = abs(tx_amount) * (-1 if tx_type == "Uscita" else 1)
                        st.session_state.suggested_category_index = 0
                        add_tx(tx_date, tx_account, tx_category, final_amount, tx_description)
                        st.toast("Movimento salvato!", icon="✅"); st.cache_data.clear(); st.rerun()
                    else: 
                        st.error("Seleziona un conto e una categoria.")

        st.markdown("---"); st.subheader("Lista e Filtri Movimenti")
        raw_data = get_all_transactions_raw()
        df_tx = pd.DataFrame(raw_data, columns=["id", "Data", "Conto", "Categoria", "Importo", "Descrizione"])
        if not df_tx.empty: df_tx['Data'] = pd.to_datetime(df_tx['Data']).dt.date
        with st.expander("🔎 Filtri Avanzati", expanded=False):
            c1, c2 = st.columns(2)
            options_c = list(df_tx['Conto'].unique()) if not df_tx.empty else []
            options_cat = list(df_tx['Categoria'].unique()) if not df_tx.empty else []
            sel_accounts = c1.multiselect("Filtra per Conto", options=options_c)
            sel_categories = c2.multiselect("Filtra per Categoria", options=options_cat)
            search_desc = st.text_input("Filtra per Descrizione")
            sel_dates = st.date_input("Filtra per intervallo", value=(), format="DD/MM/YYYY")
            min_a, max_a = (0.0, 0.0)
            if not df_tx.empty and df_tx['Importo'].nunique() > 1:
                min_v, max_v = float(df_tx['Importo'].min()), float(df_tx['Importo'].max())
                min_a, max_a = st.slider("Filtra per Importo", min_v, max_v, (min_v, max_v))
        
        filtered_df = df_tx.copy()
        if sel_accounts: filtered_df = filtered_df[filtered_df['Conto'].isin(sel_accounts)]
        if sel_categories: filtered_df = filtered_df[filtered_df['Categoria'].isin(sel_categories)]
        if search_desc: filtered_df = filtered_df[filtered_df['Descrizione'].str.contains(search_desc, case=False, na=False)]
        if not df_tx.empty and ('min_v' in locals() and (min_a != min_v or max_a != max_v)):
             filtered_df = filtered_df[filtered_df['Importo'].between(min_a, max_a)]
        if len(sel_dates) == 2:
            start_filter, end_filter = sel_dates
            filtered_df = filtered_df[filtered_df['Data'].between(start_filter, end_filter)]

        filtered_df.reset_index(drop=True, inplace=True)

        if not filtered_df.empty:
            csv = filtered_df.drop(columns=['id']).to_csv(index=False).encode('utf-8')
            st.download_button("Esporta Vista in CSV 💾", csv, f"movimenti_{date.today():%Y-%m-%d}.csv", 'text/csv')
            
            df_display = filtered_df.copy()
            df_display.insert(0, "Modifica", False)
            df_display['Data'] = df_display['Data'].apply(lambda x: x.strftime('%d/%m/%Y'))
            df_display['Importo'] = df_display['Importo'].apply(lambda x: f"€ {x:,.2f}")
            
            edited_df = st.data_editor(
                df_display.drop(columns=['id']), 
                hide_index=True, use_container_width=True, key=f"editor_{len(filtered_df)}_{len(df_tx)}", 
                column_config={"Modifica": st.column_config.CheckboxColumn(required=True)},
                disabled=df_display.columns.drop(["id", "Modifica"])
            )
            
            selected_rows = edited_df[edited_df["Modifica"]]
            if not selected_rows.empty:
                with st.expander(f"🔧 Azioni per {len(selected_rows)} movimenti selezionati", expanded=True):
                    
                    if len(selected_rows) == 1:
                        selected_id = int(filtered_df.loc[selected_rows.index[0], "id"])
                        if st.button(f"Gestisci in dettaglio il movimento (ID: {selected_id})"):
                            st.session_state.tx_to_edit = selected_id
                            st.rerun()
                        st.markdown("---")

                    st.write("**Azioni Massive**")
                    c1, c2, c3 = st.columns(3)
                    
                    placeholder = "-- Non modificare --"
                    new_cat = c1.selectbox("Imposta nuova categoria", [placeholder] + get_all_categories(), key="bulk_cat")
                    new_acc = c2.selectbox("Imposta nuovo conto", [placeholder] + get_all_accounts(), key="bulk_acc")

                    if c3.button("Applica Modifiche", type="primary"):
                        ids_to_update = filtered_df.loc[selected_rows.index, "id"].tolist()
                        final_new_cat = new_cat if new_cat != placeholder else None
                        final_new_acc = new_acc if new_acc != placeholder else None
                        if final_new_cat or final_new_acc:
                            bulk_update_transactions(ids_to_update, new_category_name=final_new_cat, new_account_name=final_new_acc)
                            st.toast(f"{len(ids_to_update)} movimenti aggiornati!", icon="✨")
                            st.cache_data.clear(); st.rerun()
                        else:
                            st.warning("Nessuna modifica selezionata.")
                    
                    if st.button("🗑️ Elimina Selezionati"):
                        ids_to_delete = filtered_df.loc[selected_rows.index, "id"].tolist()
                        deleted_count = bulk_delete_transactions(ids_to_delete)
                        st.toast(f"{deleted_count} movimenti eliminati con successo!", icon="🗑️")
                        st.cache_data.clear()
                        st.rerun()

        else: 
            st.info("Nessun movimento trovato con i filtri applicati.")

    with tabs[2]: # Pianificati
        st.header("🗓️ Movimenti Pianificati")
        st.info("In questa sezione puoi inserire entrate o uscite future che non sono ancora avvenute. Questi movimenti verranno usati per migliorare le previsioni del Forecast.")
        
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.subheader("➕ Aggiungi Evento Futuro")
            with st.form("add_planned_tx_form", clear_on_submit=True):
                accounts, categories = get_all_accounts(), get_all_categories()
                plan_description = st.text_input("Descrizione (es. Tasse, Bonus)")
                plan_date = st.date_input("Data Prevista", min_value=date.today(), format="DD/MM/YYYY")
                plan_amount = st.number_input("Importo Previsto", value=0.00, step=0.01, min_value=0.0)
                plan_type = st.radio("Tipo", ["Uscita", "Entrata"], horizontal=True)
                plan_account = st.selectbox("Conto Previsto", accounts)
                plan_category = st.selectbox("Categoria Prevista", categories)
                
                if st.form_submit_button("Salva Evento Pianificato", type="primary"):
                    if plan_description and plan_account and plan_category:
                        final_amount = abs(plan_amount) * (-1 if plan_type == "Uscita" else 1)
                        add_planned_tx(plan_date, plan_description, final_amount, plan_category, plan_account)
                        st.toast("Movimento pianificato salvato!", icon="✅")
                        st.cache_data.clear(); st.rerun()
                    else:
                        st.warning("Tutti i campi sono obbligatori.")
        
        with c2:
            st.subheader("📋 Lista Eventi Futuri")
            planned_txs = get_all_planned_tx()
            if planned_txs:
                df_planned = pd.DataFrame(planned_txs, columns=["id", "Data", "Descrizione", "Importo", "Categoria", "Conto"])
                df_planned['Data'] = pd.to_datetime(df_planned['Data']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_planned.drop(columns=['id']), use_container_width=True, hide_index=True)

                st.markdown("---")
                st.subheader("🗑️ Elimina Evento Pianificato")
                tx_to_delete_id = st.selectbox(
                    "Seleziona un evento da eliminare", 
                    options=df_planned['id'],
                    format_func=lambda x: f"{df_planned.loc[df_planned['id'] == x, 'Data'].iloc[0]} - {df_planned.loc[df_planned['id'] == x, 'Descrizione'].iloc[0]} (€ {df_planned.loc[df_planned['id'] == x, 'Importo'].iloc[0]:.2f})"
                )
                if st.button("Elimina Evento Selezionato", type="primary"):
                    delete_planned_tx(tx_to_delete_id)
                    st.toast("Evento pianificato eliminato!", icon="🗑️")
                    st.cache_data.clear(); st.rerun()
            else:
                st.info("Nessun movimento pianificato inserito.")
    
    with tabs[3]: # Pianificatore Spese
        st.header("💡 Assistente Pianificazione Spese")
        st.info("Aggiungi i tuoi obiettivi di spesa futuri. L'assistente analizzerà il tuo forecast e ti suggerirà quando potrai affrontare ogni spesa senza scendere sotto il tuo saldo di sicurezza.")

        c1, c2 = st.columns([1, 2])

        with c1:
            st.subheader("🎯 I Tuoi Obiettivi di Spesa")
            with st.form("add_goal_form", clear_on_submit=True):
                goal_desc = st.text_input("Descrizione Obiettivo (es. Nuovo Telefono)")
                goal_amount = st.number_input("Costo Previsto (€)", min_value=0.01, step=50.0, format="%.2f")
                if st.form_submit_button("Aggiungi Obiettivo", type="primary"):
                    if goal_desc and goal_amount:
                        add_goal(goal_desc, goal_amount)
                        st.toast("Obiettivo aggiunto!", icon="🎯"); st.rerun()
            
            st.markdown("---")
            pending_goals = get_goals()
            if pending_goals:
                st.write("**Obiettivi in attesa:**")
                for goal_id, desc, amount in pending_goals:
                    cols = st.columns([4, 1])
                    cols[0].write(f"- {desc} (**{abs(amount):,.2f} €**)")
                    if cols[1].button("🗑️", key=f"del_goal_{goal_id}"):
                        delete_goal(goal_id); st.rerun()
            else:
                st.write("Nessun obiettivo di spesa in attesa.")

        with c2:
            st.subheader("⚙️ Imposta e Avvia Analisi")
            if not pending_goals:
                st.warning("Aggiungi almeno un obiettivo di spesa per avviare l'analisi.")
            else:
                with st.container(border=True):
                    standard_accounts = [acc[1] for acc in get_all_accounts(with_details=True) if acc[2] == 'standard']
                    accounts = ["Tutti"] + standard_accounts
                    planner_account = st.selectbox("Analizza saldo su:", accounts, key="planner_acc")
                    safety_balance = st.number_input("Saldo minimo di sicurezza (€)", min_value=0.0, value=500.0, step=100.0)
                    planner_horizon = st.slider("Orizzonte di pianificazione (Mesi)", 1, 36, 12)

                    if st.button("Trova le date migliori per le mie spese 🚀", type="primary", use_container_width=True):
                        with st.spinner("L'assistente sta calcolando il piano migliore per te..."):
                            start_date, end_date = date.today(), date.today() + relativedelta(months=planner_horizon)
                            account_param = None if planner_account == "Tutti" else planner_account
                            initial_balance = get_balance_before_date(start_date, account_param)
                            future_events = get_future_events(start_date, end_date, account_param)

                            df_planner = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='D'), columns=['Date']).set_index('Date')
                            df_planner['Balance'] = 0.0

                            daily_deltas = defaultdict(float)
                            for event in future_events: daily_deltas[pd.to_datetime(event['date'])] += event['amount']
                            for dt, delta in daily_deltas.items():
                                if dt in df_planner.index: df_planner.loc[dt, 'Balance'] = delta
                            df_planner['Balance'] = df_planner['Balance'].cumsum() + initial_balance
                            
                            results = []
                            for goal_id, goal_desc, goal_amount in get_goals():
                                best_date = None
                                for day in df_planner.index:
                                    temp_forecast = df_planner['Balance'].copy()
                                    temp_forecast.loc[day:] += goal_amount 
                                    if temp_forecast.loc[day:].min() >= safety_balance:
                                        best_date = day.date()
                                        df_planner['Balance'] = temp_forecast
                                        break
                                results.append({
                                    "Obiettivo": goal_desc, "Costo": f"{abs(goal_amount):,.2f} €",
                                    "Data Suggerita": best_date.strftime('%d/%m/%Y') if best_date else "Non fattibile",
                                    "Stato": "✅ Fattibile" if best_date else "⚠️ Data non trovata"
                                })
                            st.session_state.planner_results = results
                        st.rerun()
        
        if st.session_state.planner_results is not None:
            st.markdown("---")
            st.subheader("📋 Il Tuo Piano d'Azione")
            st.dataframe(pd.DataFrame(st.session_state.planner_results), use_container_width=True, hide_index=True)
            if st.button("Pulisci Risultati"):
                st.session_state.planner_results = None; st.rerun()

    with tabs[4]: # Conti
        st.header("Gestione Conti")
        
        accounts_data = get_accounts_with_balance()
        df_balances = pd.DataFrame(accounts_data, columns=["Nome", "Tipo", "Plafond", "Saldo/Residuo", "Da Pagare"])
        
        st.subheader("Lista dei Conti")
        # Formattazione per la visualizzazione
        df_display = df_balances.copy()
        df_display['Tipo'] = df_display['Tipo'].map({'standard': '🏦 Standard', 'credit_card': '💳 Carta di Credito'})
        df_display['Info'] = df_display.apply(
            lambda row: f"Plafond: € {row['Plafond']:,.2f}" if row['Tipo'] == '💳 Carta di Credito' else '', axis=1
        )
        df_display['Saldo'] = df_display.apply(
            lambda row: f"€ {row['Saldo/Residuo']:,.2f}" if row['Tipo'] == '🏦 Standard' else f"€ {abs(row['Da Pagare']):,.2f} (da pagare)", axis=1
        )
        st.dataframe(df_display[['Nome', 'Tipo', 'Saldo', 'Info']], use_container_width=True, hide_index=True)
        
        st.markdown("---")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Aggiungi / Modifica Conto")
            
            all_accounts_list = get_all_accounts()
            account_to_manage = st.selectbox("Seleziona un conto da modificare o scegli 'Nuovo Conto'", ["Nuovo Conto"] + all_accounts_list)
            
            is_new_account = account_to_manage == "Nuovo Conto"
            current_details = None if is_new_account else get_account_details_by_name(account_to_manage)
            
            with st.form(f"manage_account_form_{account_to_manage}"):
                st.write(f"**Dettagli per: {account_to_manage}**")
                
                new_name = st.text_input("Nome Conto", value="" if is_new_account else current_details[0])
                acc_type = st.radio("Tipo di Conto", ["standard", "credit_card"], 
                                    index=0 if is_new_account or current_details[2] == 'standard' else 1,
                                    format_func=lambda x: "Standard" if x == "standard" else "Carta di Credito", horizontal=True)
                
                if acc_type == 'standard':
                    opening_balance = st.number_input("Saldo Iniziale", value=0.0 if is_new_account else current_details[1])
                    credit_limit, statement_day = None, None
                else:
                    credit_limit = st.number_input("Plafond / Limite di Credito (€)", min_value=0.0, value=1500.0 if is_new_account else (current_details[3] or 1500.0))
                    statement_day = st.number_input("Giorno chiusura estratto conto (1-31)", min_value=1, max_value=31, value=25 if is_new_account else (current_details[4] or 25))
                    opening_balance = 0.0

                if st.form_submit_button("Salva Conto", type="primary"):
                    if new_name:
                        if is_new_account:
                            add_account(new_name, opening_balance, acc_type, credit_limit, statement_day)
                            st.success(f"Conto '{new_name}' aggiunto!")
                        else:
                            update_account(account_to_manage, new_name, opening_balance, acc_type, credit_limit, statement_day)
                            st.success(f"Conto '{account_to_manage}' aggiornato!")
                        st.cache_data.clear(); st.rerun()
                    else:
                        st.warning("Il nome del conto è obbligatorio.")

            if not is_new_account:
                st.warning(f"Stai per eliminare il conto '{account_to_manage}' e tutte le sue transazioni. Sei sicuro?")
                if st.button("Sì, Confermo Eliminazione", key=f"delete_{account_to_manage}"):
                    delete_account(account_to_manage)
                    st.success(f"Conto '{account_to_manage}' eliminato.")
                    st.cache_data.clear(); st.rerun()

        with c2:
            st.subheader("Paga Estratto Conto Carta")
            credit_cards = [acc[1] for acc in get_all_accounts(with_details=True) if acc[2] == 'credit_card']
            standard_accounts = [acc[1] for acc in get_all_accounts(with_details=True) if acc[2] == 'standard']
            
            if not credit_cards or not standard_accounts:
                st.info("Per pagare una carta di credito, devi avere almeno una carta e un conto standard.")
            else:
                with st.form("pay_cc_bill_form"):
                    cc_to_pay = st.selectbox("Seleziona Carta di Credito da pagare", credit_cards)
                    paying_account = st.selectbox("Paga usando il conto", standard_accounts)
                    
                    # Trova il saldo da pagare per la carta selezionata
                    amount_to_pay_row = df_balances[df_balances['Nome'] == cc_to_pay]
                    amount_to_pay = abs(amount_to_pay_row['Da Pagare'].iloc[0]) if not amount_to_pay_row.empty else 0.0

                    payment_amount = st.number_input("Importo da pagare", value=amount_to_pay, min_value=0.0)
                    payment_date = st.date_input("Data Pagamento", date.today(), format="DD/MM/YYYY")

                    if st.form_submit_button("Registra Pagamento", type="primary"):
                        if payment_amount > 0:
                            desc = f"Pagamento estratto conto {cc_to_pay}"
                            # 1. Uscita dal conto standard
                            add_tx(payment_date, paying_account, "Trasferimento", -payment_amount, desc)
                            # 2. Entrata (per azzerare il debito) sulla carta di credito
                            add_tx(payment_date, cc_to_pay, "Trasferimento", payment_amount, desc)
                            st.success("Pagamento registrato con successo!")
                            st.cache_data.clear(); st.rerun()
                        else:
                            st.warning("L'importo del pagamento deve essere maggiore di zero.")

    with tabs[5]: # Debiti/Crediti
        st.header("Gestione Debiti e Crediti")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("➕ Aggiungi Nuovo")
            with st.form("debt_form", clear_on_submit=True):
                person = st.text_input("Persona")
                amount = st.number_input("Importo", min_value=0.01, format="%.2f")
                type = st.radio("Tipo", ('Ho prestato (Credito)', 'Mi hanno prestato (Debito)'))
                due_date = st.date_input("Data di scadenza", date.today(), format="DD/MM/YYYY")
                if st.form_submit_button("Aggiungi"):
                    debt_type = 'lent' if type == 'Ho prestato (Credito)' else 'borrowed'
                    add_debt(person, amount, debt_type, due_date)
                    st.success("Aggiunto con successo!"); st.rerun()
        with col2:
            st.subheader("📋 In Sospeso")
            outstanding_debts = get_debts(status='outstanding')
            if not outstanding_debts: st.info("Nessun debito o credito in sospeso. Ottimo!")
            else:
                accounts = [acc[1] for acc in get_all_accounts(with_details=True) if acc[2] == 'standard']
                if not accounts: st.warning("Crea almeno un conto standard per poter saldare i debiti.")
                else:
                    for debt in outstanding_debts:
                        debt_id, person, amount, type, due_date, status, created_at = debt
                        label_type, date_str = ("Credito", parse_date(due_date).strftime('%d/%m/%Y')) if type == 'lent' else ("Debito", parse_date(due_date).strftime('%d/%m/%Y'))
                        st.markdown(f"**{label_type}** con **{person}** di **€ {amount:,.2f}** (Scad. {date_str})")
                        account_to_settle = st.selectbox("Scegli il conto per saldare", accounts, key=f"account_{debt_id}")
                        if st.button("Segna come Saldato", key=f"settle_{debt_id}", type="primary"):
                            settle_debt(debt_id, account_to_settle)
                            st.success("Operazione registrata!")
                            st.cache_data.clear(); st.rerun()
                        st.markdown("---")
    
    with tabs[6]: # Ricorrenze
        st.header("Movimenti Ricorrenti")
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("➕ Aggiungi Ricorrenza")
            with st.form("add_recurring_form", clear_on_submit=True):
                rec_name = st.text_input("Nome Ricorrenza (es. Affitto, Stipendio)")
                rec_amount = st.number_input("Importo", step=0.01)
                rec_start_date = st.date_input("Data di Inizio", format="DD/MM/YYYY")
                rec_interval = st.selectbox("Intervallo", options=["daily", "weekly", "monthly"], format_func=lambda x: x.capitalize())
                accounts, categories = get_all_accounts(), get_all_categories()
                rec_account = st.selectbox("Conto", options=accounts)
                rec_category = st.selectbox("Categoria", options=categories)
                rec_description = st.text_area("Descrizione (Opzionale)")
                
                if st.form_submit_button("Aggiungi Ricorrenza"):
                    if rec_name and rec_account and rec_category:
                        add_recurring(rec_name, rec_start_date, rec_interval, rec_amount, rec_account, rec_category, rec_description)
                        st.toast("Ricorrenza aggiunta!", icon="✅")
                        st.cache_data.clear(); st.rerun()
                    else:
                        st.warning("Nome, conto e categoria sono obbligatori.")
        
        with c2:
            st.subheader("Lista Ricorrenze Impostate")
            recs_data = get_recurring_transactions()
            if not recs_data:
                st.info("Nessuna ricorrenza impostata.")
            else:
                df_recs = pd.DataFrame(recs_data, columns=["id", "Nome", "Data Inizio", "Intervallo", "Importo", "Conto", "Categoria", "Descrizione"])
                if not df_recs.empty:
                    df_recs['Data Inizio'] = pd.to_datetime(df_recs['Data Inizio']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_recs.drop(columns=['id']), use_container_width=True, hide_index=True)

                st.markdown("---")
                st.subheader("🗑️ Elimina Ricorrenza")
                if not df_recs.empty:
                    rec_to_delete_id = st.selectbox("Seleziona ricorrenza da eliminare", options=df_recs['id'],
                        format_func=lambda x: f"{df_recs[df_recs['id']==x].iloc[0]['Nome']} - € {df_recs[df_recs['id']==x].iloc[0]['Importo']:.2f}")
                    if st.button("Elimina Ricorrenza Selezionata", type="primary"):
                        delete_recurring(rec_to_delete_id)
                        st.toast("Ricorrenza eliminata!", icon="🗑️")
                        st.cache_data.clear(); st.rerun()
                else:
                    st.info("Nessuna ricorrenza da eliminare.")

        st.markdown("---")
        st.subheader("💡 Suggerimenti di Ricorrenze")
        st.info("L'app ha analizzato i tuoi movimenti e ha trovato dei possibili pattern ricorrenti. Puoi aggiungerli con un click.")

        with st.spinner("Analisi dei movimenti in corso..."):
            suggestions = find_recurring_suggestions()

        if not suggestions:
            st.success("Nessun nuovo pattern ricorrente trovato. Sei aggiornatissimo!")
        else:
            for i, suggestion in enumerate(suggestions):
                desc, amount, interval, cat, acc, start_date_str = suggestion
                start_date_obj = parse_date(start_date_str)
                with st.container(border=True):
                    st.subheader(f"🧾 {desc}")
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.metric(
                            label="Importo Medio Rilevato",
                            value=f"€ {abs(amount):.2f}",
                            delta="Uscita" if amount < 0 else "Entrata"
                        )
                        st.caption(f"🗓️ **Frequenza:** {interval.capitalize()} | 📂 **Categoria:** {cat} | 🏦 **Conto:** {acc}")
                    with col2:
                        st.write("")
                        st.write("")
                        if st.button("➕ Aggiungi", key=f"add_sugg_{i}", type="primary", use_container_width=True, help="Aggiungi questa operazione alle tue ricorrenze"):
                            add_recurring(
                                name=desc,
                                start_date=start_date_obj,
                                interval=interval,
                                amount=round(amount, 2),
                                account_name=acc,
                                category_name=cat,
                                description="Ricorrenza generata da suggerimento automatico."
                            )
                            st.toast(f"Ricorrenza '{desc}' aggiunta!", icon="✅")
                            st.cache_data.clear(); st.rerun()

    with tabs[7]: # Budget
        st.header("Analisi e Gestione Budget")
        year = st.number_input("Seleziona Anno", min_value=2020, max_value=2100, value=date.today().year)
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("➕ Aggiungi / Modifica Voce di Budget")
            with st.form("add_budget_form", clear_on_submit=True):
                month_map = {i: calendar.month_name[i] for i in range(1, 13)}
                budget_month = st.selectbox("Mese", options=list(month_map.keys()), format_func=lambda x: month_map[x])
                
                expense_categories = [cat[1] for cat in get_all_categories_with_types() if cat[2] == 'expense']
                budget_category = st.selectbox("Categoria di Spesa", options=expense_categories)
                
                accounts_for_budget = ["Tutti i conti"] + get_all_accounts()
                budget_account = st.selectbox("Conto di Riferimento", options=accounts_for_budget)

                budget_amount = st.number_input("Importo Budget", min_value=0.01, step=10.0)
                
                if st.form_submit_button("Salva Budget"):
                    add_budget(year, budget_month, budget_category, budget_account, budget_amount)
                    st.toast("Voce di budget salvata!", icon="💰")
                    st.cache_data.clear(); st.rerun()
            
            budgets_data_for_deletion = get_budgets_by_year(year)
            if budgets_data_for_deletion:
                st.subheader("🗑️ Elimina Voce di Budget")
                df_budget_list = pd.DataFrame(budgets_data_for_deletion, columns=["id", "Mese", "Categoria", "Conto", "Budget (€)"])
                budget_to_delete_id = st.selectbox(
                    "Seleziona budget da eliminare", options=df_budget_list['id'],
                    format_func=lambda x: f"{calendar.month_name[df_budget_list.loc[df_budget_list['id']==x, 'Mese'].iloc[0]]} - {df_budget_list.loc[df_budget_list['id']==x, 'Categoria'].iloc[0]} ({df_budget_list.loc[df_budget_list['id']==x, 'Conto'].iloc[0]})"
                )
                if st.button("Elimina Budget Selezionato", type="primary"):
                    delete_budget(budget_to_delete_id)
                    st.toast("Voce di budget eliminata!", icon="🗑️")
                    st.cache_data.clear(); st.rerun()
        
        with c2:
            st.subheader(f"Analisi Budget - {year}")
            budgets_data = get_budgets_by_year(year)
            if not budgets_data:
                st.info(f"Nessun budget impostato per il {year}. Aggiungine uno per iniziare.")
            else:
                actual_expenses_dict = get_actual_expenses_by_year(year)
                df_budget = pd.DataFrame(budgets_data, columns=["id", "Mese", "Categoria", "Conto", "Budget (€)"])
                
                df_budget["Spesa Reale (€)"] = df_budget.apply(lambda row: actual_expenses_dict.get((row["Mese"], row["Categoria"], row["Conto"]), 0.0), axis=1)
                df_budget["Scostamento (€)"] = df_budget["Budget (€)"] - df_budget["Spesa Reale (€)"]
                
                accounts_for_filter = ["Tutti i conti"] + get_all_accounts()
                account_filter = st.selectbox("Filtra Risultati per Conto", options=accounts_for_filter, key="budget_filter")

                df_display = df_budget[df_budget['Conto'].isin([account_filter, 'Tutti i conti'])].copy() if account_filter != "Tutti i conti" else df_budget.copy()
                
                if not df_display.empty:
                    df_display = df_display.sort_values(by=["Mese", "Categoria"]).reset_index(drop=True)
                    df_display['Mese'] = df_display['Mese'].astype(int).apply(lambda x: calendar.month_name[x])

                    st.dataframe(
                        df_display[['Mese', 'Categoria', 'Conto', 'Budget (€)', 'Spesa Reale (€)', 'Scostamento (€)']],
                        use_container_width=True, hide_index=True,
                        column_config={
                            "Budget (€)": st.column_config.NumberColumn(format="€ %.2f"),
                            "Spesa Reale (€)": st.column_config.NumberColumn(format="€ %.2f"),
                            "Scostamento (€)": st.column_config.NumberColumn(format="€ %.2f"),
                        }
                    )

                    df_chart = df_display.melt(id_vars=['Mese', 'Categoria'], value_vars=['Budget (€)', 'Spesa Reale (€)'], var_name='Tipo', value_name='Importo')
                    fig = px.bar(df_chart, x="Categoria", y="Importo", color="Tipo", barmode="group", facet_col="Mese", facet_col_wrap=4, title=f"Confronto Budget vs. Spesa Reale - {year}", height=500)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Nessun dato di budget da visualizzare per i filtri selezionati.")

    with tabs[8]: # Forecast
        st.header("📈 Forecast Evoluto (Saldi a Fine Mese)")
        st.info("Il forecast analizza solo i conti standard per proiettare la liquidità futura.")
        
        c1, c2 = st.columns(2)
        standard_accounts = [acc[1] for acc in get_all_accounts(with_details=True) if acc[2] == 'standard']
        accounts = ["Tutti"] + standard_accounts
        filter_account = c1.selectbox("Conto per forecast", accounts, key="fc_acc")
        months_to_project = c2.slider("Mesi di proiezione", 1, 24, 6)

        st.markdown("---")

        start_date, end_date = date.today(), date.today() + relativedelta(months=months_to_project)
        account_param = None if filter_account == "Tutti" else filter_account
        current_balance = get_balance_before_date(start_date, account_param)
        future_events = get_future_events(start_date, end_date, account_param)
        
        if not future_events:
            st.info("Nessun movimento ricorrente o pianificato trovato per il periodo selezionato. Il saldo rimarrà costante.")
            st.metric("Saldo Attuale", f"€ {current_balance:,.2f}")
        else:
            monthly_flows = defaultdict(lambda: {'income': 0.0, 'expense': 0.0})
            for event in future_events:
                month_key = event['date'].strftime("%Y-%m")
                if event['amount'] > 0: monthly_flows[month_key]['income'] += event['amount']
                else: monthly_flows[month_key]['expense'] += event['amount']
            
            forecast_data, running_balance = [], current_balance
            for i in range(months_to_project):
                current_month_date = start_date + relativedelta(months=i)
                month_key, month_name = current_month_date.strftime("%Y-%m"), current_month_date.strftime("%B %Y")
                flows = monthly_flows[month_key]
                income, expense = flows['income'], abs(flows['expense'])
                net_flow = income - expense
                running_balance += net_flow
                forecast_data.append({"Mese": month_name, "Entrate Previste": income, "Uscite Previste": expense, "Flusso Netto": net_flow, "Saldo a Fine Mese": running_balance})
            
            df_forecast = pd.DataFrame(forecast_data)
            st.subheader("Previsione Saldi Mensili")
            
            fig = px.bar(df_forecast, x="Mese", y="Saldo a Fine Mese", title=f"Evoluzione del Saldo Previsto nei Prossimi {months_to_project} Mesi", text_auto='.2f', labels={"Saldo a Fine Mese": "Saldo Previsto (€)"})
            fig.update_traces(textangle=0, textposition="outside"); st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Riepilogo Flussi di Cassa Previsti")
            st.dataframe(df_forecast, use_container_width=True, hide_index=True,
                column_config={"Entrate Previste": st.column_config.NumberColumn(format="€ %.2f"), "Uscite Previste": st.column_config.NumberColumn(format="€ %.2f"),
                               "Flusso Netto": st.column_config.NumberColumn(format="€ %.2f"), "Saldo a Fine Mese": st.column_config.NumberColumn(format="€ %.2f")})

            with st.expander("Dettaglio Eventi Inclusi nella Previsione"):
                df_events = pd.DataFrame(future_events)
                df_events['date'] = pd.to_datetime(df_events['date']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_events, column_config={"date": "Data", "description": "Descrizione", "amount": "Importo (€)"}, hide_index=True, use_container_width=True)

    with tabs[9]: # Impostazioni & Import
        st.header("⚙️ Impostazioni e Import")
        
        st.subheader("Gestione Sessione")
        with st.container(border=True):
            st.write(f"Hai effettuato l'accesso come **{st.session_state.username}**.")
            if st.button("Logout", use_container_width=True):
                st.session_state.authenticated = False
                st.session_state.username = None
                st.session_state.is_admin = False
                st.toast("Logout effettuato con successo!")
                st.rerun()

        # --- PANNELLO AMMINISTRATORE ---
        if st.session_state.is_admin:
            st.subheader("👑 Pannello Amministratore")
            with st.container(border=True):
                st.write("**Gestione Utenti**")
                all_users = auth.get_all_users()
                df_users = pd.DataFrame(all_users, columns=["Username", "Ruolo"])
                st.dataframe(df_users, use_container_width=True, hide_index=True)
                
                with st.expander("🗑️ Elimina un utente"):
                    users_to_delete = [u[0] for u in all_users if u[0] != st.session_state.username]
                    if not users_to_delete:
                        st.info("Nessun altro utente da eliminare.")
                    else:
                        user_to_delete = st.selectbox("Seleziona utente da eliminare", users_to_delete)
                        if st.button(f"Elimina {user_to_delete}", type="primary"):
                            success, message = auth.delete_user(user_to_delete)
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
        
        st.subheader("Configurazione Categorie")
        with st.container(border=True):
            st.write("**Gestione Categorie**")
            df_cat = pd.DataFrame(get_all_categories_with_types(), columns=['id', 'Nome', 'Tipo'])
            st.dataframe(df_cat[['Nome', 'Tipo']], use_container_width=True, hide_index=True)
            
            with st.expander("⬆️ Importa Categorie da File"):
                st.info("Carica un file Excel (.xlsx) o CSV (.csv) con due colonne: 'Nome' e 'Tipo'. I valori per 'Tipo' devono essere 'income' o 'expense'.")
                cat_file = st.file_uploader("Carica file Categorie", type=["xlsx", "csv"])
                if cat_file:
                    try:
                        df_cat_import = pd.read_csv(cat_file) if cat_file.name.endswith('.csv') else pd.read_excel(cat_file, engine='openpyxl')
                        df_cat_import.columns = [col.strip().lower() for col in df_cat_import.columns]
                        if 'nome' in df_cat_import.columns and 'tipo' in df_cat_import.columns:
                            df_cat_import['tipo'] = df_cat_import['tipo'].str.strip().str.lower()
                            df_cat_import = df_cat_import[df_cat_import['tipo'].isin(['income', 'expense'])]
                            df_cat_import.dropna(subset=['nome'], inplace=True)
                            df_cat_import['nome'] = df_cat_import['nome'].astype(str).str.strip()
                            st.write("Categorie valide trovate nel file:")
                            st.dataframe(df_cat_import, use_container_width=True)
                            if not df_cat_import.empty and st.button("Conferma Importazione Categorie"):
                                cats_to_add = list(df_cat_import.itertuples(index=False, name=None))
                                bulk_add_categories(cats_to_add); st.success(f"{len(cats_to_add)} categorie importate/aggiornate!"); st.cache_data.clear(); st.rerun()
                        else: st.error("Il file deve contenere le colonne 'Nome' e 'Tipo'.")
                    except Exception as e: st.error(f"Errore nella lettura del file: {e}")

            op_tabs = st.tabs(["➕ Aggiungi", "✏️ Modifica", "🗑️ Elimina"])
            with op_tabs[0]:
                with st.form("add_cat_form", clear_on_submit=True):
                    new_cat_name = st.text_input("Nome Nuova Categoria")
                    new_cat_type = st.selectbox("Tipo Categoria", options=['expense', 'income'], format_func=lambda x: "Spesa" if x == 'expense' else "Entrata")
                    if st.form_submit_button("Aggiungi Categoria", type="primary"):
                        if new_cat_name:
                            success, message = add_category(new_cat_name, new_cat_type)
                            if success: st.toast("Categoria aggiunta!", icon="✅"); st.cache_data.clear(); st.rerun()
                            else: st.error(message)
                        else: st.warning("Il nome della categoria non può essere vuoto.")
            with op_tabs[1]:
                if not df_cat.empty:
                    cat_to_edit_id = st.selectbox("Seleziona categoria da modificare", options=df_cat['id'], format_func=lambda x: df_cat[df_cat['id'] == x]['Nome'].iloc[0], key="edit_cat_select")
                    selected_cat = df_cat[df_cat['id'] == cat_to_edit_id].iloc[0]
                    with st.form(f"edit_cat_form_{cat_to_edit_id}"):
                        edit_cat_name = st.text_input("Nuovo Nome", value=selected_cat['Nome'])
                        current_type_index = 0 if selected_cat['Tipo'] == 'expense' else 1
                        edit_cat_type = st.selectbox("Nuovo Tipo", options=['expense', 'income'], index=current_type_index, format_func=lambda x: "Spesa" if x == 'expense' else "Entrata")
                        if st.form_submit_button("Salva Modifiche", type="primary"):
                            success, message = update_category(cat_to_edit_id, edit_cat_name, edit_cat_type)
                            if success: st.toast("Categoria aggiornata!", icon="🔄"); st.cache_data.clear(); st.rerun()
                            else: st.error(message)
                else: st.info("Nessuna categoria da modificare.")
            with op_tabs[2]:
                if not df_cat.empty:
                    cat_to_delete_id = st.selectbox("Seleziona categoria da eliminare", options=df_cat['id'], format_func=lambda x: df_cat[df_cat['id'] == x]['Nome'].iloc[0], key="delete_cat_select_2")
                    st.warning("L'eliminazione è possibile solo se la categoria non è associata a nessun movimento.")
                    if st.button("Elimina Categoria Selezionata", type="primary", key="delete_cat_button"):
                        success, message = delete_category(cat_to_delete_id)
                        if success: st.toast("Categoria eliminata!", icon="🗑️"); st.cache_data.clear(); st.rerun()
                        else: st.error(message)
                else: st.info("Nessuna categoria da eliminare.")
        
        st.markdown("---")
        
        st.subheader("Automazione e Apprendimento")
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                st.write("**Regole di Categorizzazione (Manuale)**")
                with st.expander("Gestisci Regole per Parola Chiave"):
                    df_rules = pd.DataFrame(get_rules(), columns=["id", "Parola Chiave", "Categoria Assegnata"])
                    st.dataframe(df_rules.drop(columns=['id']), use_container_width=True, hide_index=True)
                    with st.form("add_rule_form", clear_on_submit=True):
                        keyword = st.text_input("Se la descrizione contiene...")
                        category = st.selectbox("Assegna la categoria...", get_all_categories(), key="rule_cat")
                        if st.form_submit_button("Aggiungi Regola"):
                            if keyword and category: add_rule(keyword, category); st.success("Regola aggiunta!"); st.rerun()
                    if not df_rules.empty:
                        rule_to_delete_id = st.selectbox("Elimina una Regola", options=df_rules['id'], format_func=lambda x: f"'{df_rules[df_rules['id']==x].iloc[0]['Parola Chiave']}' -> {df_rules[df_rules['id']==x].iloc[0]['Categoria Assegnata']}")
                        if st.button("Elimina Regola Selezionata", type="primary"): delete_rule(rule_to_delete_id); st.success("Regola eliminata!"); st.rerun()
            with c2:
                st.write("**Categorizzazione Automatica (AI)**")
                st.write("L'app può imparare dalle tue transazioni passate per suggerire categorie durante l'importazione.")
                if st.button("🧠 Allena Modello", use_container_width=True):
                    with st.spinner("Allenamento in corso..."):
                        training_data = get_transactions_for_training()
                        if len(training_data) > 1:
                            success, message = train_model(training_data)
                            if success: st.success(f"Modello allenato con successo su {len(training_data)} movimenti!")
                            else: st.error(message)
                        else: st.warning("Servono almeno 2 movimenti con descrizione per allenare il modello.")

                st.write("**Verifica il Modello**")
                test_description = st.text_input("Inserisci una descrizione di test:")
                if test_description:
                    prediction = predict_single(test_description)
                    if prediction: st.success(f"Categoria Predetta: **{prediction}**")
                    else: st.warning("Il modello non è ancora stato allenato o non ha trovato una corrispondenza.")
        
        st.markdown("---")
        
        st.subheader("Importazione Dati")
        with st.container(border=True):
            st.write("**Importa Movimenti da File Excel**")
            excel_file = st.file_uploader("Carica file Excel", type=["xlsx", "xls"], key="excel_uploader")
            if excel_file is None: st.session_state.uploaded_file_id, st.session_state.df_import_review = None, None
            elif st.session_state.uploaded_file_id != excel_file.file_id: st.session_state.uploaded_file_id, st.session_state.df_import_review = excel_file.file_id, None
            
            if excel_file and st.session_state.df_import_review is None:
                try:
                    df_import = pd.read_excel(excel_file, engine='openpyxl')
                    st.info("File caricato. Mappa le colonne e scegli le modalità di assegnazione.")
                    all_accs = get_all_accounts()
                    if not all_accs: st.error("Crea almeno un conto nel tab 'Conti' prima di importare.")
                    else:
                        account_mode = st.radio("Assegnazione conto:", ("Unico", "Da colonna"), horizontal=True, key="account_mode_radio")
                        with st.form("import_mapping_form"):
                            cols = df_import.columns.tolist()
                            st.write("**Passo 1: Mappatura Colonne**")
                            c1, c2 = st.columns(2)
                            date_col = c1.selectbox("Colonna Data", cols, index=cols.index('Data') if 'Data' in cols else 0)
                            desc_col = c1.selectbox("Colonna Descrizione", cols, index=cols.index('Descrizione') if 'Descrizione' in cols else 0)
                            amount_col = c2.selectbox("Colonna Importo", cols, index=cols.index('Importo') if 'Importo' in cols else 0)
                            cat_col_opts = ["--- Usa AI ---"] + cols
                            category_col = c1.selectbox("Colonna Categoria (Opzionale)", cat_col_opts, index=cat_col_opts.index('Categoria') if 'Categoria' in cat_col_opts else 0)
                            if account_mode == "Da colonna": account_col = c2.selectbox("Colonna Conto", cols, index=cols.index('Conto') if 'Conto' in cols else 0, key="account_col_select")
                            else: account_to_import = c2.selectbox("Conto di destinazione unico", all_accs, key="account_to_import_select")
                            
                            if st.form_submit_button("Prepara per Revisione", type="primary"):
                                with st.spinner("Analisi e ricerca corrispondenze..."):
                                    df_review = df_import[[date_col, desc_col, amount_col]].copy(); df_review.columns = ['Data', 'Descrizione', 'Importo']
                                    df_review['Conto'] = df_import[account_col].astype(str).str.strip().fillna(all_accs[0]) if account_mode == "Da colonna" else account_to_import
                                    df_review['Conto'] = df_review['Conto'].apply(lambda x: x if x in all_accs else all_accs[0])
                                    if category_col != cat_col_opts[0]:
                                        df_review['Categoria'] = df_import[category_col].astype(str).str.strip().fillna('')
                                        to_predict_mask = (df_review['Categoria'] == '')
                                        if to_predict_mask.any():
                                            predicted = predict_category(df_review.loc[to_predict_mask, 'Descrizione'].fillna('').tolist())
                                            if predicted: df_review.loc[to_predict_mask, 'Categoria'] = predicted
                                        df_review['Categoria'] = df_review['Categoria'].apply(lambda x: x if x in get_all_categories() else 'Da categorizzare')
                                    else:
                                        predicted = predict_category(df_review['Descrizione'].fillna('').tolist())
                                        df_review['Categoria'] = predicted if predicted is not None else 'Da categorizzare'
                                    matches, match_ids = [], []
                                    for row in df_review.itertuples():
                                        match = find_best_matching_planned_tx(row.Data, row.Importo)
                                        if match:
                                            matches.append(f"ID:{match['id']} | {match['description']} ({match['amount']:.2f}€ il {parse_date(match['plan_date']).strftime('%d/%m/%Y')})")
                                            match_ids.append(match['id'])
                                        else: matches.append(None); match_ids.append(None)
                                    df_review['ID Pianificato'] = match_ids; df_review['Corrispondenza Suggerita'] = matches; df_review['Riconcilia'] = False
                                    st.session_state.df_import_review = df_review.to_dict('records')
                                    st.rerun()
                except Exception as e: st.error(f"Errore durante la lettura del file Excel: {e}")
            
            if st.session_state.df_import_review is not None:
                st.subheader("Revisione e Importazione Movimenti")
                st.info("Controlla i dati. Se l'app suggerisce una corrispondenza, spunta 'Riconcilia' per collegare il movimento reale a quello pianificato.")
                df_to_edit = pd.DataFrame(st.session_state.df_import_review)
                edited_df = st.data_editor(df_to_edit,
                    column_config={"Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY", required=True), "Importo": st.column_config.NumberColumn("Importo", format="€ %.2f", required=True),
                                   "Categoria": st.column_config.SelectboxColumn("Categoria", options=get_all_categories(), required=True), "Conto": st.column_config.SelectboxColumn("Conto", options=get_all_accounts(), required=True),
                                   "Corrispondenza Suggerita": st.column_config.TextColumn(disabled=True), "Riconcilia": st.column_config.CheckboxColumn(default=False), "ID Pianificato": None},
                    use_container_width=True, hide_index=True, num_rows="dynamic", key="import_data_editor",
                    column_order=["Riconcilia", "Data", "Descrizione", "Importo", "Categoria", "Conto", "Corrispondenza Suggerita"])
                if st.button("✅ Importa Tutto", type="primary"):
                    with st.spinner("Importazione in corso..."):
                        reconciled_count, new_count = 0, 0
                        for index, row in edited_df.iterrows():
                            if row['Riconcilia'] and df_to_edit.loc[index, 'ID Pianificato'] is not None:
                                planned_id = df_to_edit.loc[index, 'ID Pianificato']
                                tx_data = {'date': row['Data'], 'account': row['Conto'], 'category': row['Categoria'], 'amount': float(row['Importo']), 'description': row['Descrizione']}
                                reconcile_tx(planned_id, tx_data); reconciled_count += 1
                            else:
                                add_tx(row['Data'], row['Conto'], row['Categoria'], float(row['Importo']), row['Descrizione']); new_count += 1
                    st.success(f"Importazione completata! {reconciled_count} movimenti riconciliati e {new_count} nuovi movimenti aggiunti.")
                    st.session_state.df_import_review, st.session_state.uploaded_file_id = None, None
                    st.cache_data.clear(); st.rerun()

        st.markdown("---")
        st.subheader("Manutenzione")
        with st.container(border=True):
            st.write("**Pulizia Dati**")
            st.info("Questo comando elimina in modo sicuro tutte le categorie che non sono mai state utilizzate in nessun movimento. È utile per fare ordine.")
            if st.button("Pulisci Categorie non Utilizzate"):
                deleted_count = delete_unused_categories()
                st.success(f"{deleted_count} categorie non utilizzate eliminate.") if deleted_count > 0 else st.info("Nessuna categoria inutilizzata da eliminare.")
                st.cache_data.clear(); st.rerun()

            with st.expander("⚠️ Area Pericolosa: Inizializzazione Database"):
                st.warning("Cancellerà TUTTI i dati tranne gli account utente. Usare con cautela.")
                if st.button("Inizializza Database Adesso", type="primary"):
                    reset_db(); st.success("Database re-inizializzato!"); st.cache_data.clear(); st.rerun()

# --- SCHERMATA DI LOGIN ---
def login_screen():
    st.set_page_config(page_title="Cashflow Personale - Accesso", layout="centered")
    load_css(CSS_FILE)
    
    with st.container():
        st.title("💰 Cashflow Pro")
        st.markdown("Accedi per gestire le tue finanze")
        
        user_count = auth.get_user_count()
        if user_count == 0:
            st.info("Benvenuto! Crea il primo account per iniziare.")
            st.session_state.login_page = "Crea Account"
        else:
            if 'login_page' not in st.session_state:
                st.session_state.login_page = "Login"
            
            st.session_state.login_page = st.radio(
                "Scegli un'azione", 
                ["Login", "Crea Account", "Recupera Password"], 
                horizontal=True, 
                key="login_nav",
                index=["Login", "Crea Account", "Recupera Password"].index(st.session_state.login_page)
            )

        if st.session_state.login_page == "Login":
            with st.form("login_form"):
                username = st.text_input("Nome Utente", placeholder="mario.rossi")
                password = st.text_input("Password", type="password", placeholder="********")
                if st.form_submit_button("Login", type="primary", use_container_width=True):
                    if auth.authenticate_user(username, password):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.is_admin = auth.is_admin(username)
                        st.toast(f"Bentornato, {username}!", icon="👋")
                        st.rerun()
                    else:
                        st.error("Credenziali non valide. Riprova.")

        elif st.session_state.login_page == "Crea Account":
            with st.form("signup_form", clear_on_submit=True):
                st.subheader("Crea un nuovo account")
                new_username = st.text_input("Scegli un Nome Utente")
                new_password = st.text_input("Scegli una Password (almeno 8 caratteri)", type="password")
                confirm_password = st.text_input("Conferma Password", type="password")
                st.markdown("---")
                st.write("**Imposta il recupero password**")
                security_questions = ["Qual era il nome del tuo primo animale domestico?", "Qual è il cognome da nubile di tua madre?", "In quale città sei nato?", "Qual è il tuo film preferito?"]
                question = st.selectbox("Scegli una domanda di sicurezza", security_questions)
                answer = st.text_input("Scrivi la risposta", type="password")

                if st.form_submit_button("Registrati", type="primary", use_container_width=True):
                    if new_password != confirm_password: st.error("Le password non coincidono.")
                    else:
                        success, message = auth.create_user(new_username, new_password, question, answer)
                        if success: 
                            st.success(message)
                            st.session_state.login_page = "Login"
                            st.rerun()
                        else: 
                            st.error(message)

        elif st.session_state.login_page == "Recupera Password":
            st.subheader("Recupera la tua password")

            if st.session_state.get('reset_step', 1) == 1:
                with st.form("reset_step1_form"):
                    username_to_reset = st.text_input("Inserisci il tuo nome utente")
                    if st.form_submit_button("Avanti", use_container_width=True):
                        question = auth.get_security_question(username_to_reset)
                        if question:
                            st.session_state.reset_username, st.session_state.security_question = username_to_reset, question
                            st.session_state.reset_step = 2; st.rerun()
                        else: st.error("Nome utente non trovato.")
            
            elif st.session_state.reset_step == 2:
                st.write(f"Utente: **{st.session_state.reset_username}**")
                st.info(f"Domanda di sicurezza: **{st.session_state.security_question}**")
                with st.form("reset_step2_form"):
                    security_answer = st.text_input("La tua risposta", type="password")
                    if st.form_submit_button("Verifica Risposta", use_container_width=True):
                        if auth.verify_security_answer(st.session_state.reset_username, security_answer):
                            st.session_state.reset_step = 3; st.rerun()
                        else: st.error("Risposta non corretta.")
                if st.button("Torna indietro"):
                    st.session_state.reset_step = 1; st.rerun()

            elif st.session_state.reset_step == 3:
                st.success("Risposta corretta! Ora puoi impostare una nuova password.")
                with st.form("reset_step3_form"):
                    new_pass = st.text_input("Nuova Password (almeno 8 caratteri)", type="password")
                    confirm_new_pass = st.text_input("Conferma Nuova Password", type="password")
                    if st.form_submit_button("Salva Nuova Password", type="primary", use_container_width=True):
                        if new_pass != confirm_new_pass: st.error("Le password non coincidono.")
                        else:
                            success, message = auth.reset_password(st.session_state.reset_username, new_pass)
                            if success:
                                st.success(message)
                                st.session_state.reset_step, st.session_state.login_page = 1, 'Login'
                                st.rerun()
                            else: st.error(message)

# --- ESECUZIONE PRINCIPALE ---
if __name__ == "__main__":
    init_session_state()
    
    # Controlla se l'utente è autenticato e mostra la vista corretta
    if st.session_state.authenticated:
        st.set_page_config(page_title="Cashflow Pro", layout="wide", initial_sidebar_state="collapsed")
        load_css(CSS_FILE)   # <-- CSS dopo il login
        init_db()
        auth.create_auth_schema()
        show_main_dashboard()
    else:
        # La configurazione della pagina per il login è diversa
        st.set_page_config(page_title="Cashflow Pro - Accesso", layout="centered")
        init_db() # Il DB deve essere inizializzato anche per la pagina di login
        auth.create_auth_schema()
        load_css(CSS_FILE)   # <-- CSS anche per il login
        login_screen()
