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
import os
from pathlib import Path

# Importa le funzioni dal file db.py
try:
    from db import (
        init_db, populate_new_workspace, conn, add_tx, parse_date, DB_PATH, reset_db,
        add_account, delete_account, update_account, get_account_details_by_name,
        get_db_data,
        delete_tx, update_tx, get_all_categories, get_all_accounts,
        get_accounts_with_balance, get_all_transactions_raw, get_transaction_by_id,
        get_summary_by_category, get_monthly_summary,
        get_recurring_transactions, get_budgets_by_year, add_recurring, delete_recurring,
        add_budget, delete_budget, get_actual_expenses_by_year,
        get_balance_before_date, get_transactions_in_range,
        add_debt, get_debts, settle_debt, delete_debt,
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
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CSS_FILE = os.path.join(SCRIPT_DIR, "styles", "main.css")


# --- FUNZIONE PER CARICARE IL CSS ---
def load_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"ATTENZIONE: File CSS non trovato al percorso: '{file_name}'.")

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
        'user_id': None,
        'workspaces': [],
        'current_workspace_id': None,
        'login_page': 'Login',
        'reset_step': 1,
        'reset_username': '',
        'df_cat_import_preview': None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# --- DIALOGO DI MODIFICA ---
@st.dialog("Gestisci Movimento", width="large")
def manage_transaction_dialog():
    ws_id = st.session_state.current_workspace_id
    tx_id = st.session_state.tx_to_edit
    tx_data = get_transaction_by_id(ws_id, tx_id)
    if not tx_data:
        st.error("Movimento non trovato."); return

    st.subheader("Modifica Dati")
    with st.form("edit_dialog_form"):
        all_accounts = get_all_accounts(ws_id); all_categories = get_all_categories(ws_id)
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
            update_tx(ws_id, tx_id, edit_date, edit_account, edit_category, edit_amount, edit_description)
            st.toast("Movimento aggiornato!", icon="✅")
            st.session_state.tx_to_edit = None; st.cache_data.clear(); st.rerun()

    st.markdown("---")
    c_del, c_ann = st.columns(2)
    with c_del:
        if st.button("Elimina Definitivamente", use_container_width=True):
            delete_tx(ws_id, tx_id)
            st.toast("Movimento eliminato!", icon="🗑️")
            st.session_state.tx_to_edit = None; st.cache_data.clear(); st.rerun()
    with c_ann:
        if st.button("Annulla", use_container_width=True):
            st.session_state.tx_to_edit = None; st.rerun()

# --- VISTA PRINCIPALE ---
def show_main_dashboard():
    st.set_page_config(page_title="Cashflow Pro", layout="wide", initial_sidebar_state="expanded")
    
    st.markdown("""
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    """, unsafe_allow_html=True)
    
    load_css(CSS_FILE)

    ws_id = st.session_state.current_workspace_id
    if not ws_id:
        st.error("Nessun workspace selezionato o disponibile. Prova a ricaricare la pagina.")
        st.stop()

    workspace_map = {ws[0]: {'name': ws[1], 'role': ws[2]} for ws in st.session_state.workspaces}
    current_user_role = workspace_map.get(ws_id, {}).get('role')

    with st.sidebar:
        st.title(f"💸 Workspace")
        workspace_ids = list(workspace_map.keys())
        current_ws_index = workspace_ids.index(ws_id) if ws_id in workspace_ids else 0
        
        selected_workspace_id = st.selectbox(
            "Seleziona uno spazio di lavoro", options=workspace_ids,
            format_func=lambda ws_id: workspace_map.get(ws_id, {}).get('name', "Sconosciuto"),
            index=current_ws_index, key="workspace_selector"
        )

        if selected_workspace_id != st.session_state.current_workspace_id:
            st.session_state.current_workspace_id = selected_workspace_id
            st.cache_data.clear(); st.rerun()
        
        st.markdown("---")

        if current_user_role == 'owner':
            with st.expander("🔑 Gestione Workspace"):
                st.subheader("Membri Attuali")
                members = auth.get_workspace_members(ws_id)
                df_members = pd.DataFrame(members, columns=['ID', 'Username', 'Ruolo'])
                st.dataframe(df_members[['Username', 'Ruolo']], use_container_width=True, hide_index=True)

                st.subheader("Aggiungi Membro")
                with st.form("add_member_form", clear_on_submit=True):
                    all_users = [u[0] for u in auth.get_all_users_for_invite() if u[0] not in df_members['Username'].tolist()]
                    member_username = st.selectbox("Seleziona utente da invitare", all_users)
                    member_role = st.selectbox("Assegna ruolo", ['editor', 'viewer'], format_func=lambda r: "Editor (può modificare)" if r == 'editor' else "Visualizzatore (solo lettura)")
                    if st.form_submit_button("Aggiungi Utente", type="primary"):
                        if member_username:
                            success, message = auth.add_user_to_workspace(ws_id, member_username, member_role)
                            if success: st.success(message); st.rerun()
                            else: st.error(message)
                        else:
                            st.warning("Nessun utente da aggiungere.")

                st.subheader("Rimuovi Membro")
                members_to_remove = [m for m in members if m[2] != 'owner']
                if members_to_remove:
                    member_to_remove_id = st.selectbox("Seleziona utente da rimuovere",
                        options=[m[0] for m in members_to_remove],
                        format_func=lambda m_id: [m[1] for m in members_to_remove if m[0] == m_id][0])
                    if st.button("Rimuovi Utente Selezionato"):
                        success, message = auth.remove_user_from_workspace(ws_id, member_to_remove_id)
                        if success: st.success(message); st.rerun()
                        else: st.error(message)
        
        st.markdown("---")
        st.info(f"Accesso come: **{st.session_state.username}**")
        if st.button("Logout", use_container_width=True):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()

    current_workspace_name = workspace_map.get(ws_id, {}).get('name', "")
    st.title(f"Dashboard: {current_workspace_name}")
    
    if st.session_state.get('tx_to_edit'):
        manage_transaction_dialog()

    is_viewer = current_user_role == 'viewer'

    tabs = st.tabs([
        "📊 Dashboard", "📄 Movimenti", "🗓️ Pianificati", "💡 Pianificatore", 
        "🏦 Conti", "🤝 Debiti", "🔁 Ricorrenze", "💸 Budget", 
        "📈 Forecast", "⚙️ Impostazioni"
    ])
    
    with tabs[0]: # Dashboard
        accounts_data = get_accounts_with_balance(ws_id)
        df_balances = pd.DataFrame(accounts_data, columns=["Nome", "Tipo", "Plafond", "Saldo/Residuo", "Da Pagare"])
        st.subheader("Panoramica Attuale")
        m1, m2 = st.columns(2)
        with m1:
            total_liquidity = df_balances[df_balances['Tipo'] == 'standard']["Saldo/Residuo"].sum()
            st.metric(label="Liquidità Totale (Conti Standard)", value=f"€ {total_liquidity:,.2f}")
        with m2:
            net_worth = get_net_worth(ws_id)
            st.metric(label="Patrimonio Netto", value=f"€ {net_worth:,.2f}", help="Liquidità + Crediti - Debiti (incluse carte di credito)")
        st.markdown("---")
        
        st.subheader("Dettaglio Saldi per Conto")
        if not df_balances.empty:
            num_accounts = len(df_balances)
            max_cols = 4
            num_cols = min(num_accounts, max_cols)
            cols = st.columns(num_cols)
            for i, row in df_balances.iterrows():
                with cols[i % max_cols]:
                    if row['Tipo'] == 'credit_card':
                        st.metric(label=f"💳 {row['Nome']} (Da pagare)", value=f"€ {abs(row['Da Pagare'] or 0.0):,.2f}")
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
                accounts_list = ["Tutti"] + get_all_accounts(ws_id)
                selected_account = st.selectbox("Filtra per Conto", accounts_list)
            with c_filter2:
                date_range = st.date_input("Filtra per Intervallo di Date", value=(date.today() - timedelta(days=30), date.today()), format="DD/MM/YYYY")
            
            if len(date_range) != 2: st.stop()
            start_date, end_date = date_range
            account_param = None if selected_account == "Tutti" else selected_account
        
        adv_tabs = st.tabs(["Flusso Mensile", "Diagramma di Sankey", "Andamento Cumulativo", "Treemap Spese", "Andamento Categorie"])

        with adv_tabs[0]:
            monthly_data = get_monthly_summary(ws_id, start_date, end_date, account_param)
            if monthly_data:
                df_monthly = pd.DataFrame(monthly_data, columns=["Mese", "Entrate", "Uscite"]); df_monthly['Uscite'] = df_monthly['Uscite'].abs()
                fig_bar = px.bar(df_monthly, x="Mese", y=["Entrate", "Uscite"], barmode='group', title="Entrate vs Uscite nel Periodo")
                st.plotly_chart(fig_bar, use_container_width=True)
            else: st.info("Nessun dato di flusso cassa per il periodo selezionato.")

        with adv_tabs[1]:
            sankey_data = get_data_for_sankey(ws_id, start_date, end_date, account_param)
            if sankey_data:
                df_sankey = pd.DataFrame(sankey_data, columns=["category", "amount"])
                income_df = df_sankey[df_sankey['amount'] > 0]
                expense_df = df_sankey[df_sankey['amount'] < 0].copy()
                expense_df['amount'] = expense_df['amount'].abs()

                if not income_df.empty and not expense_df.empty:
                    sources, targets = list(income_df['category']), list(expense_df['category'])
                    all_nodes = list(set(sources + ["Patrimonio"] + targets))
                    color_palette = px.colors.qualitative.Plotly
                    color_map = {node: color_palette[i % len(color_palette)] for i, node in enumerate(all_nodes)}
                    truncated_labels = [label[:22] + '...' if len(label) > 25 else label for label in all_nodes]
                    links, link_colors = [], []
                    for _, row in income_df.iterrows():
                        links.append({"source": all_nodes.index(row['category']), "target": all_nodes.index("Patrimonio"), "value": row['amount']})
                        link_colors.append(color_map[row['category']].replace('rgb', 'rgba').replace(')', ', 0.4)'))
                    for _, row in expense_df.iterrows():
                        links.append({"source": all_nodes.index("Patrimonio"), "target": all_nodes.index(row['category']), "value": row['amount']})
                        link_colors.append(color_map[row['category']].replace('rgb', 'rgba').replace(')', ', 0.4)'))

                    fig_sankey = go.Figure(data=[go.Sankey(
                        node = dict(pad=15, thickness=20, line=dict(color="black", width=0.5), label=truncated_labels, color=[color_map[node] for node in all_nodes], customdata=all_nodes, hovertemplate='%{customdata} <extra></extra>'),
                        link = dict(source=[l['source'] for l in links], target=[l['target'] for l in links], value=[l['value'] for l in links], color=link_colors),
                        textfont=dict(color="white", size=12)
                    )])
                    fig_sankey.update_layout(title_text="Flusso dalle Entrate alle Uscite", font_size=12, height=600, margin=dict(l=20, r=20, t=50, b=20))
                    st.plotly_chart(fig_sankey, use_container_width=True)
                else:
                    st.info("Dati insufficienti per generare il diagramma di Sankey.")
            else:
                st.info("Nessun dato per generare il diagramma di Sankey.")

        with adv_tabs[2]:
            initial_balance = get_balance_before_date(ws_id, start_date, account_param)
            tx_in_range = get_transactions_in_range(ws_id, start_date, end_date, account_param)
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
            category_data = get_summary_by_category(ws_id, start_date, end_date, account_param)
            if category_data:
                df_category = pd.DataFrame(category_data, columns=["Categoria", "Totale Spese"])
                df_category = df_category[df_category['Totale Spese'] > 0]
                fig_tree = px.treemap(df_category, path=[px.Constant("Tutte le Spese"), 'Categoria'], values='Totale Spese', title="Ripartizione Spese per Categoria")
                st.plotly_chart(fig_tree, use_container_width=True)
            else:
                st.info("Nessuna spesa nel periodo selezionato.")

        with adv_tabs[4]:
            st.subheader("Analisi Andamento per Categoria di Spesa")
            all_cats_with_types = get_all_categories_with_types(ws_id)
            expense_categories = [cat[1] for cat in all_cats_with_types if cat[2] == 'expense']
            if not expense_categories:
                st.info("Nessuna categoria di spesa trovata.")
            else:
                selected_cat = st.selectbox("Seleziona una categoria da analizzare", options=expense_categories)
                trend_data = get_category_trend(ws_id, selected_cat, start_date, end_date)
                if trend_data:
                    df_trend = pd.DataFrame(trend_data, columns=["Mese", "Spesa"])
                    fig_trend = px.bar(df_trend, x="Mese", y="Spesa", title=f"Andamento Spesa per '{selected_cat}'", text_auto='.2s')
                    fig_trend.update_traces(textangle=0, textposition="outside")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.info(f"Nessuna spesa registrata per la categoria '{selected_cat}' nel periodo selezionato.")
    
    with tabs[1]: # Movimenti
        st.header("Gestione Movimenti")
        with st.expander("➕ Aggiungi un nuovo movimento", expanded=True):
            with st.form("add_tx_form"):
                accounts = get_all_accounts(ws_id); categories = get_all_categories(ws_id)
                c1,c2 = st.columns([3,1])
                tx_description = c1.text_input("Descrizione", key='add_desc', disabled=is_viewer)
                if c2.form_submit_button("Suggerisci Categoria 💡", disabled=is_viewer):
                    suggested_category = apply_rules(ws_id, tx_description)
                    st.session_state.suggested_category_index = categories.index(suggested_category) if suggested_category in categories else 0
                    st.rerun()

                c3,c4,c5 = st.columns(3)
                tx_date = c3.date_input("Data", date.today(), key='add_date', format="DD/MM/YYYY", disabled=is_viewer)
                tx_account = c3.selectbox("Conto", accounts, key='add_acc', disabled=is_viewer)
                tx_amount = c4.number_input("Importo", value=0.00, key='add_amount', step=0.01, format="%.2f", min_value=0.0, disabled=is_viewer)
                tx_category = c4.selectbox("Categoria", categories, key='add_cat', index=st.session_state.suggested_category_index, disabled=is_viewer)
                tx_type = c5.radio("Tipo", ["Uscita", "Entrata"], key='add_type', horizontal=True, disabled=is_viewer)

                if st.form_submit_button("Salva Movimento", type="primary", disabled=is_viewer):
                    if not accounts or not categories:
                        st.error("Prima di aggiungere un movimento, crea almeno un conto e una categoria nelle Impostazioni.")
                    elif tx_account and tx_category:
                        final_amount = abs(tx_amount) * (-1 if tx_type == "Uscita" else 1)
                        add_tx(ws_id, tx_date, tx_account, tx_category, final_amount, tx_description)
                        st.session_state.suggested_category_index = 0
                        st.toast("Movimento salvato!", icon="✅"); st.cache_data.clear(); st.rerun()
                    else: 
                        st.error("Seleziona un conto e una categoria.")

        st.markdown("---"); st.subheader("Lista e Filtri Movimenti")
        raw_data = get_all_transactions_raw(ws_id)
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
                df_display.drop(columns=['id']), hide_index=True, use_container_width=True, key=f"editor_{len(filtered_df)}_{len(df_tx)}", 
                column_config={"Modifica": st.column_config.CheckboxColumn(required=True, disabled=is_viewer)},
                disabled=df_display.columns.drop(["id", "Modifica"])
            )
            
            selected_rows = edited_df[edited_df["Modifica"]]
            if not selected_rows.empty:
                with st.expander(f"🔧 Azioni per {len(selected_rows)} movimenti selezionati", expanded=True):
                    if len(selected_rows) == 1:
                        selected_id = int(filtered_df.loc[selected_rows.index[0], "id"])
                        if st.button(f"Gestisci in dettaglio il movimento (ID: {selected_id})", disabled=is_viewer):
                            st.session_state.tx_to_edit = selected_id; st.rerun()
                        st.markdown("---")

                    st.write("**Azioni Massive**")
                    c1, c2, c3 = st.columns(3)
                    placeholder = "-- Non modificare --"
                    new_cat = c1.selectbox("Imposta nuova categoria", [placeholder] + get_all_categories(ws_id), key="bulk_cat", disabled=is_viewer)
                    new_acc = c2.selectbox("Imposta nuovo conto", [placeholder] + get_all_accounts(ws_id), key="bulk_acc", disabled=is_viewer)

                    if c3.button("Applica Modifiche", type="primary", disabled=is_viewer):
                        ids_to_update = filtered_df.loc[selected_rows.index, "id"].tolist()
                        final_new_cat = new_cat if new_cat != placeholder else None
                        final_new_acc = new_acc if new_acc != placeholder else None
                        if final_new_cat or final_new_acc:
                            bulk_update_transactions(ws_id, ids_to_update, new_category_name=final_new_cat, new_account_name=final_new_acc)
                            st.toast(f"{len(ids_to_update)} movimenti aggiornati!", icon="✨")
                            st.cache_data.clear(); st.rerun()
                        else: st.warning("Nessuna modifica selezionata.")
                    
                    if st.button("🗑️ Elimina Selezionati", disabled=is_viewer):
                        ids_to_delete = filtered_df.loc[selected_rows.index, "id"].tolist()
                        deleted_count = bulk_delete_transactions(ws_id, ids_to_delete)
                        st.toast(f"{deleted_count} movimenti eliminati con successo!", icon="🗑️")
                        st.cache_data.clear(); st.rerun()
        else: 
            st.info("Nessun movimento trovato con i filtri applicati.")

    with tabs[2]: # Pianificati
        st.header("🗓️ Movimenti Pianificati")
        st.info("In questa sezione puoi inserire entrate o uscite future.")
        c1, c2 = st.columns([1, 2])
        with c1:
            st.subheader("➕ Aggiungi Evento Futuro")
            with st.form("add_planned_tx_form", clear_on_submit=True):
                accounts, categories = get_all_accounts(ws_id), get_all_categories(ws_id)
                plan_description = st.text_input("Descrizione (es. Tasse, Bonus)", disabled=is_viewer)
                plan_date = st.date_input("Data Prevista", min_value=date.today(), format="DD/MM/YYYY", disabled=is_viewer)
                plan_amount = st.number_input("Importo Previsto", value=0.00, step=0.01, min_value=0.0, disabled=is_viewer)
                plan_type = st.radio("Tipo", ["Uscita", "Entrata"], horizontal=True, disabled=is_viewer)
                plan_account = st.selectbox("Conto Previsto", accounts, disabled=is_viewer)
                plan_category = st.selectbox("Categoria Prevista", categories, disabled=is_viewer)
                
                if st.form_submit_button("Salva Evento Pianificato", type="primary", disabled=is_viewer):
                    if plan_description and plan_account and plan_category:
                        final_amount = abs(plan_amount) * (-1 if plan_type == "Uscita" else 1)
                        add_planned_tx(ws_id, plan_date, plan_description, final_amount, plan_category, plan_account)
                        st.toast("Movimento pianificato salvato!", icon="✅"); st.cache_data.clear(); st.rerun()
                    else: st.warning("Tutti i campi sono obbligatori.")
        
        with c2:
            st.subheader("📋 Lista Eventi Futuri")
            planned_txs = get_all_planned_tx(ws_id)
            if planned_txs:
                df_planned = pd.DataFrame(planned_txs, columns=["id", "Data", "Descrizione", "Importo", "Categoria", "Conto"])
                df_planned['Data'] = pd.to_datetime(df_planned['Data']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_planned.drop(columns=['id']), use_container_width=True, hide_index=True)

                st.markdown("---"); st.subheader("🗑️ Elimina Evento Pianificato")
                tx_to_delete_id = st.selectbox("Seleziona un evento da eliminare", options=df_planned['id'],
                    format_func=lambda x: f"{df_planned.loc[df_planned['id'] == x, 'Data'].iloc[0]} - {df_planned.loc[df_planned['id'] == x, 'Descrizione'].iloc[0]} (€ {df_planned.loc[df_planned['id'] == x, 'Importo'].iloc[0]:.2f})",
                    disabled=is_viewer)
                if st.button("Elimina Evento Selezionato", type="primary", disabled=is_viewer):
                    delete_planned_tx(ws_id, tx_to_delete_id)
                    st.toast("Evento pianificato eliminato!", icon="🗑️"); st.cache_data.clear(); st.rerun()
            else: st.info("Nessun movimento pianificato inserito.")
    
    with tabs[3]: # Pianificatore Spese
        st.header("💡 Assistente Pianificazione Spese")
        st.info("Aggiungi i tuoi obiettivi di spesa futuri. L'assistente ti suggerirà quando potrai affrontarli.")
        c1, c2 = st.columns([1, 2])
        with c1:
            st.subheader("🎯 I Tuoi Obiettivi di Spesa")
            with st.form("add_goal_form", clear_on_submit=True):
                goal_desc = st.text_input("Descrizione Obiettivo (es. Nuovo Telefono)", disabled=is_viewer)
                goal_amount = st.number_input("Costo Previsto (€)", min_value=0.01, step=50.0, format="%.2f", disabled=is_viewer)
                if st.form_submit_button("Aggiungi Obiettivo", type="primary", disabled=is_viewer):
                    if goal_desc and goal_amount:
                        add_goal(ws_id, goal_desc, goal_amount)
                        st.toast("Obiettivo aggiunto!", icon="🎯"); st.rerun()
            
            st.markdown("---")
            pending_goals = get_goals(ws_id)
            if pending_goals:
                st.write("**Obiettivi in attesa:**")
                for goal_id, desc, amount in pending_goals:
                    cols = st.columns([4, 1])
                    cols[0].write(f"- {desc} (**{abs(amount):,.2f} €**)")
                    if cols[1].button("🗑️", key=f"del_goal_{goal_id}", disabled=is_viewer):
                        delete_goal(ws_id, goal_id); st.rerun()
            else: st.write("Nessun obiettivo di spesa in attesa.")

        with c2:
            st.subheader("⚙️ Imposta e Avvia Analisi")
            if not pending_goals:
                st.warning("Aggiungi almeno un obiettivo per avviare l'analisi.")
            else:
                with st.container(border=True):
                    standard_accounts = [acc[1] for acc in get_all_accounts(ws_id, with_details=True) if acc[2] == 'standard']
                    accounts = ["Tutti"] + standard_accounts
                    planner_account = st.selectbox("Analizza saldo su:", accounts, key="planner_acc")
                    safety_balance = st.number_input("Saldo minimo di sicurezza (€)", min_value=0.0, value=500.0, step=100.0)
                    planner_horizon = st.slider("Orizzonte di pianificazione (Mesi)", 1, 36, 12)

                    if st.button("Trova le date migliori per le mie spese 🚀", type="primary", use_container_width=True):
                        with st.spinner("Calcolo del piano in corso..."):
                            start_date, end_date = date.today(), date.today() + relativedelta(months=planner_horizon)
                            account_param = None if planner_account == "Tutti" else planner_account
                            initial_balance = get_balance_before_date(ws_id, start_date, account_param)
                            future_events = get_future_events(ws_id, start_date, end_date, account_param)
                            df_planner = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='D'), columns=['Date']).set_index('Date')
                            df_planner['Balance'] = 0.0
                            daily_deltas = defaultdict(float)
                            for event in future_events: daily_deltas[pd.to_datetime(event['date'])] += event['amount']
                            for dt, delta in daily_deltas.items():
                                if dt in df_planner.index: df_planner.loc[dt, 'Balance'] = delta
                            df_planner['Balance'] = df_planner['Balance'].cumsum() + initial_balance
                            
                            results = []
                            for goal_id, goal_desc, goal_amount in get_goals(ws_id):
                                best_date = None
                                for day in df_planner.index:
                                    temp_forecast = df_planner['Balance'].copy()
                                    temp_forecast.loc[day:] += goal_amount 
                                    if temp_forecast.loc[day:].min() >= safety_balance:
                                        best_date = day.date()
                                        df_planner['Balance'] = temp_forecast
                                        break
                                results.append({ "Obiettivo": goal_desc, "Costo": f"{abs(goal_amount):,.2f} €", "Data Suggerita": best_date.strftime('%d/%m/%Y') if best_date else "Non fattibile", "Stato": "✅ Fattibile" if best_date else "⚠️ Data non trovata" })
                            st.session_state.planner_results = results
                        st.rerun()
        
        if st.session_state.planner_results is not None:
            st.markdown("---"); st.subheader("📋 Il Tuo Piano d'Azione")
            st.dataframe(pd.DataFrame(st.session_state.planner_results), use_container_width=True, hide_index=True)
            if st.button("Pulisci Risultati"):
                st.session_state.planner_results = None; st.rerun()

    with tabs[4]: # Conti
        st.header("Gestione Conti")
        accounts_data = get_accounts_with_balance(ws_id)
        df_balances = pd.DataFrame(accounts_data, columns=["Nome", "Tipo", "Plafond", "Saldo/Residuo", "Da Pagare"])
        st.subheader("Lista dei Conti")
        df_display = df_balances.copy()
        df_display['Tipo'] = df_display['Tipo'].map({'standard': '🏦 Standard', 'credit_card': '💳 Carta di Credito'})
        df_display['Info'] = df_display.apply(lambda row: f"Plafond: € {row['Plafond']:,.2f}" if row['Tipo'] == '💳 Carta di Credito' else '', axis=1)
        df_display['Saldo'] = df_display.apply(lambda row: f"€ {row['Saldo/Residuo']:,.2f}" if row['Tipo'] == '🏦 Standard' else f"€ {abs(row['Da Pagare'] or 0.0):,.2f} (da pagare)", axis=1)
        st.dataframe(df_display[['Nome', 'Tipo', 'Saldo', 'Info']], use_container_width=True, hide_index=True)
        st.markdown("---")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Aggiungi / Modifica Conto")
            all_accounts_list = get_all_accounts(ws_id)
            account_to_manage = st.selectbox("Seleziona un conto da modificare o 'Nuovo Conto'", ["Nuovo Conto"] + all_accounts_list, disabled=is_viewer)
            is_new_account = account_to_manage == "Nuovo Conto"
            current_details = None if is_new_account else get_account_details_by_name(ws_id, account_to_manage)
            
            with st.form(f"manage_account_form_{account_to_manage}"):
                st.write(f"**Dettagli per: {account_to_manage}**")
                new_name = st.text_input("Nome Conto", value="" if is_new_account else current_details[0], disabled=is_viewer)
                acc_type = st.radio("Tipo di Conto", ["standard", "credit_card"], 
                                    index=0 if is_new_account or (current_details and current_details[2] == 'standard') else 1,
                                    format_func=lambda x: "Standard" if x == "standard" else "Carta di Credito", horizontal=True, disabled=is_viewer)
                
                if acc_type == 'standard':
                    opening_balance = st.number_input("Saldo Iniziale", value=0.0 if is_new_account or not current_details else current_details[1], disabled=is_viewer)
                    credit_limit, statement_day = None, None
                else:
                    credit_limit = st.number_input("Plafond / Limite di Credito (€)", min_value=0.0, value=1500.0 if is_new_account or not current_details else (current_details[3] or 1500.0), disabled=is_viewer)
                    statement_day = st.number_input("Giorno chiusura estratto conto (1-31)", min_value=1, max_value=31, value=25 if is_new_account or not current_details else (current_details[4] or 25), disabled=is_viewer)
                    opening_balance = 0.0

                if st.form_submit_button("Salva Conto", type="primary", disabled=is_viewer):
                    if new_name:
                        if is_new_account:
                            add_account(ws_id, new_name, opening_balance, acc_type, credit_limit, statement_day)
                            st.success(f"Conto '{new_name}' aggiunto!")
                        else:
                            update_account(ws_id, account_to_manage, new_name, opening_balance, acc_type, credit_limit, statement_day)
                            st.success(f"Conto '{account_to_manage}' aggiornato!")
                        st.cache_data.clear(); st.rerun()
                    else: st.warning("Il nome del conto è obbligatorio.")

            if not is_new_account:
                if st.button(f"Elimina Conto '{account_to_manage}'", type="primary", disabled=is_viewer):
                    delete_account(ws_id, account_to_manage)
                    st.success(f"Conto '{account_to_manage}' eliminato."); st.cache_data.clear(); st.rerun()

        with c2:
            st.subheader("Paga Estratto Conto Carta")
            credit_cards = [acc[1] for acc in get_all_accounts(ws_id, with_details=True) if acc[2] == 'credit_card']
            standard_accounts = [acc[1] for acc in get_all_accounts(ws_id, with_details=True) if acc[2] == 'standard']
            
            if not credit_cards or not standard_accounts:
                st.info("Devi avere almeno una carta di credito e un conto standard.")
            else:
                with st.form("pay_cc_bill_form"):
                    cc_to_pay = st.selectbox("Seleziona Carta di Credito da pagare", credit_cards, disabled=is_viewer)
                    paying_account = st.selectbox("Paga usando il conto", standard_accounts, disabled=is_viewer)
                    amount_to_pay_row = df_balances[df_balances['Nome'] == cc_to_pay]
                    amount_to_pay = abs(amount_to_pay_row['Da Pagare'].iloc[0]) if not amount_to_pay_row.empty and amount_to_pay_row['Da Pagare'].iloc[0] is not None else 0.0
                    payment_amount = st.number_input("Importo da pagare", value=amount_to_pay, min_value=0.0, disabled=is_viewer)
                    payment_date = st.date_input("Data Pagamento", date.today(), format="DD/MM/YYYY", disabled=is_viewer)

                    if st.form_submit_button("Registra Pagamento", type="primary", disabled=is_viewer):
                        if payment_amount > 0:
                            desc = f"Pagamento estratto conto {cc_to_pay}"
                            add_tx(ws_id, payment_date, paying_account, "Trasferimento", -payment_amount, desc)
                            add_tx(ws_id, payment_date, cc_to_pay, "Trasferimento", payment_amount, desc)
                            st.success("Pagamento registrato!"); st.cache_data.clear(); st.rerun()
                        else: st.warning("L'importo deve essere maggiore di zero.")

    with tabs[5]: # Debiti/Crediti
        st.header("Gestione Debiti e Crediti")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("➕ Aggiungi Nuovo")
            with st.form("debt_form", clear_on_submit=True):
                person = st.text_input("Persona", disabled=is_viewer)
                amount = st.number_input("Importo", min_value=0.01, format="%.2f", disabled=is_viewer)
                type = st.radio("Tipo", ('Ho prestato (Credito)', 'Mi hanno prestato (Debito)'), disabled=is_viewer)
                due_date = st.date_input("Data di scadenza", date.today(), format="DD/MM/YYYY", disabled=is_viewer)
                if st.form_submit_button("Aggiungi", disabled=is_viewer):
                    debt_type = 'lent' if type == 'Ho prestato (Credito)' else 'borrowed'
                    add_debt(ws_id, person, amount, debt_type, due_date)
                    st.success("Aggiunto!"); st.rerun()
        with col2:
            st.subheader("📋 In Sospeso")
            outstanding_debts = get_debts(ws_id, status='outstanding')
            if not outstanding_debts: st.info("Nessun debito o credito in sospeso.")
            else:
                accounts = [acc[1] for acc in get_all_accounts(ws_id, with_details=True) if acc[2] == 'standard']
                if not accounts and not is_viewer: st.warning("Crea un conto standard per saldare i debiti.")
                
                for debt in outstanding_debts:
                    debt_id, _, person, amount, type, due_date, status, created_at = debt
                    label_type, date_str = ("Credito", parse_date(due_date).strftime('%d/%m/%Y')) if type == 'lent' else ("Debito", parse_date(due_date).strftime('%d/%m/%Y'))
                    
                    with st.container(border=True):
                        st.markdown(f"**{label_type}** con **{person}** di **€ {amount:,.2f}** (Scad. {date_str})")
                        account_to_settle = st.selectbox("Scegli il conto per saldare", accounts, key=f"account_{debt_id}", disabled=is_viewer)
                        
                        b1, b2 = st.columns(2)
                        if b1.button("Segna come Saldato", key=f"settle_{debt_id}", use_container_width=True, disabled=is_viewer):
                            settle_debt(ws_id, debt_id, account_to_settle)
                            st.success("Operazione registrata!"); st.cache_data.clear(); st.rerun()
                        
                        if b2.button("Elimina", key=f"delete_{debt_id}", use_container_width=True, disabled=is_viewer):
                            delete_debt(ws_id, debt_id)
                            st.toast("Voce eliminata con successo!", icon="🗑️"); st.rerun()

    with tabs[6]: # Ricorrenze
        st.header("Movimenti Ricorrenti")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("➕ Aggiungi Ricorrenza")
            with st.form("add_recurring_form", clear_on_submit=True):
                rec_name = st.text_input("Nome Ricorrenza (es. Affitto, Stipendio)", disabled=is_viewer)
                rec_type = st.radio("Tipo", ["Uscita", "Entrata"], horizontal=True, disabled=is_viewer)
                rec_amount = st.number_input("Importo", min_value=0.01, step=0.01, format="%.2f", disabled=is_viewer)
                rec_start_date = st.date_input("Data di Inizio", format="DD/MM/YYYY", disabled=is_viewer)
                rec_interval = st.selectbox("Intervallo", options=["daily", "weekly", "monthly"], format_func=lambda x: x.capitalize(), disabled=is_viewer)
                accounts, categories = get_all_accounts(ws_id), get_all_categories(ws_id)
                rec_account = st.selectbox("Conto", options=accounts, disabled=is_viewer)
                rec_category = st.selectbox("Categoria", options=categories, disabled=is_viewer)
                rec_description = st.text_area("Descrizione (Opzionale)", disabled=is_viewer)
                
                if st.form_submit_button("Aggiungi Ricorrenza", disabled=is_viewer):
                    if rec_name and rec_account and rec_category:
                        final_amount = -abs(rec_amount) if rec_type == "Uscita" else abs(rec_amount)
                        add_recurring(ws_id, rec_name, rec_start_date, rec_interval, final_amount, rec_account, rec_category, rec_description)
                        st.toast("Ricorrenza aggiunta!", icon="✅"); st.cache_data.clear(); st.rerun()
                    else: st.warning("Nome, conto e categoria sono obbligatori.")
        
        with c2:
            st.subheader("Lista Ricorrenze Impostate")
            recs_data = get_recurring_transactions(ws_id)
            if not recs_data:
                st.info("Nessuna ricorrenza impostata.")
            else:
                df_recs = pd.DataFrame(recs_data, columns=["id", "Nome", "Data Inizio", "Intervallo", "Importo", "Conto", "Categoria", "Descrizione"])
                if not df_recs.empty:
                    df_recs['Data Inizio'] = pd.to_datetime(df_recs['Data Inizio']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_recs.drop(columns=['id']), use_container_width=True, hide_index=True)

                st.markdown("---"); st.subheader("🗑️ Elimina Ricorrenza")
                if not df_recs.empty:
                    rec_to_delete_id = st.selectbox("Seleziona ricorrenza da eliminare", options=df_recs['id'],
                        format_func=lambda x: f"{df_recs[df_recs['id']==x].iloc[0]['Nome']} - € {df_recs[df_recs['id']==x].iloc[0]['Importo']:.2f}",
                        disabled=is_viewer)
                    if st.button("Elimina Ricorrenza Selezionata", type="primary", disabled=is_viewer):
                        delete_recurring(ws_id, rec_to_delete_id)
                        st.toast("Ricorrenza eliminata!", icon="🗑️"); st.cache_data.clear(); st.rerun()
                else: st.info("Nessuna ricorrenza da eliminare.")

        st.markdown("---"); st.subheader("💡 Suggerimenti di Ricorrenze")
        st.info("L'app analizza i tuoi movimenti e trova possibili pattern ricorrenti.")
        with st.spinner("Analisi dei movimenti in corso..."):
            suggestions = find_recurring_suggestions(ws_id)

        if not suggestions:
            st.success("Nessun nuovo pattern ricorrente trovato.")
        else:
            for i, suggestion in enumerate(suggestions):
                desc, amount, interval, cat, acc, start_date_str = suggestion
                with st.container(border=True):
                    st.subheader(f"🧾 {desc}")
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.metric("Importo Medio Rilevato", f"€ {abs(amount):.2f}", delta="Uscita" if amount < 0 else "Entrata")
                        st.caption(f"🗓️ **Frequenza:** {interval.capitalize()} | 📂 **Categoria:** {cat} | 🏦 **Conto:** {acc}")
                    with col2:
                        st.write("")
                        if st.button("➕ Aggiungi", key=f"add_sugg_{i}", type="primary", use_container_width=True, disabled=is_viewer):
                            add_recurring(ws_id, desc, parse_date(start_date_str), interval, round(amount, 2), acc, cat, "Ricorrenza generata da suggerimento.")
                            st.toast(f"Ricorrenza '{desc}' aggiunta!", icon="✅"); st.cache_data.clear(); st.rerun()

    with tabs[7]: # Budget
        st.header("Analisi e Gestione Budget")
        year = st.number_input("Seleziona Anno", min_value=2020, max_value=2100, value=date.today().year)
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("➕ Aggiungi / Modifica Voce di Budget")
            with st.form("add_budget_form", clear_on_submit=True):
                month_map = {i: calendar.month_name[i] for i in range(1, 13)}
                budget_month = st.selectbox("Mese", options=list(month_map.keys()), format_func=lambda x: month_map[x], disabled=is_viewer)
                expense_categories = [cat[1] for cat in get_all_categories_with_types(ws_id) if cat[2] == 'expense']
                budget_category = st.selectbox("Categoria di Spesa", options=expense_categories, disabled=is_viewer)
                accounts_for_budget = ["Tutti i conti"] + get_all_accounts(ws_id)
                budget_account = st.selectbox("Conto di Riferimento", options=accounts_for_budget, disabled=is_viewer)
                budget_amount = st.number_input("Importo Budget (Spesa Prevista)", min_value=0.01, step=10.0, disabled=is_viewer)
                
                if st.form_submit_button("Salva Budget", disabled=is_viewer):
                    add_budget(ws_id, year, budget_month, budget_category, budget_account, budget_amount)
                    st.toast("Voce di budget salvata!", icon="💰"); st.cache_data.clear(); st.rerun()
            
            budgets_data_for_deletion = get_budgets_by_year(ws_id, year)
            if budgets_data_for_deletion:
                st.subheader("🗑️ Elimina Voce di Budget")
                df_budget_list = pd.DataFrame(budgets_data_for_deletion, columns=["id", "Mese", "Categoria", "Conto", "Budget (€)"])
                budget_to_delete_id = st.selectbox("Seleziona budget da eliminare", options=df_budget_list['id'],
                    format_func=lambda x: f"{calendar.month_name[df_budget_list.loc[df_budget_list['id']==x, 'Mese'].iloc[0]]} - {df_budget_list.loc[df_budget_list['id']==x, 'Categoria'].iloc[0]} ({df_budget_list.loc[df_budget_list['id']==x, 'Conto'].iloc[0]})",
                    disabled=is_viewer)
                if st.button("Elimina Budget Selezionato", type="primary", disabled=is_viewer):
                    delete_budget(ws_id, budget_to_delete_id)
                    st.toast("Voce di budget eliminata!", icon="🗑️"); st.cache_data.clear(); st.rerun()
        
        with c2:
            st.subheader(f"Analisi Budget - {year}")
            budgets_data = get_budgets_by_year(ws_id, year)
            if not budgets_data:
                st.info(f"Nessun budget impostato per il {year}.")
            else:
                actual_expenses_dict = get_actual_expenses_by_year(ws_id, year)
                df_budget = pd.DataFrame(budgets_data, columns=["id", "Mese", "Categoria", "Conto", "Budget (€)"])
                df_budget["Spesa Reale (€)"] = df_budget.apply(lambda row: actual_expenses_dict.get((row["Mese"], row["Categoria"], row["Conto"]), 0.0), axis=1)
                df_budget["Scostamento (€)"] = df_budget["Budget (€)"] - df_budget["Spesa Reale (€)"]
                
                accounts_for_filter = ["Tutti i conti"] + get_all_accounts(ws_id)
                account_filter = st.selectbox("Filtra Risultati per Conto", options=accounts_for_filter, key="budget_filter")
                df_display = df_budget[df_budget['Conto'].isin([account_filter, 'Tutti i conti'])].copy() if account_filter != "Tutti i conti" else df_budget.copy()
                
                if not df_display.empty:
                    df_display = df_display.sort_values(by=["Mese", "Categoria"]).reset_index(drop=True)
                    df_display['Mese'] = df_display['Mese'].astype(int).apply(lambda x: calendar.month_name[x])
                    st.dataframe(df_display[['Mese', 'Categoria', 'Conto', 'Budget (€)', 'Spesa Reale (€)', 'Scostamento (€)']], use_container_width=True, hide_index=True)
                    df_chart = df_display.melt(id_vars=['Mese', 'Categoria'], value_vars=['Budget (€)', 'Spesa Reale (€)'], var_name='Tipo', value_name='Importo')
                    fig = px.bar(df_chart, x="Categoria", y="Importo", color="Tipo", barmode="group", facet_col="Mese", facet_col_wrap=4, title=f"Confronto Budget vs. Spesa Reale - {year}", height=500)
                    st.plotly_chart(fig, use_container_width=True)
                else: st.info("Nessun dato di budget da visualizzare per i filtri.")

    with tabs[8]: # Forecast
        st.header("📈 Forecast Evoluto (Saldi a Fine Mese)")
        st.info("Il forecast analizza solo i conti standard per proiettare la liquidità futura.")
        c1, c2 = st.columns(2)
        standard_accounts = [acc[1] for acc in get_all_accounts(ws_id, with_details=True) if acc[2] == 'standard']
        accounts = ["Tutti"] + standard_accounts
        filter_account = c1.selectbox("Conto per forecast", accounts, key="fc_acc")
        months_to_project = c2.slider("Mesi di proiezione", 1, 24, 6)

        st.markdown("---")
        start_date, end_date = date.today(), date.today() + relativedelta(months=months_to_project)
        account_param = None if filter_account == "Tutti" else filter_account
        current_balance = get_balance_before_date(ws_id, start_date, account_param)
        future_events = get_future_events(ws_id, start_date, end_date, account_param)
        
        monthly_flows = defaultdict(lambda: {'income': 0.0, 'expense': 0.0})
        for event in future_events:
            month_key = event['date'].strftime("%Y-%m")
            if event['amount'] > 0: monthly_flows[month_key]['income'] += event['amount']
            else: monthly_flows[month_key]['expense'] += event['amount']
        
        forecast_data = [{"Mese": "Saldo Attuale", "Entrate Previste": 0, "Uscite Previste": 0, "Flusso Netto": 0, "Saldo a Fine Mese": current_balance}]
        
        running_balance = current_balance
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

        fig = px.line(df_forecast, x="Mese", y="Saldo a Fine Mese", title="Evoluzione Prevista del Saldo", markers=True, text="Saldo a Fine Mese")
        fig.update_traces(texttemplate='%{text:,.2f} €', textposition='top center')
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Riepilogo Flussi di Cassa Previsti")
        st.dataframe(df_forecast[df_forecast['Mese'] != 'Saldo Attuale'], use_container_width=True, hide_index=True)

        with st.expander("Dettaglio Eventi Inclusi nella Previsione"):
            df_events = pd.DataFrame(future_events)
            if not df_events.empty:
                df_display = df_events[['date', 'description', 'category', 'amount']].copy()
                df_display.rename(columns={'date': 'Data', 'description': 'Descrizione', 'category': 'Categoria', 'amount': 'Importo (€)'}, inplace=True)
                df_display['Data'] = pd.to_datetime(df_display['Data']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_display, hide_index=True, use_container_width=True,
                    column_config={ "Importo (€)": st.column_config.NumberColumn(format="€ %.2f") }
                )
            else:
                st.info("Nessun evento futuro da mostrare.")
    
    with tabs[9]: # Impostazioni
        st.header("⚙️ Impostazioni")
        
        st.subheader("Configurazione Categorie")
        with st.container(border=True):
            df_cat = pd.DataFrame(get_all_categories_with_types(ws_id), columns=['id', 'Nome', 'Tipo'])
            st.dataframe(df_cat[['Nome', 'Tipo']], use_container_width=True, hide_index=True)
            
            with st.expander("⬆️ Importa Categorie da File"):
                cat_file = st.file_uploader("Carica file Categorie", type=["xlsx", "csv"], key="cat_uploader", disabled=is_viewer)

                if cat_file and st.session_state.df_cat_import_preview is None:
                    try:
                        df_import = pd.read_csv(cat_file) if cat_file.name.endswith('.csv') else pd.read_excel(cat_file, engine='openpyxl')
                        st.info("File caricato. Ora mappa le colonne per l'importazione.")

                        with st.form("category_mapping_form"):
                            st.write("**Passo 1: Mappatura Colonne**")
                            cols = df_import.columns.tolist()
                            c1, c2 = st.columns(2)
                            name_col = c1.selectbox("Seleziona la colonna NOME", cols, index=0)
                            type_col = c2.selectbox("Seleziona la colonna TIPO", cols, index=1 if len(cols) > 1 else 0)
                            
                            if st.form_submit_button("Prepara per l'Importazione", type="primary"):
                                df_clean = df_import[[name_col, type_col]].copy()
                                df_clean.columns = ['nome', 'tipo']
                                df_clean['nome'] = df_clean['nome'].astype(str).str.strip()
                                df_clean['tipo'] = df_clean['tipo'].astype(str).str.strip().str.lower()
                                df_clean = df_clean[df_clean['tipo'].isin(['income', 'expense'])]
                                df_clean.dropna(subset=['nome'], inplace=True)
                                df_clean = df_clean[df_clean['nome'] != '']
                                st.session_state.df_cat_import_preview = df_clean.to_dict('records')
                                st.rerun()
                    
                    except Exception as e:
                        st.error(f"Errore nella lettura del file: {e}")

                if st.session_state.df_cat_import_preview is not None:
                    st.write("**Passo 2: Revisione e Conferma**")
                    df_preview = pd.DataFrame(st.session_state.df_cat_import_preview)
                    
                    if not df_preview.empty:
                        st.dataframe(df_preview, use_container_width=True)
                        c1, c2 = st.columns(2)
                        if c1.button("✅ Conferma Importazione", type="primary", use_container_width=True):
                            cats_to_add = list(df_preview.itertuples(index=False, name=None))
                            bulk_add_categories(ws_id, cats_to_add)
                            st.success(f"{len(cats_to_add)} categorie importate/aggiornate!")
                            st.session_state.df_cat_import_preview = None
                            st.cache_data.clear()
                            st.rerun()
                        if c2.button("❌ Annulla", use_container_width=True):
                            st.session_state.df_cat_import_preview = None
                            st.rerun()
                    else:
                        st.warning("Nessuna categoria valida trovata nel file dopo la pulizia. Controlla che i tipi siano 'income' o 'expense'.")
                        if st.button("Riprova"):
                            st.session_state.df_cat_import_preview = None
                            st.rerun()

            op_tabs = st.tabs(["➕ Aggiungi", "✏️ Modifica", "🗑️ Elimina"])
            with op_tabs[0]:
                with st.form("add_cat_form", clear_on_submit=True):
                    new_cat_name = st.text_input("Nome Nuova Categoria")
                    new_cat_type = st.selectbox("Tipo Categoria", options=['expense', 'income'], format_func=lambda x: "Spesa" if x == 'expense' else "Entrata")
                    if st.form_submit_button("Aggiungi Categoria", type="primary", disabled=is_viewer):
                        if new_cat_name:
                            success, message = add_category(ws_id, new_cat_name, new_cat_type)
                            if success: st.toast("Categoria aggiunta!", icon="✅"); st.cache_data.clear(); st.rerun()
                            else: st.error(message)
            with op_tabs[1]:
                if not df_cat.empty:
                    cat_to_edit_id = st.selectbox("Seleziona categoria da modificare", options=df_cat['id'], format_func=lambda x: df_cat[df_cat['id'] == x]['Nome'].iloc[0], key="edit_cat_select", disabled=is_viewer)
                    selected_cat = df_cat[df_cat['id'] == cat_to_edit_id].iloc[0]
                    with st.form(f"edit_cat_form_{cat_to_edit_id}"):
                        edit_cat_name = st.text_input("Nuovo Nome", value=selected_cat['Nome'], disabled=is_viewer)
                        edit_cat_type = st.selectbox("Nuovo Tipo", options=['expense', 'income'], index=0 if selected_cat['Tipo'] == 'expense' else 1, format_func=lambda x: "Spesa" if x == 'expense' else "Entrata", disabled=is_viewer)
                        if st.form_submit_button("Salva Modifiche", type="primary", disabled=is_viewer):
                            success, message = update_category(ws_id, cat_to_edit_id, edit_cat_name, edit_cat_type)
                            if success: st.toast("Categoria aggiornata!", icon="🔄"); st.cache_data.clear(); st.rerun()
                            else: st.error(message)
            with op_tabs[2]:
                if not df_cat.empty:
                    cat_to_delete_id = st.selectbox("Seleziona categoria da eliminare", options=df_cat['id'], format_func=lambda x: df_cat[df_cat['id'] == x]['Nome'].iloc[0], key="delete_cat_select_2", disabled=is_viewer)
                    if st.button("Elimina Categoria Selezionata", type="primary", key="delete_cat_button", disabled=is_viewer):
                        success, message = delete_category(ws_id, cat_to_delete_id)
                        if success: st.toast("Categoria eliminata!", icon="🗑️"); st.cache_data.clear(); st.rerun()
                        else: st.error(message)
        
        st.markdown("---")
        st.subheader("Automazione e Apprendimento")
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                with st.expander("Gestisci Regole per Parola Chiave"):
                    df_rules = pd.DataFrame(get_rules(ws_id), columns=["id", "Parola Chiave", "Categoria Assegnata"])
                    st.dataframe(df_rules.drop(columns=['id']), use_container_width=True, hide_index=True)
                    with st.form("add_rule_form", clear_on_submit=True):
                        keyword = st.text_input("Se la descrizione contiene...", disabled=is_viewer)
                        category = st.selectbox("Assegna la categoria...", get_all_categories(ws_id), key="rule_cat", disabled=is_viewer)
                        if st.form_submit_button("Aggiungi Regola", disabled=is_viewer):
                            if keyword and category: add_rule(ws_id, keyword, category); st.success("Regola aggiunta!"); st.rerun()
            with c2:
                st.write("**Categorizzazione Automatica (AI)**")
                if st.button("🧠 Allena Modello", use_container_width=True, disabled=is_viewer):
                    with st.spinner("Allenamento in corso..."):
                        training_data = get_transactions_for_training(ws_id)
                        success, message = train_model(ws_id, training_data)
                        if success: st.success(f"Modello allenato con successo!")
                        else: st.error(message)
                
                test_description = st.text_input("Inserisci una descrizione di test:")
                if test_description:
                    prediction = predict_single(ws_id, test_description)
                    if prediction: st.success(f"Categoria Predetta: **{prediction}**")

        st.markdown("---")
        st.subheader("Importazione Dati")
        with st.container(border=True):
            st.write("**Importa Movimenti da File Excel**")
            excel_file = st.file_uploader("Carica file Excel", type=["xlsx", "xls"], key="excel_uploader", disabled=is_viewer)
            if excel_file and st.session_state.df_import_review is None:
                # ... [Logica import movimenti invariata] ...
                pass

        st.markdown("---")
        st.subheader("Manutenzione")
        with st.container(border=True):
            st.write("**Pulizia Dati**")
            st.info("Questo comando elimina le categorie non utilizzate in nessun movimento.")
            if st.button("Pulisci Categorie non Utilizzate", disabled=is_viewer):
                deleted_count = delete_unused_categories(ws_id)
                st.success(f"{deleted_count} categorie eliminate.") if deleted_count > 0 else st.info("Nessuna categoria da eliminare.")
                st.cache_data.clear(); st.rerun()

            with st.expander("⚠️ Area Pericolosa: Reset Globale"):
                st.warning("Cancellerà TUTTI i dati di TUTTI gli utenti tranne gli account. Usare con cautela.")
                if st.button("Inizializza Database Adesso", type="primary", disabled=is_viewer):
                    reset_db(); st.success("Database re-inizializzato!"); st.cache_data.clear(); st.rerun()

# --- SCHERMATA DI LOGIN ---
def login_screen():
    st.set_page_config(page_title="Cashflow Pro - Accesso", layout="centered")
    load_css(CSS_FILE)
    
    with st.container():
        st.title("💰 Cashflow Pro")
        st.markdown("Accedi per gestire le tue finanze")
        
        user_count = auth.get_user_count()
        if user_count == 0:
            st.info("Benvenuto! Crea il primo account per iniziare.")
            st.session_state.login_page = "Crea Account"
        else:
            if 'login_page' not in st.session_state: st.session_state.login_page = "Login"
            st.session_state.login_page = st.radio("Scegli un'azione", ["Login", "Crea Account", "Recupera Password"], 
                horizontal=True, key="login_nav", index=["Login", "Crea Account", "Recupera Password"].index(st.session_state.login_page))

        if st.session_state.login_page == "Login":
            with st.form("login_form"):
                username = st.text_input("Nome Utente", placeholder="mario.rossi")
                password = st.text_input("Password", type="password", placeholder="********")
                if st.form_submit_button("Login", type="primary", use_container_width=True):
                    if auth.authenticate_user(username, password):
                        user_id = auth.get_user_id(username)
                        workspaces = auth.get_user_workspaces(user_id)
                        
                        if not workspaces:
                            ws_id = auth.create_workspace(user_id, f"Workspace di {username}")
                            populate_new_workspace(ws_id)
                            workspaces = auth.get_user_workspaces(user_id)

                        st.session_state.update({
                            'authenticated': True, 'username': username, 'user_id': user_id,
                            'workspaces': workspaces,
                            'current_workspace_id': workspaces[0][0]
                        })
                        st.toast(f"Bentornato, {username}!", icon="👋"); st.rerun()
                    else: st.error("Credenziali non valide. Riprova.")

        elif st.session_state.login_page == "Crea Account":
            with st.form("signup_form", clear_on_submit=True):
                st.subheader("Crea un nuovo account")
                new_username = st.text_input("Scegli un Nome Utente")
                new_password = st.text_input("Scegli una Password (almeno 8 caratteri)", type="password")
                confirm_password = st.text_input("Conferma Password", type="password")
                st.markdown("---"); st.write("**Imposta il recupero password**")
                security_questions = ["Qual era il nome del tuo primo animale domestico?", "Qual è il cognome da nubile di tua madre?", "In quale città sei nato?", "Qual è il tuo film preferito?"]
                question = st.selectbox("Scegli una domanda di sicurezza", security_questions)
                answer = st.text_input("Scrivi la risposta", type="password")

                if st.form_submit_button("Registrati", type="primary", use_container_width=True):
                    if new_password != confirm_password: st.error("Le password non coincidono.")
                    else:
                        success, message = auth.create_user(new_username, new_password, question, answer)
                        if success: 
                            new_user_id = auth.get_user_id(new_username)
                            new_workspaces = auth.get_user_workspaces(new_user_id)
                            if new_workspaces: populate_new_workspace(new_workspaces[0][0])
                            st.success(message)
                            st.session_state.login_page = "Login"; st.rerun()
                        else: st.error(message)

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
    auth.create_auth_schema()
    init_db()

    if st.session_state.authenticated:
        show_main_dashboard()
    else:
        login_screen()
