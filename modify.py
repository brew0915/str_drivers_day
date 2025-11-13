import pandas as pd
import streamlit as st
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder
from google.oauth2.service_account import Credentials
import gspread
import re

# =====================================================
# 1. CONFIGURAÇÕES GERAIS
# =====================================================
st.set_page_config(page_title="Dashboard Motoristas - Shopee", layout="wide")
st.title("📊 Dashboard Drivers (OFERTA + CARREG + CADASTRO + ATUALIZAÇÃO)")

SERVICE_ACCOUNT_FILE = "credentials.json"
SHEET_ID = "1PwudX5L5c_zuQJXSCzAyZSdxTVRY0MMcqzGqS-up7nw"
ABA_OFERTA = "SHEET_OFERTA"
ABA_CARREG = "SHEET_CARREG"
ABA_CADASTRO = "SHEET_CADASTRO"          # ✅ Base fixa
ABA_ATUALIZAR_CAD = "SHEET_ATUALIZAR_CAD"  # ✅ Atualização recorrente

# =====================================================
# 2. CONEXÃO COM GOOGLE SHEETS
# =====================================================
@st.cache_resource
def conectar_sheets():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    cliente = gspread.authorize(creds)
    return cliente

# =====================================================
# 3. NORMALIZAÇÃO DE COLUNAS
# =====================================================
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df

# =====================================================
# 4. CARREGAMENTO DOS DADOS (OFERTA + CARREG + CADASTROS)
# =====================================================
@st.cache_data(ttl=1800)
def carregar_dados():
    cliente = conectar_sheets()

    # --------------------------
    # SHEET_OFERTA
    # --------------------------
    plan_oferta = cliente.open_by_key(SHEET_ID).worksheet(ABA_OFERTA)
    dados_oferta = pd.DataFrame(plan_oferta.get_all_records())
    df_oferta = normalizar_colunas(dados_oferta)

    colunas_fixas = ["driver_id", "driver_name", "cluster", "vehicle_type", "no_show_time"]
    colunas_fixas = [c for c in colunas_fixas if c in df_oferta.columns]
    colunas_datas = [c for c in df_oferta.columns if c not in colunas_fixas]

    df_long = df_oferta.melt(
        id_vars=colunas_fixas,
        value_vars=colunas_datas,
        var_name="data",
        value_name="status"
    )
    df_long["data"] = pd.to_datetime(df_long["data"], errors="coerce")
    df_long = df_long.dropna(subset=["data"])

    def verificar_disponibilidade_e_turno(valor):
        if pd.isna(valor) or str(valor).strip() in ["", "--", "Not Available"]:
            return 0, "Sem Oferta"
        texto = str(valor)
        if "05:15-09:00" in texto:
            return 1, "AM"
        elif "11:45-14:30" in texto:
            return 1, "PM1"
        else:
            return 0, "Sem Oferta"

    df_long[["disponivel", "turno"]] = df_long["status"].apply(
        lambda x: pd.Series(verificar_disponibilidade_e_turno(x))
    )

    if "cluster" in df_long.columns:
        df_long["cluster_individual"] = df_long["cluster"].apply(lambda x: [c.strip() for c in str(x).split(",")])
        df_long = df_long.explode("cluster_individual")
        df_long["cluster_individual"] = df_long["cluster_individual"].str.replace(r"^\d+\.\s*", "", regex=True)
    else:
        df_long["cluster_individual"] = None

    # --------------------------
    # SHEET_CARREG
    # --------------------------
    plan_carreg = cliente.open_by_key(SHEET_ID).worksheet(ABA_CARREG)
    dados_carreg = pd.DataFrame(plan_carreg.get_all_records())
    df_carreg = normalizar_colunas(dados_carreg)

    driver_id_col = next((c for c in df_carreg.columns if "driver_id" in c), None)
    driver_name_col = next((c for c in df_carreg.columns if "driver_name" in c or "driver_nome" in c), None)
    delivery_col = "delivery_date" if "delivery_date" in df_carreg.columns else None

    if delivery_col:
        df_carreg["delivery_date"] = pd.to_datetime(df_carreg[delivery_col], errors="coerce")
        df_carreg = df_carreg.dropna(subset=["delivery_date"])
        df_carreg["dia_carregado"] = df_carreg["delivery_date"].dt.date
        dias_carregados_df = (
            df_carreg.groupby(["driver_id", "driver_name"])["dia_carregado"]
            .nunique()
            .reset_index(name="dias_carregado")
        )
    else:
        dias_carregados_df = pd.DataFrame(columns=["driver_id", "driver_name", "dias_carregado"])

    # --------------------------
    # SHEET_CADASTRO (base fixa)
    # --------------------------
    plan_cadastro = cliente.open_by_key(SHEET_ID).worksheet(ABA_CADASTRO)
    df_cadastro = normalizar_colunas(pd.DataFrame(plan_cadastro.get_all_records()))
    df_cadastro = df_cadastro.drop_duplicates(subset=["driver_id", "driver_name"])

    # --------------------------
    # SHEET_ATUALIZAR_CAD (base nova)
    # --------------------------
    plan_atualizar = cliente.open_by_key(SHEET_ID).worksheet(ABA_ATUALIZAR_CAD)
    df_atualizar = normalizar_colunas(pd.DataFrame(plan_atualizar.get_all_records()))
    df_atualizar = df_atualizar.drop_duplicates(subset=["driver_id", "driver_name"])

    # --------------------------
    # COMPARAÇÃO ENTRE BASES
    # --------------------------
    novos_motoristas_base = df_atualizar[~df_atualizar["driver_id"].isin(df_cadastro["driver_id"])]
    removidos_base = df_cadastro[~df_cadastro["driver_id"].isin(df_atualizar["driver_id"])]

    # --------------------------
    # RESUMO OFERTA/CARREGAMENTO
    # --------------------------
    dias_ofertados = (
        df_long[df_long["disponivel"] == 1]
        .groupby(["driver_id", "driver_name", "data"])["disponivel"]
        .max()
        .reset_index()
    )

    resumo = df_long.groupby(
        ["driver_id", "driver_name", "vehicle_type", "no_show_time"]
    ).agg(total_dias=("data", "nunique")).reset_index()

    dias_disponiveis = dias_ofertados.groupby(["driver_id", "driver_name"]).agg(
        dias_disponivel=("data", "nunique")
    ).reset_index()

    resumo = resumo.merge(dias_disponiveis, on=["driver_id", "driver_name"], how="left").fillna({"dias_disponivel": 0})
    resumo["dias_disponivel"] = resumo["dias_disponivel"].astype(int)
    resumo["dias_sem_ofertar"] = resumo["total_dias"] - resumo["dias_disponivel"]

    def max_consecutivos(grupo):
        grupo = grupo.sort_values("data")
        faltou = (grupo["disponivel"] == 0).astype(int)
        max_seq = seq = 0
        for f in faltou:
            if f == 1:
                seq += 1
                max_seq = max(max_seq, seq)
            else:
                seq = 0
        return max_seq

    seq_inatividade = df_long.groupby("driver_name").apply(max_consecutivos).reset_index(name="max_dias_sem_ofertar")
    resumo = resumo.merge(seq_inatividade, on="driver_name", how="left")

    resumo = resumo.merge(dias_carregados_df, on=["driver_id", "driver_name"], how="left")
    resumo["dias_carregado"] = resumo["dias_carregado"].fillna(0).astype(int)
    resumo["oferta_x_carregamento_%"] = (
        (resumo["dias_carregado"] / resumo["dias_disponivel"].replace(0, pd.NA)) * 100
    ).fillna(0).round(1)

    def classificar_motorista(row):
        if row["dias_disponivel"] == 0:
            return "Inativo"
        elif row["dias_sem_ofertar"] > 14:
            return "Risco de Churn"
        elif row["dias_disponivel"] > row["total_dias"] * 0.5:
            return "Engajado"
        else:
            return "Intermediário"

    resumo["categoria"] = resumo.apply(classificar_motorista, axis=1)

    return resumo, df_long, df_cadastro, df_atualizar, novos_motoristas_base, removidos_base

# =====================================================
# 5. EXECUÇÃO
# =====================================================
try:
    resumo, df_long, df_cadastro, df_atualizar, novos_motoristas_base, removidos_base = carregar_dados()
    st.success("✅ Dados carregados com sucesso das abas SHEET_OFERTA, SHEET_CARREG, SHEET_CADASTRO e SHEET_ATUALIZAR_CAD!")
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    st.stop()

# =====================================================
# 6. COMPARAÇÃO DE BASES
# =====================================================
st.subheader("🧾 Comparativo de Bases de Motoristas")

col1, col2, col3 = st.columns(3)
col1.metric("Motoristas na Base Fixa", len(df_cadastro))
col2.metric("Motoristas na Base Atualizada", len(df_atualizar))
col3.metric("Novos Detectados", len(novos_motoristas_base))

if len(novos_motoristas_base) > 0:
    st.warning("⚠️ Novos motoristas identificados na atualização:")
    st.dataframe(novos_motoristas_base[["driver_id", "driver_name"]])
else:
    st.success("✅ Nenhum novo motorista encontrado.")

if len(removidos_base) > 0:
    st.error("🚫 Motoristas que saíram da base atualizada:")
    st.dataframe(removidos_base[["driver_id", "driver_name"]])

# =====================================================
# (continua todo o restante do dashboard normal: filtros, KPIs, gráficos, etc.)
# =====================================================
