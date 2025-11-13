# app.py
import pandas as pd
import streamlit as st
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder
from google.oauth2.service_account import Credentials
import gspread
import re
from typing import Tuple, List

# =====================================================
# 1. CONFIGURAÇÕES GERAIS
# =====================================================
st.set_page_config(page_title="Dashboard Motoristas - Shopee", layout="wide")
st.title("📊 Dashboard Drivers (SHEET_OFERTA + SHEET_CARREG + CADASTRO)")

SERVICE_ACCOUNT_FILE = "credentials.json"  # <-- confirme que esse arquivo existe
SHEET_ID = "1PwudX5L5c_zuQJXSCzAyZSdxTVRY0MMcqzGqS-up7nw"

# Abas
ABA_OFERTA = "SHEET_OFERTA"
ABA_CARREG = "SHEET_CARREG"
ABA_CADASTRO = "BASE_CADASTRO"            # aba fixa onde escreveremos 'contato'
ABA_ATUALIZAR = "SHEET_ATUALIZAR_CAD"

# =====================================================
# 2. UTILIDADES
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

def detectar_coluna_telefone(cols: List[str]) -> str:
    """Procura nomes comuns para telefone e retorna o nome normalizado."""
    cand = [c.lower().strip() for c in cols]
    if "phone_number" in cand:
        return cols[cand.index("phone_number")]
    for opt in ("phone number", "phone", "telefone", "telefone_celular", "celular"):
        if opt in cand:
            return cols[cand.index(opt)]
    # fallback: procura coluna que contenha 'phone' ou 'tel'
    for i, c in enumerate(cand):
        if "phone" in c or "tel" in c:
            return cols[i]
    return None

# =====================================================
# 3. CONEXÃO COM GOOGLE SHEETS
# =====================================================
@st.cache_resource
def conectar_sheets():
    # Usamos escopo de spreadsheets completo para leitura/escrita (se necessário)
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    cliente = gspread.authorize(creds)
    return cliente

# =====================================================
# 4. CARREGAMENTO E TRATAMENTO DOS DADOS
# =====================================================
@st.cache_data(ttl=1800)
def carregar_dados() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cliente = conectar_sheets()

    # ---------- SHEET_OFERTA ----------
    plan_oferta = cliente.open_by_key(SHEET_ID).worksheet(ABA_OFERTA)
    dados_oferta = pd.DataFrame(plan_oferta.get_all_records())
    df_oferta = normalizar_colunas(dados_oferta)

    # colunas fixas esperadas (ajustamos para o que existe realmente)
    colunas_fixas = ["driver_id", "driver_name", "cluster", "vehicle_type", "no_show_time"]
    colunas_fixas = [c for c in colunas_fixas if c in df_oferta.columns]
    colunas_datas = [c for c in df_oferta.columns if c not in colunas_fixas]

    # evitar naming collision no melt
    value_col_name = "status"
    i = 1
    while value_col_name in df_oferta.columns:
        value_col_name = f"status_{i}"
        i += 1

    df_long = df_oferta.melt(
        id_vars=colunas_fixas,
        value_vars=colunas_datas,
        var_name="data",
        value_name=value_col_name
    )
    # renomear para 'status' internamente
    df_long = df_long.rename(columns={value_col_name: "status"})

    df_long["data"] = pd.to_datetime(df_long["data"], errors="coerce")
    df_long = df_long.dropna(subset=["data"])

    # Disponibilidade e turno
    def verificar_disponibilidade_e_turno(valor):
        if pd.isna(valor) or str(valor).strip() in ["", "--", "Not Available"]:
            return 0, "Sem Oferta"
        texto = str(valor)
        # aceita se aparece o horário (string exata)
        am = "05:15-09:00"
        pm1 = "11:45-14:30"
        has_am = am in texto
        has_pm1 = pm1 in texto
        # um mesmo campo pode conter ambos; vamos tratar em flags separadas:
        if has_am and has_pm1:
            # caso o status contenha ambos, retornamos Disponível e marcar como "AM|PM1"
            return 1, "AM|PM1"
        if has_am:
            return 1, "AM"
        if has_pm1:
            return 1, "PM1"
        # else: se contém algum horário diferente mas regex tem, considerar disponivel
        if re.search(r"\d{2}:\d{2}-\d{2}:\d{2}", texto):
            return 1, "Outro"
        return 0, "Sem Oferta"

    # aplica e cria colunas
    df_long[["disponivel", "turno"]] = df_long["status"].apply(
        lambda x: pd.Series(verificar_disponibilidade_e_turno(x))
    )

    # Explodir clusters em linhas separadas para filtro por cluster
    if "cluster" in df_long.columns:
        df_long["cluster_individual"] = df_long["cluster"].apply(lambda x: [c.strip() for c in str(x).split(",")])
        df_long = df_long.explode("cluster_individual")
        # limpar prefixos numéricos "01. NOME"
        df_long["cluster_individual"] = df_long["cluster_individual"].str.replace(r"^\d+\.\s*", "", regex=True)
    else:
        df_long["cluster_individual"] = None

    # ---------- SHEET_CARREG ----------
    plan_carreg = cliente.open_by_key(SHEET_ID).worksheet(ABA_CARREG)
    dados_carreg = pd.DataFrame(plan_carreg.get_all_records())
    df_carreg = normalizar_colunas(dados_carreg)

    # identificar coluna de data / driver
    # tentativas comuns:
    delivery_col = None
    for cand in ["delivery_date", "date", "data_entrega", "task_date", "task_at_date"]:
        if cand in df_carreg.columns:
            delivery_col = cand
            break

    # driver columns detection fallback
    driver_id_col = None
    driver_name_col = None
    for c in df_carreg.columns:
        lc = c.lower()
        if "driver_id" in lc:
            driver_id_col = c
        if "driver_name" in lc or "driver_nome" in lc or "driver" == lc:
            driver_name_col = c
    # Normalize presence
    if driver_id_col is None and "driver_id" in df_carreg.columns:
        driver_id_col = "driver_id"
    if driver_name_col is None and "driver_name" in df_carreg.columns:
        driver_name_col = "driver_name"

    if delivery_col and driver_id_col and driver_name_col:
        df_carreg[delivery_col] = pd.to_datetime(df_carreg[delivery_col], errors="coerce")
        df_carreg = df_carreg.dropna(subset=[delivery_col])
        df_carreg["dia_carregado"] = df_carreg[delivery_col].dt.date
        dias_carregados_df = (
            df_carreg.groupby([driver_id_col, driver_name_col])["dia_carregado"]
            .nunique()
            .reset_index()
            .rename(columns={driver_id_col: "driver_id", driver_name_col: "driver_name", "dia_carregado": "dias_carregado"})
        )
    else:
        # se não encontrou colunas suficientes, criar df vazio com colunas esperadas
        dias_carregados_df = pd.DataFrame(columns=["driver_id", "driver_name", "dias_carregado"])

    # ---------- SHEET_CADASTRO e SHEET_ATUALIZAR ----------
    plan_cadastro = cliente.open_by_key(SHEET_ID).worksheet(ABA_CADASTRO)
    dados_cadastro = plan_cadastro.get_all_records()
    df_cadastro = pd.DataFrame(dados_cadastro)
    df_cadastro = normalizar_colunas(df_cadastro)

    # ABA_ATUALIZAR
    plan_atual = cliente.open_by_key(SHEET_ID).worksheet(ABA_ATUALIZAR)
    dados_atual = plan_atual.get_all_records()
    df_atual = pd.DataFrame(dados_atual)
    df_atual = normalizar_colunas(df_atual)

    # detectar coluna de telefone (preferir na aba de atualização, depois cadastro)
    tel_col = detectar_coluna_telefone(list(df_atual.columns)) or detectar_coluna_telefone(list(df_cadastro.columns))
    # padronizar nome interno
    if tel_col:
        # renomear localmente para phone_number se diferente
        if tel_col != "phone_number":
            if tel_col in df_atual.columns:
                df_atual = df_atual.rename(columns={tel_col: "phone_number"})
            if tel_col in df_cadastro.columns:
                df_cadastro = df_cadastro.rename(columns={tel_col: "phone_number"})

    # preencher colunas driver_id / driver_name nas bases se existirem nomes diferentes
    for df in (df_cadastro, df_atual):
        cols = [c for c in df.columns]
        if "driver_id" not in cols:
            # tentar achar algo parecido
            for cand in cols:
                if "driver" in cand and "id" in cand:
                    df.rename(columns={cand: "driver_id"}, inplace=True)
                    break
        if "driver_name" not in cols:
            for cand in cols:
                if "driver" in cand and ("name" in cand or "nome" in cand):
                    df.rename(columns={cand: "driver_name"}, inplace=True)
                    break

    # garantir colunas na forma esperada
    if "driver_id" not in df_cadastro.columns:
        df_cadastro["driver_id"] = pd.NA
    if "driver_name" not in df_cadastro.columns:
        df_cadastro["driver_name"] = pd.NA
    if "driver_id" not in df_atual.columns:
        df_atual["driver_id"] = pd.NA
    if "driver_name" not in df_atual.columns:
        df_atual["driver_name"] = pd.NA

    # limpar duplicados
    df_cadastro = df_cadastro.drop_duplicates(subset=["driver_id", "driver_name"])
    df_atual = df_atual.drop_duplicates(subset=["driver_id", "driver_name"])

    # ---------- RESUMO OFERTA ----------
    # dias ofertados por dia (um dia é contado se qualquer turno ofertado naquele dia)
    dias_ofertados = (
        df_long[df_long["disponivel"] == 1]
        .groupby(["driver_id", "driver_name", "data"])["disponivel"]
        .max()
        .reset_index()
    )

    resumo = df_long.groupby(["driver_id", "driver_name", "vehicle_type", "no_show_time"], dropna=False).agg(
        total_dias=("data", "nunique")
    ).reset_index()

    dias_disponiveis = dias_ofertados.groupby(["driver_id", "driver_name"], dropna=False).agg(
        dias_disponivel=("data", "nunique")
    ).reset_index()

    resumo = resumo.merge(dias_disponiveis, on=["driver_id", "driver_name"], how="left")
    resumo["dias_disponivel"] = resumo["dias_disponivel"].fillna(0).astype(int)
    resumo["dias_sem_ofertar"] = resumo["total_dias"] - resumo["dias_disponivel"]

    # sequência máxima sem ofertar (compatível com várias versões pandas)
    def max_consecutivos(grp):
        grp = grp.sort_values("data")
        faltou = (grp["disponivel"] == 0).astype(int)
        max_seq = seq = 0
        for f in faltou:
            if f == 1:
                seq += 1
                if seq > max_seq:
                    max_seq = seq
            else:
                seq = 0
        return max_seq

    seq_inatividade = df_long.groupby("driver_name").apply(max_consecutivos).reset_index()
    seq_inatividade.columns = ["driver_name", "max_dias_sem_ofertar"]
    resumo = resumo.merge(seq_inatividade, on="driver_name", how="left")

    # adicionar dias_carregado
    resumo = resumo.merge(dias_carregados_df, on=["driver_id", "driver_name"], how="left")
    resumo["dias_carregado"] = resumo["dias_carregado"].fillna(0).astype(int)

    # oferta x carregamento %
    resumo["oferta_x_carregamento_%"] = ((resumo["dias_carregado"] / resumo["dias_disponivel"].replace(0, pd.NA)) * 100).fillna(0).round(1)

    # regra extra: se ofertou <= 1 dia por cada 7 dias no período, marcar Risco de Churn
    # para comparação usamos total_dias (período disponível no relatório)
    resumo["rate_por_7dias"] = resumo.apply(lambda r: (r["dias_disponivel"] / r["total_dias"] * 7) if r["total_dias"]>0 else 0, axis=1)
    # rate_por_7dias é número de dias ofertados por janela de 7 dias; se <=1 então risco
    # Implementamos classificação combinada abaixo.

    # Classificação
    def classificar(row):
        if row["dias_disponivel"] == 0:
            return "Inativo"
        # risco se dias_sem_ofertar > 14 OU se rate_por_7dias <= 1
        if (row.get("dias_sem_ofertar", 0) > 14) or (row.get("rate_por_7dias", 0) <= 1):
            return "Risco de Churn"
        if row["dias_disponivel"] > row["total_dias"] * 0.5:
            return "Engajado"
        return "Intermediário"

    resumo["categoria"] = resumo.apply(classificar, axis=1)

    # anexar telefone e status cadastro (união com df_cadastro / df_atual)
    # criar df_cad_total para procurar phone/status
    df_cad_total = pd.concat([df_cadastro.assign(status_cadastro="Existente"), df_atual.assign(status_cadastro="Atualização")], ignore_index=True, sort=False)
    # normalizar coluna phone_number se existir
    if "phone_number" not in df_cad_total.columns:
        df_cad_total["phone_number"] = pd.NA

    # garantir driver_id na df_cad_total
    if "driver_id" not in df_cad_total.columns:
        df_cad_total["driver_id"] = pd.NA
    if "driver_name" not in df_cad_total.columns:
        df_cad_total["driver_name"] = pd.NA

    # dedup por driver_id preferencialmente
    df_cad_total = df_cad_total.drop_duplicates(subset=["driver_id", "driver_name"])

    resumo = resumo.merge(df_cad_total[["driver_id", "phone_number", "status_cadastro"]], on="driver_id", how="left")

    # preencher nulos
    resumo["phone_number"] = resumo["phone_number"].fillna("N/A")
    resumo["status_cadastro"] = resumo["status_cadastro"].fillna("N/A")

    # preparar conjuntos para filtros (clusters originais únicos)
    clusters_unicos = sorted(df_long["cluster_individual"].dropna().unique().tolist())

    return resumo, df_long, df_cadastro, df_atual, clusters_unicos

# =====================================================
# 5. EXECUÇÃO
# =====================================================
try:
    resumo, df_long, df_cadastro, df_atual, clusters_unicos = carregar_dados()
    st.success("✅ Dados carregados com sucesso (SHEET_OFERTA, SHEET_CARREG, CADASTRO)!")
except FileNotFoundError as e:
    st.error(f"Erro ao localizar {SERVICE_ACCOUNT_FILE}: {e}")
    st.stop()
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    st.stop()

# =====================================================
# 6. FILTROS (aplicados globalmente)
# =====================================================
st.sidebar.header("🔍 Filtros")

# Categoria
categoria_filtro = st.sidebar.multiselect(
    "Categoria:",
    options=sorted(resumo["categoria"].unique()),
    default=sorted(resumo["categoria"].unique())
)

# Cluster (aplica-se às abas que têm cluster)
cluster_selecionado = st.sidebar.selectbox(
    "Cluster:",
    options=["(Todos)"] + clusters_unicos,
    index=0
)

# Turno
turno_filtro = st.sidebar.multiselect(
    "Turno:",
    options=["AM", "PM1", "AM|PM1", "Outro", "Sem Oferta"],
    default=["AM", "PM1", "AM|PM1"]
)

# Veículo
veiculo_filtro = st.sidebar.multiselect(
    "Tipo de Veículo:",
    options=sorted(resumo["vehicle_type"].dropna().unique()),
    default=sorted(resumo["vehicle_type"].dropna().unique())
)

# Ranking filters
st.sidebar.header("🏆 Ranking de Aproveitamento")
top_n = st.sidebar.slider("Quantos motoristas exibir:", min_value=5, max_value=100, value=10, step=5)
min_aprov = st.sidebar.slider("Aproveitamento mínimo (%):", min_value=0, max_value=100, value=0, step=5)

# Aplicar filtro global: filtrar df_long por cluster selecionado para obter lista de drivers no cluster
if cluster_selecionado and cluster_selecionado != "(Todos)":
    mask_cluster = df_long["cluster_individual"] == cluster_selecionado
    drivers_no_cluster = df_long.loc[mask_cluster, "driver_id"].unique().tolist()
else:
    drivers_no_cluster = resumo["driver_id"].unique().tolist()

# Filtrar 'resumo' por drivers_no_cluster e demais filtros
resumo_filtrado = resumo[
    (resumo["driver_id"].isin(drivers_no_cluster))
    & (resumo["categoria"].isin(categoria_filtro))
    & (resumo["vehicle_type"].isin(veiculo_filtro))
    & (resumo["oferta_x_carregamento_%"] >= min_aprov)
].copy()

# Filtrar df_long também para exibições detalhadas
if cluster_selecionado and cluster_selecionado != "(Todos)":
    df_long_filtrado = df_long[df_long["cluster_individual"] == cluster_selecionado].copy()
else:
    df_long_filtrado = df_long.copy()

df_long_filtrado = df_long_filtrado[df_long_filtrado["turno"].isin(turno_filtro)]

# =====================================================
# 7. KPIs
# =====================================================
col1, col2, col3, col4, col5 = st.columns([1,1,1,1,1.2])
col1.metric("Total Motoristas", resumo_filtrado["driver_name"].nunique())
col2.metric("Engajados", (resumo_filtrado["categoria"] == "Engajado").sum())
col3.metric("Risco de Churn", (resumo_filtrado["categoria"] == "Risco de Churn").sum())
col4.metric("Inativos", (resumo_filtrado["categoria"] == "Inativo").sum())
media_aproveitamento_val = round(resumo_filtrado["oferta_x_carregamento_%"].mean() if not resumo_filtrado.empty else 0, 1)
col5.metric("Aproveitamento médio (%)", f"{media_aproveitamento_val}%")

# =====================================================
# 8. GRÁFICOS PRINCIPAIS
# =====================================================
col1, col2 = st.columns(2)
ordem = ["Engajado", "Intermediário", "Risco de Churn", "Inativo"]

with col1:
    fig1 = px.histogram(
        resumo_filtrado,
        x="categoria",
        color="categoria",
        category_orders={"categoria": ordem},
        title="Distribuição por Categoria"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.box(
        resumo_filtrado,
        x="categoria",
        y="dias_sem_ofertar",
        color="categoria",
        category_orders={"categoria": ordem},
        title="Dias sem ofertar por Categoria"
    )
    st.plotly_chart(fig2, use_container_width=True)

# =====================================================
# 9. CORRELAÇÃO OFERTA x CARREGAMENTO
# =====================================================
st.subheader("🔄 Correlação: Dias com Oferta vs Dias com Carregamento")
fig_corr = px.scatter(
    resumo_filtrado,
    x="dias_disponivel",
    y="dias_carregado",
    color="categoria",
    size="oferta_x_carregamento_%",
    hover_data=["driver_name", "phone_number", "dias_disponivel", "dias_carregado", "oferta_x_carregamento_%"],
    title="Correlação entre dias ofertados e dias carregados"
)
st.plotly_chart(fig_corr, use_container_width=True)

# =====================================================
# 10. RANKING
# =====================================================
st.subheader("🏆 Ranking de Motoristas (Oferta × Carregamento)")
ranking = resumo_filtrado.sort_values("oferta_x_carregamento_%", ascending=False).head(top_n)
ranking = ranking.assign(
    label_text=lambda df: "Oferta: " + df["dias_disponivel"].astype(str) +
                           " | Carreg: " + df["dias_carregado"].astype(str) +
                           " | " + df["oferta_x_carregamento_%"].astype(str) + "%"
)
fig_rank = px.bar(
    ranking,
    x="driver_name",
    y="oferta_x_carregamento_%",
    color="categoria",
    text="label_text",
    hover_data=["phone_number", "status_cadastro", "dias_disponivel", "dias_carregado"],
    title=f"Top {top_n} Motoristas com Maior Aproveitamento (≥ {min_aprov}%)"
)
fig_rank.update_traces(texttemplate="%{text}", textposition="outside")
st.plotly_chart(fig_rank, use_container_width=True)

# =====================================================
# 11. EVOLUÇÃO TEMPORAL
# =====================================================
st.subheader("📈 Evolução da Disponibilidade")
df_evolucao = df_long_filtrado.groupby("data")["disponivel"].mean().reset_index()
fig3 = px.line(df_evolucao, x="data", y="disponivel", title="Disponibilidade Média Diária")
st.plotly_chart(fig3, use_container_width=True)

# =====================================================
# 12. TABELA DETALHADA + DOWNLOAD
# =====================================================
st.subheader("📋 Tabela Detalhada")
gb = GridOptionsBuilder.from_dataframe(resumo_filtrado)
gb.configure_pagination(paginationAutoPageSize=True)
gb.configure_side_bar()
gridOptions = gb.build()
AgGrid(resumo_filtrado, gridOptions=gridOptions, enable_enterprise_modules=True)

csv = resumo_filtrado.to_csv(index=False).encode("utf-8")
st.download_button(
    label="📥 Baixar CSV filtrado",
    data=csv,
    file_name="resumo_motoristas_com_carregamentos.csv",
    mime="text/csv",
)

# =====================================================
# 13. MÓDULO DE CONTATO (NOVOS / INATIVOS) -> atualiza BASE_CADASTRO
# =====================================================
st.subheader("📞 Registro de Contato com Motoristas Novos / Inativos")

try:
    cliente = conectar_sheets()
    plan_base = cliente.open_by_key(SHEET_ID).worksheet(ABA_CADASTRO)

    # obter valores brutos para lidar com cabeçalhos sujos / duplicados
    raw = plan_base.get_all_values()
    if len(raw) == 0:
        st.warning("A aba BASE_CADASTRO está vazia. Será criada quando salvar um contato.")
        df_base = pd.DataFrame(columns=["driver_id", "driver_name", "phone_number", "contato"])
    else:
        headers_raw = raw[0]
        # limpeza de headers duplicados / vazios
        def limpar_headers(header_list):
            seen = {}
            cleaned = []
            for idx, h in enumerate(header_list):
                h = (h or "").strip()
                if h == "":
                    h = f"col_{idx}"
                h_low = h.lower().replace(" ", "_")
                if h_low in seen:
                    h_low = f"{h_low}_{idx}"
                seen[h_low] = True
                cleaned.append(h_low)
            return cleaned
        headers = limpar_headers(headers_raw)
        df_base = pd.DataFrame(raw[1:], columns=headers)
        df_base = normalizar_colunas(df_base)

    # garantir colunas esperadas
    for c in ["driver_id", "driver_name", "phone_number", "contato"]:
        if c not in df_base.columns:
            df_base[c] = pd.NA

    # identificar novos motoristas (da aba de atualização)
    # usamos df_atual carregado antes na função carregar_dados (cache) - read novamente aqui para consistência
    plan_atual = cliente.open_by_key(SHEET_ID).worksheet(ABA_ATUALIZAR)
    raw_atual = plan_atual.get_all_values()
    if len(raw_atual) >= 1:
        headers_atual = normalizar_colunas(pd.DataFrame([raw_atual[0]])).columns.tolist()
        df_atual_local = pd.DataFrame(raw_atual[1:], columns=headers_atual)
        df_atual_local = normalizar_colunas(df_atual_local)
    else:
        df_atual_local = pd.DataFrame(columns=["driver_id", "driver_name", "phone_number"])

    # detect phone col name in atualização
    phone_col_atual = detectar_coluna_telefone(list(df_atual_local.columns))
    if phone_col_atual and phone_col_atual != "phone_number":
        df_atual_local = df_atual_local.rename(columns={phone_col_atual: "phone_number"})

    # quais são novos (presentes em atualização e não na base)
    novos = pd.DataFrame()
    if not df_atual_local.empty:
        mask_novos = ~df_atual_local["driver_id"].isin(df_base["driver_id"])
        novos = df_atual_local.loc[mask_novos, ["driver_id", "driver_name", "phone_number"]].drop_duplicates()

    # inativos do resumo
    inativos = resumo[resumo["categoria"] == "Inativo"][["driver_id", "driver_name"]].drop_duplicates()

    # combinar para contato (novos + inativos)
    contatos = pd.concat([novos, inativos], ignore_index=True, sort=False).drop_duplicates(subset=["driver_id"])

    if contatos.empty:
        st.info("Nenhum motorista novo ou inativo pendente de contato.")
    else:
        st.write("Motoristas sugeridos para contato (novos e inativos):")
        AgGrid(contatos)

        driver_selecionado = st.selectbox("Selecione o motorista:", options=contatos["driver_name"].tolist())

        status_contato = st.radio(
            "Status de contato:",
            options=["Contato Efetivado", "Sem Interesse"],
            horizontal=True
        )

        if st.button("💾 Marcar contato na BASE_CADASTRO"):
            # buscar registro na base: por driver_id ou nome
            registro = contatos[contatos["driver_name"] == driver_selecionado].iloc[0].to_dict()
            driver_id_sel = registro.get("driver_id")
            driver_name_sel = registro.get("driver_name")
            phone_sel = registro.get("phone_number", pd.NA)

            # atualizar df_base local
            mask = df_base["driver_id"].astype(str).str.strip() == str(driver_id_sel).strip()
            if mask.any():
                df_base.loc[mask, "contato"] = status_contato
            else:
                # acrescentar novo registro
                novo = {"driver_id": driver_id_sel, "driver_name": driver_name_sel, "phone_number": phone_sel, "contato": status_contato}
                df_base = pd.concat([df_base, pd.DataFrame([novo])], ignore_index=True, sort=False)

            # tentar gravar de volta na planilha (tratamento de erros)
            try:
                # converter para lista de listas (primeira linha = headers)
                valores = [df_base.columns.tolist()] + df_base.fillna("").values.tolist()
                plan_base.update(valores)
                st.success(f"✅ Contato '{status_contato}' registrado para {driver_selecionado} na aba {ABA_CADASTRO}.")
            except Exception as e:
                st.error("Erro ao atualizar BASE_CADASTRO. Possíveis causas:\n"
                         "- credenciais insuficientes (verifique scopes no service account)\n"
                         "- célula ou faixa protegida (remover proteção na planilha)\n"
                         f"Erro recebido: {e}")

except Exception as e:
    st.error(f"Erro no módulo de contatos: {e}")
