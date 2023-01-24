# -------------------- #
# --- PROJEÇÃO VDA --- #
# -------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_bibliotecas.R")
source("R/0_funcoes.R")

future::plan("multisession")

# função ------------------------------------------------------------------

vda <- function(anos, trimestre, fonte, mes_analise) {
  cat(glue::glue("\n\n ========== VDA - {trimestre} {anos[2]} ==========\n\n"))
  
  # download de dados financeiros -------------------------------------------
  
  if (fonte == "ftp") {
    urls <- urls_dem_cont(
      anos = anos,
      tri = trimestre
    )
    
    diops <- pbapply::pblapply(
      urls,
      unpack_read 
    ) |>
      data.table::rbindlist(use.names = TRUE)
  } else {
    diops <- glue::glue("G:/Economia/1_Base de Informações/3_Demonstrações financeiras_DIOPE/0. DADOS BRUTOS AQUI/{anos}/{trimestre}{anos}.csv") |>
      vroom::vroom(delim = ";", locale = locale(encoding = "latin1", decimal_mark = ",", grouping_mark = ".")) |>
      janitor::clean_names() |>
      data.table::as.data.table()
  }
  
  # tratamento de dados financeiros ----------------------------------------
  
  diops <- diops[
    nchar(cd_cc) == 8 & vl_saldo_final != 0
  ][
    stringr::str_sub(cd_cc, start = 1, end = 6) == "411111"
  ][
    stringr::str_sub(cd_cc, start = 8) == "2"
  ]
  
  data.table::setnames(
    diops,
    old = "data",
    new = "id_calendar"
  )
  
  diops <- diops |>
    tibble::as_tibble() |>
    dplyr::group_by(registro, id_calendar) |>
    dplyr::summarise(despesa = sum(vl_saldo_final), .groups = "drop")
  
  despesa <- glue::glue("despesa_{anos}") |> 
    rlang::syms()
  
  anos_sym <- glue::glue("{anos}") |> 
    rlang::syms()
  
  base_despesa <- diops |>
    dplyr::mutate(ano = lubridate::year(id_calendar)) |>
    dplyr::select(registro, despesa, ano) |>
    tidyr::pivot_wider(
      names_from = ano,
      values_from = despesa,
      values_fill = 0
    ) |> 
    dplyr::rename(
      !!despesa[[1]] := !!anos_sym[[1]],
      !!despesa[[2]] := !!anos_sym[[2]]
    ) |>
    dplyr::filter(!!despesa[[1]] > 0, !!despesa[[1]] > 0)
  
  # base de beneficiários --------------------------------------------------
  
  url_da <- "C:/Users/luisa.becegato/Associação Brasileira de Planos de Saúde/Groups - Documentos/Economia/rpi/Beneficiarios_operadora_e_carteira.csv"
  
  beneficiarios <- vroom::vroom(url_da, delim = ";", locale = locale(encoding = "utf8")) |>
    janitor::clean_names() |> 
    dplyr::mutate(
      id_calendar = glue::glue("{stringr::str_sub(mes, 1, 4)}-{stringr::str_sub(mes, 5, 6)}-01") |> 
        lubridate::as_date(),
      cd_operadora = as.integer(cd_operadora)
    ) |>
    dplyr::filter(lubridate::year(id_calendar) %in% anos) |> 
    dplyr::rename(
      modalidade = gr_modalidade,
      contratacao = gr_contratacao,
      vigencia = vigencia_plano,
      financiamento = tipo_financiamento
    ) |> 
    dplyr::filter(
      modalidade %in% c("Cooperativa Médica", "Filantropia", "Medicina de Grupo", "Seguradora"),
      vigencia == "P",
      financiamento != "Pós-estabelecido",
      contratacao == "Individual ou familiar",
      cobertura == "Médico-hospitalar"
    ) |>
    dplyr::group_by(cd_operadora, id_calendar) |>
    dplyr::summarize(benef = sum(nr_benef, na.rm = TRUE)) |>
    dplyr::group_by(cd_operadora, ano = lubridate::year(id_calendar)) |>
    dplyr::mutate(
      benef = dplyr::cummean(benef),
      n_meses = length(benef),
      mes = lubridate::month(id_calendar)
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(mes == mes_analise) |>
    dplyr::select(ano, mes, cd_operadora, n_meses, benef)
  
  benefs <- glue::glue("benefs_{anos}") |> 
    rlang::syms()
  
  beneficiarios <- beneficiarios |>
    dplyr::select(cd_operadora, ano, benef) |>
    tidyr::pivot_wider(names_from = ano, values_from = benef, values_fill = 0) |>
    dplyr::select(
      registro = cd_operadora, 
      !!benefs[[1]] := !!anos_sym[[1]], 
      !!benefs[[2]] := !!anos_sym[[2]]
    )
  
  # vda --------------------------------------------------------------------
  
  dpb <- glue::glue("dpb_{anos}") |> 
    rlang::syms()
  
  cadop <- arrow::read_parquet("data/2_cadop.parquet") |> 
    dplyr::mutate(registro = as.numeric(registro))
  
  vda <- base_despesa |>
    dplyr::mutate(registro = as.double(registro)) |>
    dplyr::left_join(beneficiarios, by = "registro") |>
    dplyr::filter(!!benefs[[1]] > 0 & !!benefs[[2]] > 0) |>
    dplyr::mutate(
      !!dpb[[1]] := !!despesa[[1]] / !!benefs[[1]],
      !!dpb[[2]] := !!despesa[[2]] / !!benefs[[2]],
      pond := !!benefs[[1]] / sum(!!benefs[[1]], na.rm = TRUE),
      vda_parcial := (!!dpb[[2]] / !!dpb[[1]]) - 1,
      vda_ponderada = vda_parcial * pond
    ) |>
    dplyr::left_join(cadop, by = "registro") |> 
    dplyr::select(registro, razao_social, modalidade, dplyr::everything())
  
  stats <- statsVDA(vda)
  
  lim_inf <- stats |>
    filter(stats == "lim_inf") |>
    dplyr::pull(values)
  
  lim_sup <- stats |>
    filter(stats == "lim_sup") |>
    dplyr::pull(values)
  
  vda_sem_outliers <- vda |>
    dplyr::filter(vda_parcial > lim_inf, vda_parcial < lim_sup) |>
    dplyr::mutate(
      pond := !!benefs[[1]] / sum(!!benefs[[1]], na.rm = TRUE),
      vda_ponderada = vda_parcial * pond
    )
  
  purrr::walk2(
    list(vda, vda_sem_outliers, stats),
    c("VDA", "VDA_sem_out", "boxplot"),
    ~ writexl::write_xlsx(.x, glue::glue("outputs/{.y}_{trimestre}{anos[2]}_{fonte}.xlsx"))
  )
}

# download ----------------------------------------------------------------

periodo <- 2021:2022

purrr::walk2(
  .x = periodo - 1,
  .y = periodo,
  .f = function(i, j) {
    vda(
      anos = seq(i, j),
      trimestre = "3T",
      fonte = "ftp", # ou "local"
      mes_analise = 09
    )
  }
)
