# ----------------------------- #
# --- DESPESA POR TRIMESTRE --- #
# ----------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_bibliotecas.R")
source("R/0_funcoes.R")

memory.limit(size = 10^12)
future::plan("multisession")

# demonstrações financeiras -----------------------------------------------

 cadop <- readxl::read_xlsx("data/cadop.xlsx")

contas <- data.table::fread("data/contas_dpi21.csv") |> 
  purrr::flatten_chr()

urls <- list(
  purrr::map(
    paste0(1:4, "T"),
    ~ urls_dem_cont(
      anos = 2015:2020,
      tri = .x
    )
  ),
  purrr::map(
    paste0(1:3, "T"),
    ~ urls_dem_cont(
      anos = 2021,
      tri = .x
    )
  )
) |> 
  purrr::flatten() |>
  purrr::flatten_chr()
  
diops <- pbapply::pblapply(
  urls,
  trat_diops
) |>
  data.table::rbindlist(use.names = TRUE)

diops <- diops[
  ,
  data := zoo::as.yearqtr(data)
][
  lubridate::year(data) > 2014
]

desp21 <- diops[
  cd_conta_contabil %in% contas,
  .(desp_ind = collapse::fsum(vl_saldo_final)),
  by = "data"
][
  ,
  data := as.character(data)
]

writexl::write_xlsx(desp21, "outputs/desp21.xlsx")
