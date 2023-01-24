# --------------- #
# --- FUNÇÕES --- #
# --------------- #

# leitura e descompactação ------------------------------------------------

unpack_read <- function(url, type, cols = NULL) {
  tempdir <- tempdir()
  
  temp <- tempfile(tmpdir = tempdir, fileext = ".zip")
  
  if (stringr::str_detect(url, "3-Trimestre")) {
    string_subset <- "3T2017"
  } else {
    string_subset <- stringr::str_sub(url, start = 73, end = 79)
  }
  
  regex_expression <- glue::glue("{string_subset}.csv")
  
  h <- curl::new_handle()
  
  curl::handle_setopt(
    h,
    ssl_verifyhost = 0,
    ssl_verifypeer = 0
  )
  
  curl::curl_download(
    url = url,
    destfile = temp,
    quiet = TRUE,
    handle = h
  )
  
  zip::unzip(
    zipfile = temp,
    exdir = tempdir
  )
  
  csv_path <- fs::dir_ls(
    tempdir,
    regexp = regex_expression
  )
  
  df <- data.table::fread(
    file = csv_path,
    sep = ";",
    dec = ",",
    blank.lines.skip = TRUE,
    encoding = "UTF-8",
    drop = "DESCRICAO",
  ) |>
    janitor::clean_names() |>
    collapse::na_omit()
  
  first_date_df <- df$data[1]
  
  if (stringr::str_detect(first_date_df, "/")) {
    test_data_format <- stringr::str_sub(first_date_df, start = 1, end = 4)
    
    if (stringr::str_detect(test_data_format, "/")) {
      df[, data := zoo::as.yearqtr(data, "%d/%m/%Y")]
    } else {
      df[, data := zoo::as.yearqtr(data, "%Y/%m/%d")]
    }
  } else {
    df[, data := zoo::as.yearqtr(data)]
  }
  
  if ("vl_saldo_inicial" %in% names(df)) df <- df[, !"vl_saldo_inicial"]
  
  df <- df[
    ,
    cd_cc := as.character(cd_conta_contabil)
  ][
    ,
    .(data, registro = reg_ans, cd_cc, vl_saldo_final)
  ]
  
  gc()
  
  purrr::walk(
    list(temp, csv_path),
    fs::file_delete
  )
  
  return(df)
}

# urls de demonstrações contábeis -----------------------------------------

urls_dem_cont <- function(anos, tri) {
  z <- tibble::tibble(
    a = "https://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/",
    b = anos
  ) |>
    dplyr::group_by(a, b) |>
    tidyr::nest() |>
    dplyr::mutate(
      data = purrr::map2(
        data,
        b,
        ~ tibble::tibble(
          c = glue::glue("/{tri}"),
          d = glue::glue("{.y}.zip")
        )
      )
    ) |>
    tidyr::unnest(cols = data) |>
    tidyr::unite(
      "url",
      c(a, b, c, d),
      sep = ""
    ) |>
    purrr::flatten_chr()

  return(z)
}

# função para gerar resumo de estatísticas descritivas --------------------

statsVDA <- function(df) {
  benefs <- names(df) |> 
    stringr::str_subset("benef") |> 
    rlang::syms()
  
  anos <- names(df) |> 
    stringr::str_subset("despesa") |> 
    stringr::str_sub(start = -4, end = -1)
  
  n_benefs <- glue::glue("n_benefs_{anos}") |> 
    rlang::syms()
  
  media <- glue::glue("media_{anos}") |> 
    rlang::syms()
  
  df <- df |>
    dplyr::summarise(
      n_ops = dplyr::n_distinct(registro),
      !!n_benefs[[1]] := sum(!!benefs[[1]]),
      !!n_benefs[[2]] := sum(!!benefs[[2]]),
      !!media[[1]] := weighted.mean(vda_parcial, w = !!benefs[[1]]),
      !!media[[2]] := weighted.mean(vda_parcial, w = !!benefs[[2]]),
      median = median(vda_parcial),
      q1 = quantile(vda_parcial, p = .25),
      q3 = quantile(vda_parcial, p = .75),
      aiq = (q3 - q1) * 1.5,
      lim_inf = q1 - aiq,
      lim_sup = q3 + aiq,
      min = min(vda_parcial),
      max = max(vda_parcial)
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        round,
        4
      )
    ) |>
    tidyr::pivot_longer(
      dplyr::everything(),
      names_to = "stats",
      values_to = "values"
    )

  return(df)
}

