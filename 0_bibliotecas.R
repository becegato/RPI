# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #

if (!require("pacman")) install.packages("pacman")

pacman::p_load(
  data.table, 
  tidyverse, 
  zoo, 
  future, 
  lubridate, 
  writexl,
  pbapply,
  arrow,
  collapse,
  rlang
)
