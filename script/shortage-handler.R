library(readr)
library(magrittr)
library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)


#Sys.setlocale("LC_TIME", "fr_FR.UTF-8")

# Chargement des données
raw_prix <- purrr::map_dfr(list.files("script/fuel_web_app/data/fuel-prices-csv/", full.names = T), function(x) {
  
  data_to_return <- read_delim(x,
                               delim = ";",
                               col_types = cols(.default="c"))
  
  data_to_return
  
})
  

raw_ruptures <- read_delim("data/fuel-shortage.csv",
                           delim = ";") %>%
  mutate(id_pdv = as.character(id_pdv))

instant_ruptures <- read_delim("data/fuel-shortage-instant.csv",
                               delim = ";") %>%
  mutate(id_pdv = as.character(id_pdv))


# Binding des ruptures

ruptures_instant <- instant_ruptures %>%
  mutate(filtre = paste0(id_pdv, "-", debut))

ruptures_histo <- raw_ruptures %>%
  mutate(filtre = paste0(id_pdv, "-", debut)) %>%
  filter(!filtre%in%ruptures_instant$filtre)


rutptures_binded <- rbind(ruptures_histo, ruptures_instant) %>%
  select(-filtre)

# Tri des données

## Compteur échantillon de stations
number_of_stations_ref <- raw_prix %>%
  mutate(date=as.Date(maj)) %>%
  filter(nom_carbu %in% c("E10", "Gazole")) %>% # Filtre des stations distribuant du Gazole ou du SP95
  filter(date>=ymd("2026-02-01")) %>% # filtre des mises à jour >= 2026-02-01
  select(id_pdv) %>%
  distinct()


## Mise en forme données ruptures
tidy_ruptures <- rutptures_binded %>%
  filter(debut >= ymd("2026-01-01")) %>%
  mutate(type_of_fuel = case_when(
    nom_carbu == "Gazole" ~ "Gazole",
    nom_carbu %in% c("E10") ~ "SP95-E10", # On ne garde que l'E10 pour le SP-95
    nom_carbu %in% c("SP98", "E85", "GPLc", "SP95") ~ "Autre"
  )) %>%
  filter(type_of_fuel %in% c("Gazole", "SP95-E10")) %>%
  mutate(date_debut=as.Date(debut),
         date_fin=as.Date(fin)) %>%
  filter((as.numeric(if_else(is.na(date_fin), today(), date_fin)-date_debut)<=30)) %>% # Les carburants en rupture depuis plus de 30 jours sont écartés
  select(id_pdv, date_debut, date_fin, pop, type_of_fuel) %>%
  distinct()


## Data.frame - une ligne par jour et par carburant en rupture pour chaque station
daily_ruptures <- purrr::map_dfr(seq(1, nrow(tidy_ruptures), by = 1), function(x){
  
  
  data_sample <- tidy_ruptures %>%
    filter(row_number() == x)
  
  date_debut <- data_sample$date_debut
  
  date_fin <- if_else(is.na(data_sample$date_fin), today(), data_sample$date_fin)
  
  data_to_return <- data.frame(date = seq(date_debut, date_fin, by = "1 day")) %>%
    mutate(
      id_pdv = data_sample$id_pdv,
      pop = data_sample$pop,
      type_of_fuel = data_sample$type_of_fuel
    )
  
  
})


## Décompte journalier
decompte_par_jour <- daily_ruptures %>%
  distinct() %>%
  mutate(count_carbu = 1) %>%
  group_by(date, id_pdv) %>%
  mutate(indicateur = case_when(
    sum(count_carbu) == 2 ~ "Les deux carburants",
    T ~ type_of_fuel
  )) %>%
  ungroup() %>%
  select(id_pdv, date, indicateur) %>%
  distinct() %>%
  group_by(date, indicateur) %>%
  summarise(nb=n()) %>%
  ungroup() %>%
  mutate(tx=round(nb/nrow(number_of_stations_ref)*100, 1))


## Sorties CSV

data_tx_to_flourish <- decompte_par_jour %>%
  select(date, indicateur, tx) %>%
  spread(key="indicateur", value="tx") %>%
  select(date, Gazole, `SP95-E10`, `Les deux carburants`) %>%
  mutate(TOTAL = Gazole + `SP95-E10` + `Les deux carburants`) %>%
  filter(date>=ymd("2026-02-23")) %>%
  mutate(date_axis=if_else(day(date)==1,
                           month(date, label=T, abbr = F),
                           as.character(date))) %>%
  mutate(date_for_popup = if_else(day(date)==1,
                                  paste0(
                                    day(date),
                                    "er ",
                                    month(date, label = T, abbr = F),
                                    " ",
                                    year(date)
                                  ),
                                  paste0(
                                    day(date),
                                    " ",
                                    month(date, label = T, abbr = F),
                                    " ",
                                    year(date)
                                  )
  )) %>%
  mutate(nb_station = nrow(number_of_stations_ref))

write_csv(data_tx_to_flourish, "data/fuel-shortage-to-flourish.csv")






