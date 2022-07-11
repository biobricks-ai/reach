library(tidyverse)

tmp   <- tempdir()
bson  <- fs::path("reachExtractor_V2/reachExtractor/hazards.bson")
bson2 <- fs::path("reachExtractor_V2/reachExtractor/ChemicalData.bson")
unzip('import/reachExtractor_V2.zip', files=c(bson,bson2), exdir=tmp)

cmd <- sprintf('bsondump %s --outFile=%s',fs::path(tmp,bson),fs::path(tmp,"haz.json"))
system(cmd)

cmd2 <- sprintf('bsondump %s --outFile=%s',fs::path(tmp,bson2),fs::path(tmp,"chem.json"))
system(cmd2)


haz <- readLines(fs::path(tmp,"haz.json")) |> lapply(jsonlite::fromJSON)

tb <- tibble(js=haz) |> unnest_wider(js) |> 
  select(Hazard,description,`data lacking`,`conclusive but not sufficient for classification`,chemicals) |>
  mutate(description = trimws(description)) |>
  select(hazard=Hazard,description,
    negative=`conclusive but not sufficient for classification`,
    positive=chemicals)

neg <- tb |> select(negative,hazard,description) |> unnest_longer(negative) |>
  mutate(value="negative") |> 
  select(ecnumber=negative,hazard,value)

pos <- tb |> select(positive,hazard,description) |> unnest_longer(positive) |>
  mutate(value="positive") |> 
  select(ecnumber=positive,hazard,value)

haz <- dplyr::bind_rows(neg,pos)

# Chemical data
jm <- possibly(jsonlite::fromJSON,otherwise=list())
chem <- readLines(fs::path(tmp,"chem.json")) |> map(jm)

chemtb <- tibble(js=chem) |>unnest_wider(js) |> select(ecnumber=ECNumber,name) |>
  filter(!is.na(ecnumber)) |> unique()

tb <- chemtb |> inner_join(haz,by="ecnumber")

out <- fs::dir_create("data")
arrow::write_parquet(tb,fs::path(out,"reach.parquet"))

