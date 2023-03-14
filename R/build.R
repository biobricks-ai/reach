library(tidyverse,jsonlite)

tmp   <- tempdir()
unzip('import/reachExtractor_V2.zip', exdir=tmp)
bson <- fs::path(tmp,"reachExtractor_V2/reachExtractor") |> fs::dir_ls(regex="bson")

# might need to install mongo for the bsondump command
url <- "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.7.0.deb"
download.file(url,destfile="mongosh.deb")
system("sudo apt install ./mongosh.deb")

walk(bson,\(bs){ 
  js  <- fs::path_ext_set(bs,"json")
  cmd <- sprintf('bsondump %s --outFile=%s',bs,js)
  system(cmd)
})
bsfile = \(name){fs::path(tmp,"reachExtractor_V2/reachExtractor/",name)}
haz <- readLines(bsfile("hazards.json")) |> lapply(jsonlite::fromJSON)

tb <- tibble(js=haz) |> unnest_wider(js) |> 
  select(
    Hazard,description,`data lacking`,
      `conclusive but not sufficient for classification`,chemicals
  ) |>
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
chem <- readLines(bsfile("ChemicalData.json")) |> map(jm)

chemtb <- tibble(js=chem) |> unnest_wider(js) |> 
  select(ecnumber=ECNumber, smiles=SMILES) |>
  filter(!is.na(ecnumber)) |> unique()

tb <- chemtb |> inner_join(haz,by="ecnumber")

# enrichedReach
out <- fs::dir_create("brick") |> fs::path("reach.parquet")
arrow::write_parquet(tb,out)

# cleanup
fs::dir_delete(tmp)
fs::file_delete("mongosh.deb")