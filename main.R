library(Rcpp)
library(olivr)
library(data.table)

Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
Rcpp::sourceCpp("automata.cpp")

output <- list()

spider_gen <- function(x){
  return(grepl(x = x, pattern = "(bot|zao|borg|DBot|oegp|silk|Xenu|zeal|^NING|CCBot|crawl|htdig|lycos|slurp|teoma|voila|yahoo|Sogou|CiBra|Nutch|^Java/|^JNLP/|Daumoa|Genieo|ichiro|larbin|pompos|Scrapy|snappy|speedy|spider|msnbot|msrbot|vortex|^vortex|crawler|favicon|indexer|Riddler|scooter|scraper|scrubby|WhatWeb|WinHTTP|bingbot|openbot|gigabot|furlbot|polybot|seekbot|^voyager|archiver|Icarus6j|mogimogi|Netvibes|blitzbot|altavista|charlotte|findlinks|Retreiver|TLSProber|WordPress|SeznamBot|ProoXiBot|wsr\\-agent|Squrl Java|EtaoSpider|PaperLiBot|SputnikBot|A6\\-Indexer|netresearch|searchsight|baiduspider|YisouSpider|ICC\\-Crawler|http%20client|Python-urllib|dataparksearch|converacrawler|Screaming Frog|AppEngine-Google|YahooCacheSystem|fast\\-webcrawler|Sogou Pic Spider|semanticdiscovery|Innovazion Crawler|facebookexternalhit|web/snippet|Google-HTTP-Java-Client|BlogBridge|IlTrovatore-Setaccio|InternetArchive|GomezAgent|WebThumbnail|heritrix|NewsGator|PagePeeker|Reaper|ZooShot|holmes)"))
}

# Retrieve data and generate output
output_gen <- function(table){
  data <- olivr::mysql_read(paste("SELECT userAgent FROM",table,"WHERE timestamp >= '20151203000000'"),"log")$userAgent
  automata_bools <- (is_automata(data) | spider_gen(data))
  return(data.frame(
    table = table,
    events = length(automata_bools),
    automata = sum(automata_bools),
    percentage = sum(automata_bools)/length(automata_bools),
    stringsAsFactors = FALSE
  ))
}

# Loop through grabbing and processing data
tables <- c("WikipediaPortal_14377354", "Search_14361785", "MobileWebSearch_12054448","GeoFeatures_12914994")
results <- do.call("rbind", lapply(tables, output_gen))
write.table(results, "eventlogging_automata_data.tsv", row.names = FALSE, sep = "\t")
