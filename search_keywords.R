

library(yaml)
library(rjson)
library(plyr)


# This function takes a text, cleans it, and returns a split text by words
cleanText<-function(text){
  text<-unlist(strsplit(text, ""))
  text<-tolower(text)
  text<-text[which(text %in% c(letters, " "))]
  text<-paste(text, collapse="")
  text<-unlist(strsplit(text, " "))
  return(text)
}


SearchKeywords <- function(chicago.path, word.yaml.path, out.path) {

  words <- yaml.load_file(word.yaml.path)

  paths <- list.files(chicago.path, pattern='*.bz2', full.names=T, recursive=T)

  result <- lapply(paths, function(path) {

    novel <- fromJSON(file=path)

    text <- cleanText(novel$plain_text)

    row <- c(
      corpus=novel$corpus,
      identifier=novel$identifier,
      title=novel$title,
      year=novel$year,
      authorFirst=novel$author_first,
      authorLast=novel$author_last
    )

    for (group in names(words)) {

      group.words <- words[[group]]

      group.words.counts <- lapply(group.words, function(word) length(which(text == word)))
      names(group.words.counts) <- group.words
      total.count <- Reduce("+", group.words.counts)

      row[group] <- total.count

    }

    print(path)

    return(row)

  })

  write.csv(ldply(result), file=out.path)

}


SearchKeywords('/scratch/PI/malgeehe/data/stacks/ext/chicago', 'words.yaml', 'groups.csv')
