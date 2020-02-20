# make names db safe: no '.' or other illegal characters,
# all lower case and unique
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

# formatter for monetary values
moneyold <- function (x) {
  
  r <- round(x/1000000, digits = 3)
  lab <- paste("£", format(r, big.mark = ",", trim = TRUE, nsmall = 1), "m", sep = "")

}


# http://stackoverflow.com/questions/28159936/formatting-large-currency-or-dollar-values-to-millions-billions
money <-  function(x) {
  z <- abs(x)
  
  if(is.na(z)) {return(0)}
  
  b.index <-  z >= 1e9
  m.index <-  z >= 1e5 & z < 1e9
  
  if(x > 0) {
      output <-  paste("£", formatC(z, format = "d", big.mark = ","), sep = "")
      output[b.index] <-  paste("£", formatC(z[b.index] / 1e9, digits = 1, format = "f"), "bn", sep = "")
      output[m.index] <-  paste("£", formatC(x[m.index] / 1e6, digits = 1, format = "f"), "m", sep = "")
      return(output)
    } else {
      output <-  paste("-£", formatC(z, format = "d", big.mark = ","), sep = "")
      output[b.index] <-  paste("-£", formatC(z[b.index] / 1e9, digits = 1, format = "f"), "bn", sep = "")
      output[m.index] <-  paste("-£", formatC(z[m.index] / 1e6, digits = 1, format = "f"), "m", sep = "")
      return(output)
    }
}

wraptext <- function(x, wrapat = 20) {
  
  z <- strwrap(x, wrapat)
  z <- paste(z, collapse = "\n")
  
}