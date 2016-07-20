#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

home <- Sys.getenv("SPARK_HOME")
.libPaths(c(file.path(home, "R", "lib"), .libPaths()))
Sys.setenv(NOAWT=1)

# Make sure SparkR package is the last loaded one
old <- getOption("defaultPackages")
options(defaultPackages = c(old, "SparkR"))

library(utils)
library(datasets)
library(stats)
library(SparkR)


#import hidden functions from SparkR
invokeJava <- getFromNamespace("invokeJava", "SparkR")
.sparkREnv <- getFromNamespace(".sparkREnv","SparkR")
connectBackend <- getFromNamespace("connectBackend","SparkR")
map <- getMethod("map",signature(X = "DataFrame", FUN = "function"))
RDD <- getFromNamespace("RDD","SparkR");
getJRDD <- getFromNamespace("getJRDD","SparkR");
groupByKey <- getFromNamespace("groupByKey","SparkR")
reduceByKey <- getFromNamespace("reduceByKey","SparkR")
mapPartitionsWithIndex <- getFromNamespace("mapPartitionsWithIndex","SparkR")
numPartitions <- getFromNamespace("numPartitions","SparkR")
hashCode <- getFromNamespace("hashCode","SparkR")
filterRDD <- getFromNamespace("filterRDD","SparkR")
toRDD <- getFromNamespace("toRDD","SparkR")
callJStatic <- getFromNamespace("callJStatic","SparkR");
callJMethod <- getFromNamespace("callJMethod","SparkR");
broadcast <- getFromNamespace("broadcast","SparkR")
parallelize <-  getFromNamespace("parallelize","SparkR")
value <-  getFromNamespace("value","SparkR")


backendPort <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT")

#
tryCatch({
  connectBackend("localhost", backendPort)
}, error = function(err) {
  cat(paste("Failed to connect JVM\n", err))
})

#notify interpreter
sparkRInterpreterObjId <- Sys.getenv("SPARKR_INTERPRETER_ID")
invokeJava(isStatic = FALSE, sparkRInterpreterObjId, "onRScriptInitialized")
assign(".scStartTime", as.integer(Sys.time()), envir = .sparkREnv)

#define uberdata context methods
uc.sparkContext <- function() {

  if (exists(".sparkRjsc", envir = .sparkREnv)) {
    cat("Re-using existing Uberdata Context. \n")
    return(get(".sparkRjsc", envir = .sparkREnv))
  }

  sparkRJsc <- callJStatic("org.apache.spark.api.r.ZeppelinRBackend",
                            "createSparkContext")
  assign(".sparkRjsc", sparkRJsc, envir = .sparkREnv)
  return(sparkRJsc)

}

uc.sqlContext <- function() {
  if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
    cat("Re-using existing Sql Context. \n")
    return(get(".sparkRSQLsc", envir = .sparkREnv))
  }


  sparkRJsc <- callJStatic("org.apache.spark.api.r.ZeppelinRBackend", "createSqlContext",uc.sparkContext())

  assign(".sparkRSQLsc", sparkRJsc, envir = .sparkREnv)
  assign(".sparkRHivesc", sparkRJsc, envir = .sparkREnv)
  return(sparkRJsc)
}

uc.stop <- function(){
  sparkR.stop()
}

# accept commands from the notebook
while(TRUE){
  requestStatements <- invokeJava(isStatic = FALSE, sparkRInterpreterObjId, "getStatements");
  statementStr <- invokeJava(isStatic = FALSE, requestStatements$id, "statements");
  tryCatch({
    out <- capture.output(eval(parse(text=statementStr)));
    invokeJava(isStatic = FALSE, sparkRInterpreterObjId, "setStatementsFinished", paste(out, collapse="\n"), FALSE)
  }, error = function(err) {
    invokeJava(isStatic = FALSE, sparkRInterpreterObjId, "setStatementsFinished", paste(err, collapse="\n"), TRUE)
  })
}


