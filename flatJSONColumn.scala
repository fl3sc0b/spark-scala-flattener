def flatJSONColumn( orgDF: DataFrame, columnName: String ) : DataFrame = {
  /** Flats recursively a JSON Object inside a Column on a DataFrame
  *
  * orgDF: source DataFrame
  * columnName: Name of the column to be flattened
  *
  **  returns: A DataFrame result of the operation
  *
  **/
  
  var ret = orgDF // Starts with the original DataFrame
  orgDF.schema(columnName).dataType match {
    case ArrayType(_,_) /* Array detected => To explode */ => ret = orgDF.withColumn("exploded_column", explode_outer(orgDF(columnName)))
                           ret = ret.drop(s"$columnName").withColumnRenamed("exploded_column", s"$columnName") // Rename exploded column
                           ret = flatJSONColumn(ret, columnName) // Continue (vertically) 
    case StructType(_) => /* Struct detected => To flat */ val colsNames = orgDF.select(s"$columnName".concat(".*")).columns
                             for (i <- colsNames) { // Iterate through all elements at the same level 
                               val new_column = s"$columnName".concat("_").concat(i)
                               ret = ret.withColumn(new_column, orgDF.col(s"$columnName.$i")) // Add the new element as a Column
                               ret = flatJSONColumn(ret, new_column) // Continue (horizontally)
                             } 
                             ret = ret.drop(s"$columnName") // Remove original Column
    case _ => /* Otherwise no action is required */
  }

  ret

}