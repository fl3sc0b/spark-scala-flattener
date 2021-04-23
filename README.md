# spark-scala-flattener
This scala function for spark allows to flat a whole JSON column into its Dataframe .
## Description
In spark world, if you need to work with a JSON file, you normally use a typical:
``` Scala
spark.read.json("<json file>")
```
However, this will generate a Dataframe that wraps the first level of the JSON hierarchy only. So, for instance, you will end with something like:
| name    | booksIntersted                                                                             |
|---------|--------------------------------------------------------------------------------------------|
| James   | [{"name":"Java", "author":"XX", "pages":120}, {name":"Scala", "author":"XA", "pages":300}] |
| Michael | [{"name":"Java", "author":"XY", "pages":200}, {name":"Scala", "author":"XB", "pages":500}] |
| Robert  | [{"name":"Java", "author":"XZ", "pages":400}, {name":"Scala", "author":"XC", "pages":250}] |
This little function can ease your life, performing a full flattenning for a particular column of your Dataframe.
## Usage
In the previous example, if we do:
``` Scala
flatJSONColumn(df, "booksIntersted")
```
We will get this:
| name    | booksIntersted_name | bookIntersted_author | bookInterested_pages |
|---------|---------------------|----------------------|----------------------|
| James   | Java                | XX                   | 120                  |
| James   | Scala               | XA                   | 300                  |
| Michael | Java                | XY                   | 200                  |
| Michael | Scala               | XB                   | 500                  |
| Robert  | Java                | XZ                   | 400                  |
| Robert  | Scala               | XC                   | 250                  |
## Notes
The function makes use of *explode_outer()* spark function, so should be able to cope even with null/missing values.
