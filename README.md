# README #

To make proper jar files, we "need" SBT 
```
#!bash

brew install sbt
```
SBT enforces a strict directory structure:

```
#!noname

src/
  main/
    resources/
       <files to include in main jar here (e.g english.txt>
    scala/
       <main Scala sources>
lib/
  <external libraries which cannot be automatically downloaded (tinyIR in the future?)>
```

In order to create the .jar file we run `sbt assembly` from the project root path. The .jar file will end up in *target/scala-{version}/filname.jar*. 

After this we can run it with `scala target/target/scala-{version}/filname.jar webpage-to-crawl`.