Large scale distributed systems - lab 3
u186412 - u186415 - u186615

  #EX. 3:
how to run:
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties --class edu.upf.MastodonStateless
target/lab3-mastodon-1.0-SNAPSHOT.jar src/main/resources/map.tsv

example output:
-------------------------------------------
Time: 1710533670000 ms
-------------------------------------------
(English,189)
(Japanese,21)
(German,8)
(Portuguese,7)
(Russian,3)
(French,2)
(Dutch; Flemish,2)
(Swedish,2)
(Turkish,1)
(Czech,1)
...


  #EX.4:
how to run:
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties --class edu.upf.MastodonWindows ta
rget/lab3-mastodon-1.0-SNAPSHOT.jar src/main/resources/map.tsv

example output:
-------------------------------------------
Time: 1710534100000 ms
-------------------------------------------
(185,English)
(21,German)
(9,Spanish; Castilian)
...

-------------------------------------------
Time: 1710534100000 ms
-------------------------------------------
(185,English)
(21,German)
(9,Spanish; Castilian)
...


  #EX.5:
how to run:
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties --class edu.upf.MastodonWithState target/lab3-masto
don-1.0-SNAPSHOT.jar [language]

example output: ('en' language)
-------------------------------------------
Time: 1710519410000 ms
-------------------------------------------
(11,FanSided)
(11,Hindustan Times :press:)
(9,Factline)
(6,now playing on RFF)
(5,Chris Adamski 🤖)
(5,The Ebbtide)
(5,The Times Of India :press:)
(5,Italy 24 Press)
(4,:rss: NME)
(4,WSJ news feed)
(4,Europe Says)
(4,The Recipe Exchange)
(4,Playing Games)
(4,nova)
(4,Vanuatu GIS Feed)
(4,Club de TéléMatique :verified:)
(4,The Hindu :press:)
(4,Derek)
(3,Biking Japan)
(3,Farming Simulator Mods)
...


  #EX.6:
We weren't able to get exercise 6 working before the deadline. :(
