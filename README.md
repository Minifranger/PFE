# PFE

Guide d'utilisation

1) Télécharger RabbitMQ https://www.rabbitmq.com/download.html et lancer le serveur avec la commande : invoke-rc.d rabbitmq-server start 

2) Télécharger Flink 1.2.0 https://flink.apache.org/downloads.html (Nous utilisons la version Scala 2.11 / Hadoop2.7.0)

3) Après avoir récupéré le projet depuis le Github l'importer en tant que projet Maven dans Eclipse.
Remarque: penser à modifier les variables "chemin" et "cheminMeta" dans les classes RMQtestKmeans et Metadata.

4) Avant de lancer le projet :
  => Dans le dossier du projet : mvn clean install -Pbuild-jar pour construire les .jar dans le dossier target
  => Depuis le dossier flink1.2.0 : ./bin/start-local.sh pour lancer le serveur Flink
  => Depuis le dossier flink1.2.0 : ./bin/flink run -c ensai.RMQtestKmeans /<chemin vers le projet>/target/debs-1.0-SNAPSHOT.jar
  
  La sortie est le fichier .out qui se trouve dans le dossier log de flink
  Pour le lire "en direct" utiliser la commande tail -f <nom du fichier>

5) Pour simuler l'envoi des données dans une file RabbitMQ lancer la classe send depuis Eclipse. Le fichier à envoyer se situe dans le dossier ressources.

6) La classe Rec permet de recoir, depuis Eclipse les sorties de flink vers RabbitMQ

7) Après l'execution du programme, ne pas oublier de stoper flink depuis le dossier flink1.2.0 avec la commande : ./bin/stop-local.sh


