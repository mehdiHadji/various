# Introduction 1: Solution proposée
La solution que je propose consiste à introduire un mécanisme de reprocessing ciblé, basé sur les partitions impactées.

L’idée est simple : au lieu de recalculer toute la BV, on identifie uniquement les dates de valorisation impactées par des corrections, et on rejoue uniquement ces partitions.

On complète cela avec une table de tracking pour garantir qu’on ne traite pas plusieurs fois les mêmes corrections, et assurer l’idempotence du processus.

# Introduction 2: Principe de la solution
La solution qu’on propose repose sur un principe simple :

👉 ne recalculer que ce qui est nécessaire

Concrètement, au lieu de recalculer toute la BV, on va identifier uniquement les partitions impactées par des corrections, et rejouer uniquement ces partitions.


# Etapes
## Étape 1 – Identification des corrections

On commence par intégrer le fichier de correction dans le Vault, comme n’importe quelle donnée.

Ensuite, dans notre traitement BV, on identifie les corrections qui n’ont pas encore été prises en compte, grâce à une table de tracking.

Cette table va nous permettre de savoir exactement quelles corrections sont nouvelles.

## Étape 2 – Détermination des partitions à recalculer

À partir de ces corrections, on récupère les dates de valorisation concernées.

Et on construit une liste de partitions à recalculer :
- la partition du flux quotidien (J-1)
- plus les partitions impactées par les corrections

👉 C’est cette liste qui va piloter tout le recalcul.

## Étape 3 – Recalcul ciblé

Ensuite, on rejoue le traitement BV uniquement sur ces partitions :
- on filtre le MAS sur les dates concernées
- on applique les jointures habituelles avec les objets Vault

Et on reconstruit les données comme dans un run classique, mais sur un périmètre réduit.

## Étape 4 – Application des corrections

Une fois les données reconstruites, on applique les corrections via une jointure.

On utilise une règle métier simple :
👉 si une correction existe, elle remplace la valeur
👉 sinon, on garde la valeur d’origine

Et pour garantir la cohérence, on ne garde que la dernière version de correction par clé et par date de valorisation.

## Étape 5 – Écriture optimisée

Une fois le dataset prêt, on écrit dans la BV en utilisant un overwrite dynamique des partitions.

👉 Ça permet d’écraser uniquement les partitions concernées, sans impacter le reste de la table

C’est ce qui rend la solution scalable malgré la volumétrie.

## Étape 6 – Tracking

Enfin, on met à jour la table de tracking avec les corrections traitées.

👉 Ça nous garantit deux choses :
- ne pas retraiter plusieurs fois la même correction
- assurer que le traitement est idempotent et rejouable
