# Chiffrage

| Phase                    | Activités                                                                                                                                  | Charge estimée (JH) |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------: |
| Analyse métier           | Compréhension des règles de correction, latest record par BK, stratégie tracking, gestion des anciennes partitions, stratégie d’overwrite  |             1.5 – 2 |
| Analyse technique        | Analyse impact BV existante, MAS + joins, validation perf sur 40M+, partition overwrite, tracking table                                    |               2 – 3 |
| Développement            | Création table tracking + logique d’alimentation + idempotence                                                                             |                   1 |
| Développement            | Logique correction BV : lecture correction, latest record, identification partitions, recalcul ciblé, overwrite dynamique, gestion erreurs |               3 – 4 |
| Optimisation performance | Tests Spark, optimisation joins, broadcast, pruning partition, validation temps d’exécution                                                |             1.5 – 2 |
| Tests DEV                | TU + tests techniques : cas standard, ancienne date, correction multiple, idempotence, rejouabilité                                        |                   2 |
| Déploiement Recette      | Package + déploiement en recette                                                                                                           |                 0.5 |
| Support MOA Recette      | Échanges MOA, analyse anomalies, ajustements éventuels                                                                                     |               1 – 2 |
| Control-M                | Préparation dossier ordonnancement, dépendances, documentation exploitation                                                                |                   1 |
| DPI                      | Intervention DPI pour déploiement PRE PROD / PROD (hors charge MOE)                                                                        |               1 – 2 |
| Pré-prod                 | Support déploiement PRE PROD                                                                                                               |                 0.5 |
| Support MOA Pré-prod     | Recette MOA PRE PROD, validation finale                                                                                                    |               1 – 2 |
| Production               | Mise en production + supervision initiale                                                                                                  |             0.5 – 1 |


# Total MOE estimé
| Type                               |         Charge |
| ---------------------------------- | -------------: |
| Total MOE réaliste                 | **13 – 18 JH** |
| Total recommandé avec marge projet | **15 – 20 JH** |
