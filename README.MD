## Moogodb — Nicolas Becharat

ORM pour MongoDB

### Aperçu
- **Langage**: Python
- **DB**: MongoDB
- **Libs clés**: `pymongo`, `python-dotenv`
- **Entrées principales**:
  - `project/database.py`: classe `Database` (toutes les opérations)
  - `project/test.py`: script de démo qui injecte des données et parcourt les fonctionnalités


### Installation
```bash
cd project
python -m venv .venv
pip install -r requirements.txt
```

### Configuration
Créez un fichier `.env` à la racine de `project/` avec vos variables de connexion:
```dotenv
MONGO_CONNECTION_STRING=mongodb+srv://USERNAME:PASSWORD@HOST/?retryWrites=true&w=majority
MONGO_DATABASE_NAME=ma_base
```

### Exécuter la démo
Le script `project/test.py` nettoie des collections de test, insère des jeux de données et illustre les principales méthodes.

```bash
cd project
source .venv/bin/activate
python test.py
```

Vous devriez voir des sorties telles que la création d'items, des requêtes avec tri/limite, des stats, des mises à jour, des opérations sur tableaux et des suppressions.


### API fournie
- **Connexion**
  - `test_connection()` — retourne `True` si la connexion est disponible
- **Création**
  - `create_item(table, item, created_by=None)`
  - `create_items(table, items, created_by=None)`
- **Lecture**
  - `get_item_by_attr(table, attributes, fields=None, pipeline=None)`
  - `get_item_by_pid(table, pid, fields=None, pipeline=None)`
  - `get_items(table, attributes, fields=None, sort=None, skip=0, limit=None, return_stats=False, pipeline=None)`
- **Mise à jour**
  - `update_item_by_attr(table, attributes, item_data, updated_by=None)`
  - `update_items_by_attr(table, attributes, items_data, updated_by=None)`
  - `update_item_by_pid(table, pid, item_data, updated_by=None)`
  - `update_items_by_pids(table, pids, items_data, updated_by=None)`
- **Suppression**
  - `delete_item_by_attr(table, attributes)` / `delete_items_by_attr(...)`
  - `delete_item_by_pid(table, pid)` / `delete_items_by_pids(...)`
- **Opérations sur tableaux**
  - `array_push_item_by_attr(table, attributes, array, new_item, updated_by=None)`
  - `array_pull_item_by_attr(table, attributes, array, item_attr, updated_by=None)`
  - Équivalents par `pid`: `array_push_item_by_pid(...)`, `array_pull_item_by_pid(...)`
- **Agrégations et recherche**
  - `aggregate_stats(table, group_by, match_criteria=None)` — groupements simples avec tri par `count`
  - `aggregate_lookup(table, lookup_table, local_field, foreign_field, as_field)` — jointure `$lookup`
  - `search_text(table, search_text, fields_to_search, limit=None)` — recherche insensible à la casse par regex

Chaque création ajoute automatiquement: `pid` (UUID), `created_at`, `updated_at` et, si fourni, `created_by`. Les mises à jour ajoutent `updated_at` et éventuellement `updated_by`.

### Structure du projet
```
exercices/
  exercices.py
project/
  database.py        # API principale
  requirements.txt   # dépendances Python
  test.py            # script de démonstration
README.MD
```
