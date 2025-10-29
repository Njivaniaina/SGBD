import os
import json
import shutil
from datetime import datetime, date
import re
import hashlib
from typing import Optional, Dict, Any, List


DB_ROOT = "databases"

os.makedirs(DB_ROOT, exist_ok=True)

current_db = None

USERS_PATH = os.path.join(DB_ROOT, "users.json")  # fichier utilisateur 
current_user: Optional[str] = None  # utilisateur courant 

###   Fonction    ###

# parse 
def parse_schema_input(schema_txt):
    cols = []
    if not schema_txt.strip():
        return cols
    parts = schema_txt.split(",")
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if ":" in p:
            name, typ = p.split(":", 1)
            cols.append({"name": name.strip(), "type": typ.strip().lower()})
        else:
            # si pas de type fourni => str par défaut
            cols.append({"name": p.strip(), "type": "str"})
    return cols

# convertion en type python ( Vérification )
def convert_input_to_type(value_str, type_str):
    if value_str == "":
        if type_str == "str":
            return ""
        return None

    t = type_str.lower()
    if t == "str":
        return value_str
    if t == "int":
        return int(value_str)
    if t == "float":
        return float(value_str)
    if t == "bool":
        low = value_str.strip().lower()
        if low in ("true", "1", "yes", "y", "oui", "o"):
            return True
        if low in ("false", "0", "no", "n", "non"):
            return False
        raise ValueError(f"bool attendu (true/false), reçu: {value_str}")
    if t == "date":
        # YYYY-MM-DD
        try:
            return date.fromisoformat(value_str)
        except Exception as e:
            raise ValueError("date au format ISO attendu: YYYY-MM-DD") from e
    if t == "datetime":
        # YYYY-MM-DDTHH:MM:SS
        try:
            return datetime.fromisoformat(value_str)
        except Exception as e:
            raise ValueError("datetime au format ISO attendu: YYYY-MM-DDTHH:MM:SS") from e
    if t == "list" or t == "dict":
        try:
            val = json.loads(value_str)
            if t == "list" and not isinstance(val, list):
                raise ValueError("list attendu (format JSON)")
            if t == "dict" and not isinstance(val, dict):
                raise ValueError("dict attendu (format JSON)")
            return val
        except json.JSONDecodeError as e:
            raise ValueError("JSON invalide pour list/dict") from e
    # fallback: accepter n'importe quel type sous forme de chaîne
    raise ValueError(f"Type non supporté: {type_str}")

# transform en str pour ecrire 
def serializable_value(val):
    if isinstance(val, date) and not isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, datetime):
        return val.isoformat()
    return val

# message 
def ensure_db_selected():
    if not current_db:
        print("Sélectionnez d'abord une base avec 'use <nom_base>'.")
        return False
    return True


# Pour la fonction recherche 
def _parse_single_condition(cond_txt): # fonction de la condition  ( colonne value opérateur)
    cond_txt = cond_txt.strip()

    # regex pour extraire col, op, value
    m = re.match(r'^([A-Za-z0-9_.]+)\s*(==|=|!=|>=|<=|>|<|(?i:like))\s*(\'[^\']*\'|"[^"]*"|[^ ]+)$', cond_txt)
    
    if not m:
        return None
    
    col, op, val = m.group(1), m.group(2).lower(), m.group(3)
    
    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
        val = val[1:-1]

    return col, op, val

# condition séparer 
def _parse_where_clause(where_txt): 
    parts = re.split(r'\s+(?i:and)\s+', where_txt)
    conds = []
    for p in parts:
        parsed = _parse_single_condition(p)
        if not parsed:
            return None
        conds.append(parsed)
    return conds

def _match_condition(row, schema_map, col, op, val_str):
    
    row_val = row.get(col, None)
    col_type = schema_map.get(col, "str")

    # pour like 
    if op == "like":

        pattern = re.escape(val_str)
        pattern = pattern.replace(r'\%', '.*') # regex, % -> *

        try:
            return re.search(pattern, str(row_val), flags=re.IGNORECASE) is not None
        except re.error:
            return False

    # si pas de value 
    if row_val is None:
        if (op in ("=", "==")) and val_str.lower() in ("null", "none"):
            return True
        return False

    # convertion de type à rechercher 
    try:
        left = convert_input_to_type(str(row_val), col_type)
    except Exception:
        left = row_val

    try:
        right = convert_input_to_type(val_str, col_type)
    except Exception:
        try:
            if isinstance(left, int):
                right = int(val_str)
            elif isinstance(left, float):
                right = float(val_str)
            elif isinstance(left, bool):
                right = val_str.lower() in ("true", "1", "yes", "o", "oui")
            else:
                right = val_str
        except Exception:
            right = val_str

    # Comparaisons
    try:
        if op in ("=", "=="):
            return left == right
        if op == "!=":
            return left != right
        if op == ">":
            return left > right
        if op == "<":
            return left < right
        if op == ">=":
            return left >= right
        if op == "<=":
            return left <= right
    except Exception:
        return False

    return False





###   Base de donnée   ###

# Creation de base de donnée
def create_db(db_name):
    global current_db
    path = os.path.join(DB_ROOT, db_name)
    if os.path.exists(path):
        print(f"La base '{db_name}' existe déjà.")
    else:
        os.makedirs(path)
        current_db = db_name
        print(f"Base de données '{db_name}' créée avec succès.")

# Utilise une base de donnée
def use_db(db_name):
    global current_db
    path = os.path.join(DB_ROOT, db_name)
    if os.path.exists(path) and os.path.isdir(path):
        current_db = db_name
        print(f"Vous utilisez maintenant la base '{db_name}'.")
    else:
        print(f"La base '{db_name}' n'existe pas.")

# List les bases de données
def list_dbs():
    dbs = [d for d in os.listdir(DB_ROOT) if os.path.isdir(os.path.join(DB_ROOT, d))]
    if not dbs:
        print("Aucune base de données trouvée.")
    else:
        print("Bases de données disponibles :")
        for db in dbs:
            print(f" - {db}")

# Suppression 
def delete_db(db_name):
    path = os.path.join(DB_ROOT, db_name)
    if not os.path.exists(path):
        print(f"La base '{db_name}' n'existe pas.")
        return

        # permission : suppression de la base
    # on exige un admin/droit drop_db sur la base cible
    if not require_permission(db_name, "drop_db"):
        return


    confirm = input(f"Voulez-vous vraiment supprimer '{db_name}' ? (oui/non) : ").strip().lower()
    if confirm == "oui":
        shutil.rmtree(path)
        global current_db
        if current_db == db_name:
            # si utilisée
            current_db = None  
        print(f"Base de données '{db_name}' supprimée.")
    else:
        print("Suppression annulée.")


###########################

###     Partie concernant les tables   ###

# Création de table 
def create_table(table_name):
    if not ensure_db_selected():
        return

    if not require_permission(current_db, "create_table"):
        return

    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if os.path.exists(schema_path):
        print(f"⚠ La table '{table_name}' existe déjà.")
        return

    print("\n=== Création de la table ===")
    schema = []
    auto_inc_present = False

    while True:
        col_name = input("Nom de la colonne (ou vide pour terminer) : ").strip()
        if not col_name:
            break

        # Type de donnée
        col_type = input("Type (int, float, str, bool, date, datetime, list, dict) : ").strip().lower()
        if col_type not in ["int", "float", "str", "bool", "date", "datetime", "list", "dict"]:
            print("Type invalide !")
            continue

        not_null = input("NOT NULL ? (o/n) : ").strip().lower() == "o"
        unique = input("UNIQUE ? (o/n) : ").strip().lower() == "o"
        auto_increment = False
        #primary_key = False
        default_val = None

        if col_type == "int":
            auto_increment = input("AUTO_INCREMENT ? (o/n) : ").strip().lower() == "o"
            if auto_increment:
                if auto_inc_present:
                    print("Une seule colonne AUTO_INCREMENT est autorisée par table.")
                    auto_increment = False
                else:
                    auto_inc_present = True

        # primary_key = input("PRIMARY KEY ? (o/n) : ").strip().lower() == "o"

        default_input = input("Valeur par défaut (laisser vide si aucune) : ").strip()
        if default_input:
            try:
                default_val = convert_input_to_type(default_input, col_type)
            except Exception:
                print("Valeur par défaut invalide — ignorée.")
                default_val = None

        schema.append({
            "name": col_name,
            "type": col_type,
            "not_null": not_null,
            "unique": unique,
            "auto_increment": auto_increment,
            "default": default_val,
            #            "primary_key": primary_key
        })

    if not schema:
        print("Aucun champ défini — table non créée.")
        return

    # Sauvegarde du schéma
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    print(f"Table '{table_name}' créée avec succès.")


# Lister les tables
def list_tables():
    if not ensure_db_selected():
        return

        # permission : lecture/listage des tables
    if not require_permission(current_db, "read"):
        return


    db_path = os.path.join(DB_ROOT, current_db)
    files = os.listdir(db_path)

    tables = set()
    for f in files:
        if f.endswith("_schema.json"):
            tables.add(f[:-12]) 
    if not tables:
        print("Aucune table trouvée dans la base sélectionnée.")
    else:
        print(f"Tables dans la base '{current_db}':")
        for t in sorted(tables):
            print(f" - {t}")


# Suppression 
def delete_table(table_name):
    if not ensure_db_selected():
        return

        # permission : suppression de table
    if not require_permission(current_db, "drop_table"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path) and not os.path.exists(data_path):
        print(f"La table '{table_name}' n'existe pas dans la base '{current_db}'.")
        return

    confirm = input(f"Supprimer la table '{table_name}' (schema + données) ? (oui/non) : ").strip().lower()
    if confirm not in ("oui", "o", "yes", "y"):
        print("Suppression annulée.")
        return

    try:
        if os.path.exists(schema_path):
            os.remove(schema_path)
        if os.path.exists(data_path):
            os.remove(data_path)
        print(f"Table '{table_name}' supprimée de la base '{current_db}'.")
    except Exception as e:
        print("Erreur lors de la suppression :", e)


# Description du table
def describe_table(table_name):
    if not ensure_db_selected():
        return

        # permission : lecture des métadonnées
    if not require_permission(current_db, "read"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas (pas de schema).")
        return

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        print("Impossible de lire le schema :", e)
        return

    data = []
    if os.path.exists(data_path):
        try:
            with open(data_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print("Impossible de lire data (ou fichier vide/corrompu) :", e)

    nrows = len(data) if isinstance(data, list) else 0

    # affichage
    print(f"Description de la table '{table_name}' dans la base '{current_db}':")

    # Colonnes avec types et contraintes
    cols = []
    if isinstance(schema, list):
        for c in schema:
            if isinstance(c, dict) and "name" in c and "type" in c:
                pieces = [f"{c['name']}:{c['type']}"]
                # contraintes optionnelles
                if c.get("not_null"):
                    pieces.append("NOT NULL")
                if c.get("unique"):
                    pieces.append("UNIQUE")
                if c.get("auto_increment"):
                    pieces.append("AUTO_INCREMENT")
                if "default" in c and c.get("default") is not None:
                    pieces.append(f"DEFAULT={c.get('default')}")
                cols.append(" ".join(pieces))
            elif isinstance(c, str):
                # compatibilité: chaine seule -> type par défaut str
                cols.append(f"{c}:str")
    elif isinstance(schema, dict):
        for k, v in schema.items():
            cols.append(f"{k}:{v}")

    print(f" - Colonnes ({len(cols)}):")
    if cols:
        for item in cols:
            print(f"    - {item}")
    else:
        print("    (aucune)")

    # nombre de lignes
    # print(f" - Nombre de lignes : {nrows}")

    # Exemples 
    #if nrows == 0:
    #    print(" - Table vide — aucun exemple à afficher.")
    #else:
    #    print(" - Exemples (premières 5 lignes) :")
    #    for i, row in enumerate(data[:5], start=1):
    #        print(f"   {i}. {row}")


# Modifier le table (supprimer/ajouter/modifier)
def alter_table(table_name):
    if not ensure_db_selected():
        return

        # permission : modifier le schéma de la table
    if not require_permission(current_db, "alter_table"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas.")
        return

    # Lire le schema et les données
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(data_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Afficher le schéma actuel (nom, type, contraintes)
    print("Schéma actuel :")
    for col in schema:
        desc = f"{col.get('name')} ({col.get('type')})"
        flags = []
        if col.get("not_null"): flags.append("NOT NULL")
        if col.get("unique"): flags.append("UNIQUE")
        if col.get("auto_increment"): flags.append("AUTO_INCREMENT")
        if "default" in col and col.get("default") is not None: flags.append(f"DEFAULT={col.get('default')}")
        if flags:
            desc += " [" + ", ".join(flags) + "]"
        print(f" - {desc}")

    print("\nActions disponibles :")
    print("1. Ajouter une colonne")
    print("2. Supprimer une colonne")
    print("3. Modifier le type d'une colonne")
    print("4. Renommer une colonne")
    choice = input("Choisir une action (1/2/3/4) : ").strip()

    valid_types = {"str", "int", "float", "bool", "date", "datetime", "list", "dict"}

    # Helper : vérifier s'il existe déjà une colonne auto_increment
    def has_auto_increment(sch):
        return any(c.get("auto_increment") for c in sch)

    # ajout  
    if choice == "1":
        new_col_name = input("Nom de la nouvelle colonne : ").strip()
        if any(c["name"] == new_col_name for c in schema):
            print(f"Une colonne '{new_col_name}' existe déjà.")
            return

        new_col_type = input("Type de la colonne : ").strip().lower()
        if new_col_type not in valid_types:
            print(f"Type invalide '{new_col_type}', abandon.")
            return

        not_null = input("NOT NULL ? (o/n) : ").strip().lower() == "o"
        unique = input("UNIQUE ? (o/n) : ").strip().lower() == "o"
        auto_increment = False
        default_val = None

        if new_col_type == "int":
            auto_increment = input("AUTO_INCREMENT ? (o/n) : ").strip().lower() == "o"
            if auto_increment and has_auto_increment(schema):
                print("⚠ Une colonne AUTO_INCREMENT existe déjà. AUTO_INCREMENT annulé pour cette colonne.")
                auto_increment = False

        default_input = input("Valeur par défaut (laisser vide si aucune) : ").strip()
        if default_input != "":
            try:
                default_val = convert_input_to_type(default_input, new_col_type)
                default_val = serializable_value(default_val)
            except Exception:
                print("Valeur par défaut invalide — ignorée.")
                default_val = None

        # ajouter au schema
        new_col = {
            "name": new_col_name,
            "type": new_col_type,
            "not_null": not_null,
            "unique": unique,
            "auto_increment": auto_increment,
            "default": default_val
        }
        schema.append(new_col)

        # si il existe , ajouter le max 
        if auto_increment:

            existing_vals = [row.get(new_col_name) for row in data if row.get(new_col_name) is not None]
            max_val = 0

            # recherche de max 
            for v in existing_vals:
                try:
                    iv = int(v)
                    if iv > max_val:
                        max_val = iv
                except Exception:
                    pass

            # assigner auto-incr
            next_val = max_val + 1
            for row in data:
                if new_col_name not in row or row.get(new_col_name) is None:
                    row[new_col_name] = serializable_value(next_val)
                    next_val += 1
        else:
            # assigner default ou None
            for row in data:
                if new_col_name not in row:
                    row[new_col_name] = default_val if default_val is not None else None

        print(f"Colonne '{new_col_name}' ajoutée (type {new_col_type}).")

    # Suppression 
    elif choice == "2":
        del_col_name = input("Nom de la colonne à supprimer : ").strip()
        if not any(c["name"] == del_col_name for c in schema):
            print(f"Colonne '{del_col_name}' introuvable.")
            return
        schema = [c for c in schema if c["name"] != del_col_name]
        for row in data:
            row.pop(del_col_name, None)
        print(f"Colonne '{del_col_name}' supprimée.")

    # modification 
    elif choice == "3":
        col_name = input("Nom de la colonne à modifier : ").strip()
        col = next((c for c in schema if c["name"] == col_name), None)
        if not col:
            print(f"Colonne '{col_name}' introuvable.")
            return

        new_type = input(f"Nouveau type pour '{col_name}' : ").strip().lower()
        if new_type not in valid_types:
            print(f"Type invalide '{new_type}', abandon.")
            return

        # si colonne auto_increment 
        if col.get("auto_increment") and new_type != "int":
            print("Impossible : colonne AUTO_INCREMENT doit rester de type int. Abandon.")
            return

        # conversion 
        for i, row in enumerate(data):
            raw_val = row.get(col_name)
            if raw_val is None:
                continue
            try:
                conv = convert_input_to_type(str(raw_val), new_type)
                row[col_name] = serializable_value(conv)
            except Exception:
                print(f"Impossible de convertir la valeur '{raw_val}' (ligne {i+1}). Mise à None.")
                row[col_name] = None

        # verification double si unique 
        if col.get("unique"):
            seen = set()
            dup_found = False
            for i, row in enumerate(data):
                v = row.get(col_name)
                if v is None:
                    continue
                if v in seen:
                    print(f"Valeur dupliquée après conversion trouvée (ligne {i+1}) — la valeur sera mise à None.")
                    row[col_name] = None
                    dup_found = True
                else:
                    seen.add(v)
            if dup_found:
                print("Des doublons ont été éliminés (mis à None) pour satisfaire UNIQUE.")

        # mise à jour 
        col["type"] = new_type
        print(f"Type de '{col_name}' modifié en '{new_type}'.")

    # Renommer 
    elif choice == "4":
        old_name = input("Nom de la colonne à renommer : ").strip()
        new_name = input("Nouveau nom : ").strip()
        if not any(c["name"] == old_name for c in schema):
            print(f"Colonne '{old_name}' introuvable.")
            return
        if any(c["name"] == new_name for c in schema):
            print(f"Une colonne '{new_name}' existe déjà.")
            return

        # Modifier dans le schéma
        for c in schema:
            if c["name"] == old_name:
                c["name"] = new_name
                break

        # Modifier dans les données
        for row in data:
            if old_name in row:
                row[new_name] = row.pop(old_name)

        print(f"Colonne '{old_name}' renommée en '{new_name}'.")

    else:
        print("Action invalide.")
        return

    # Sauvegarde
    try:
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=4, ensure_ascii=False)
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print("Erreur lors de la sauvegarde :", e)
        return

    print("Fin de modification.")



###   Pour gestion de données 

# Vérification de l'unicité 
def is_unique_violation(col_name, candidate_serialized):
        for row in data:
            existing = row.get(col_name, None)
            if existing is None and candidate_serialized is None:
                return True
            if existing == candidate_serialized:
                return True
        return False

# Insertion dans une table 
def insert_data(table_name):
    if not ensure_db_selected():
        return

        # permission : écriture/insertion dans la table
    if not require_permission(current_db, "write"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas.")
        return

    # Charger le schéma
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        print("Impossible de lire le schema :", e)
        return

    # Charger les données 
    try:
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                data = []
    except Exception:
        data = []

    schema_map = {}
    for col in schema:
        if isinstance(col, dict):
            schema_map[col["name"]] = col
        else:
            schema_map[str(col)] = {"name": str(col), "type": "str"}

    

    new_record = {}

    # auto incrémenter: get max 
    auto_inc_cols = [c["name"] for c in schema if isinstance(c, dict) and c.get("auto_increment")]
    auto_inc_next = {}
    for col_name in auto_inc_cols:
        max_val = 0
        for row in data:
            v = row.get(col_name)
            if v is None:
                continue
            try:
                iv = int(v)
                if iv > max_val:
                    max_val = iv
            except Exception:
                continue
        auto_inc_next[col_name] = max_val + 1

    # Itérer sur colonnes selon l'ordre du schema
    for col in schema:
        # compatibilité
        if isinstance(col, str):
            col_name = col
            col_type = "str"
            col_constraints = {}
        else:
            col_name = col.get("name")
            col_type = col.get("type", "str")
            col_constraints = col  

        # Si auto_increment -> assigner automatiquement (pas de prompt)
        if col_constraints.get("auto_increment"):
            val = auto_inc_next.get(col_name, 1)
            new_record[col_name] = serializable_value(val)
            auto_inc_next[col_name] = val + 1
            print(f"{col_name} (AUTO_INCREMENT) = {val}")
            continue

        # boucle variable 
        while True:
            default_display = ""
            if "default" in col_constraints and col_constraints.get("default") is not None:
                default_display = f" [default: {col_constraints.get('default')}]"
            raw = input(f"{col_name} ({col_type}){default_display} = ").strip()

            # Si vide et default fourni -> utiliser default
            if raw == "" and "default" in col_constraints and col_constraints.get("default") is not None:
                try:
                    default_raw = col_constraints.get("default")
                    converted_default = default_raw
                    if not isinstance(default_raw, (int, float, bool, list, dict)):
                        converted_default = convert_input_to_type(str(default_raw), col_type)
                    new_val = serializable_value(converted_default)
                except Exception:
                    new_val = default_raw
                candidate_serialized = new_val

            elif raw == "":
                candidate_serialized = None
            else:
                try:
                    conv = convert_input_to_type(raw, col_type)
                    candidate_serialized = serializable_value(conv)
                except ValueError as ve:
                    print(f"Valeur invalide pour {col_name} (type {col_type}) : {ve}")
                    print("Réessaye (ou laisse vide pour NULL).")
                    continue

            # Vérifier NOT NULL
            if col_constraints.get("not_null") and (candidate_serialized is None or candidate_serialized == ""):
                print(f"Violation: colonne '{col_name}' est NOT NULL — une valeur est requise.")
                if "default" in col_constraints and col_constraints.get("default") is not None:
                    use_def = input("Utiliser la valeur DEFAULT ? (o/n) : ").strip().lower()
                    if use_def == "o":
                        try:
                            default_raw = col_constraints.get("default")
                            converted_default = default_raw
                            if not isinstance(default_raw, (int, float, bool, list, dict)):
                                converted_default = convert_input_to_type(str(default_raw), col_type)
                            candidate_serialized = serializable_value(converted_default)
                        except Exception:
                            candidate_serialized = None
                        if candidate_serialized is None:
                            print("DEFAULT non applicable, réessaye.")
                            continue
                    else:
                        continue
                else:
                    continue

            # Vérifier UNIQUE
            if col_constraints.get("unique") and candidate_serialized is not None:
                if is_unique_violation(col_name, candidate_serialized):
                    print(f"Violation UNIQUE: la valeur '{candidate_serialized}' existe déjà dans '{col_name}'.")
                    retry = input("Entrer une autre valeur ? (o/n) : ").strip().lower()
                    if retry == "o":
                        continue
                    else:
                        print("Insertion annulée.")
                        return  
            new_record[col_name] = candidate_serialized
            break

    # enregistrement
    data.append(new_record)

    # Sauvegarder
    try:
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print("Donnée insérée avec succès.")
    except Exception as e:
        print("Erreur lors de la sauvegarde :", e)



# Affichage de tables 
def select_table(table_name, columns=None):
    if not ensure_db_selected():
        return

        # permission : lecture des données
    if not require_permission(current_db, "read"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas.")
        return

    # charge les données 
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(data_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_columns = [col["name"] for col in schema]

    # Si aucune
    if not columns or columns == ["*"]:
        columns = all_columns
    else:
        # Vérifier que les colonnes existent
        for col in columns:
            if col not in all_columns:
                print(f"Colonne '{col}' inexistante dans la table '{table_name}'.")
                return

    # Affichage formaté
    print(f"\nRésultat de select {', '.join(columns)} from {table_name} :")
    if not data:
        print("(aucune ligne)")
        return

    # Largeurs automatiques pour un affichage tabulaire
    col_widths = {col: max(len(col), max((len(str(row.get(col, ''))) for row in data), default=0)) for col in columns}

    # Ligne d’en-tête
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    # Lignes de données
    for row in data:
        line = " | ".join(str(row.get(col, "")).ljust(col_widths[col]) for col in columns)
        print(line)

    print("-" * len(header))
    print(f"({len(data)} ligne{'s' if len(data) > 1 else ''})\n")

    
# recherche
def search_table(table_name, columns=None, where_clause=None):
    if not ensure_db_selected():
        return

        # permission : lecture des données (filter/search)
    if not require_permission(current_db, "read"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas.")
        return

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        print("Impossible de lire le schema :", e)
        return
    try:
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print("Impossible de lire les données :", e)
        return

    if not isinstance(schema, list):
        print("Schema invalide.")
        return

    schema_cols = [c["name"] if isinstance(c, dict) else str(c) for c in schema]
    
    schema_map = {}
    for c in schema:
        if isinstance(c, dict):
            schema_map[c["name"]] = c.get("type", "str")
        else:
            schema_map[str(c)] = "str"

    # colonnes à afficher
    all_columns = schema_cols
    if not columns or columns == ["*"]:
        columns = all_columns
    else:
        # vérifier existence
        for col in columns:
            if col not in all_columns:
                print(f"Colonne '{col}' inexistante dans la table '{table_name}'.")
                return

    # analyse de conditions 
    conditions = []
    if where_clause and where_clause.strip():
        parsed = _parse_where_clause(where_clause)
        if parsed is None:
            print("Impossible de parser la clause WHERE. Syntaxe attendue: col op value [and col2 op2 value2 ...]")
            return
        conditions = parsed

    # filtrage des lignes
    matched = []
    for row in data:
        ok = True
        for (col, op, val_str) in conditions:
            if col not in schema_map:
                ok = False
                break
            if not _match_condition(row, schema_map, col, op, val_str):
                ok = False
                break
        if ok:
            matched.append(row)

    # affichage tabulaire (comme select_table)
    # print(f"\nRésultat de search {','.join(columns)} FROM {table_name}" + (f" WHERE {where_clause}" if where_clause else "") + " :")
    if not matched:
        print("(aucune ligne)")
        return

    col_widths = {col: max(len(col), max((len(str(r.get(col, ""))) for r in matched), default=0)) for col in columns}
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    print("-" * len(header))
    print(header)
    print("-" * len(header))
    for row in matched:
        line = " | ".join(str(row.get(col, "")).ljust(col_widths[col]) for col in columns)
        print(line)
    print("-" * len(header))
    print(f"({len(matched)} ligne{'s' if len(matched) > 1 else ''})\n")

# modification de valeur
def alter_on_tables(table_name, where_clause):
    if not ensure_db_selected():
        return

        # permission : modification des données (bulk update)
    if not require_permission(current_db, "write"):
        return


    schema_path = os.path.join(DB_ROOT, current_db, f"{table_name}_schema.json")
    data_path = os.path.join(DB_ROOT, current_db, f"{table_name}_data.json")

    if not os.path.exists(schema_path):
        print(f"La table '{table_name}' n'existe pas.")
        return

    # Charger schema et données
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        print("Impossible de lire le schema :", e)
        return
    try:
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                print("Format de données invalide.")
                return
    except Exception as e:
        print("Impossible de lire les données :", e)
        return

    schema_map = {}
    schema_order = []
    for c in schema:
        if isinstance(c, dict):
            schema_map[c["name"]] = c
            schema_order.append(c["name"])
        else:
            schema_map[str(c)] = {"name": str(c), "type": "str"}
            schema_order.append(str(c))

    # condition de where 
    if where_clause and where_clause.strip():
        conds = _parse_where_clause(where_clause)
        if conds is None:
            print("Impossible de parser la clause WHERE. Syntaxe invalide.")
            return
    else:
        ok = input("Aucune condition fournie — modifier toutes les lignes ? (oui/non) : ").strip().lower()
        if ok not in ("oui","o","yes","y"):
            print("Annulé.")
            return
        conds = []

    # Trouver indices des lignes correspondantes
    matched_indices = []
    for idx, row in enumerate(data):
        ok = True
        for col, op, val in conds:
            if col not in schema_map:
                ok = False
                break
            if not _match_condition(row, {k: v.get("type","str") for k,v in schema_map.items()}, col, op, val):
                ok = False
                break
        if ok:
            matched_indices.append(idx)

    if not matched_indices:
        print("Aucune ligne trouvée pour la condition donnée.")
        return

    print(f"{len(matched_indices)} ligne(s) trouvée(s).")

    # Helper pour tester unicité (exclut la ligne en cours)
    def unique_conflict(col_name, candidate_serialized, exclude_idx):
        if candidate_serialized is None:
            return False
        for i, r in enumerate(data):
            if i == exclude_idx:
                continue
            if r.get(col_name) == candidate_serialized:
                return True
        return False

    # Pour chaque ligne correspondante, proposer modifications
    for i, row_idx in enumerate(matched_indices, start=1):
        row = data[row_idx]
        print("\n" + "="*40)
        print(f"Ligne {i} (index interne {row_idx}):")
        
        for col_name in schema_order:
            print(f"  - {col_name} = {row.get(col_name)}")
        print("="*40)

        # Confirmation de modification 
        mod = input("Modifier cette ligne ? 'n' pour passer à la suivante ou quitter. (o/n) : ").strip().lower()
        if mod not in ("o","oui","y","yes","1"):
            print("Passé.")
            continue

        # Choix : modification 
        cols_choice = input("Modifier (all = toutes colonnes, liste séparée par ,) : ").strip()
        if cols_choice.lower() in ("all","*","allcols"):
            cols_to_edit = [c for c in schema_order]
        else:
            cols_to_edit = [c.strip() for c in cols_choice.split(",") if c.strip()]

        # Valider que les colonnes existent
        invalid = [c for c in cols_to_edit if c not in schema_map]
        if invalid:
            print(f"Colonnes inconnues : {invalid}. Ligne ignorée.")
            continue

        # Pour chaque colonne demandée, faire le prompt et validation
        row_changed = False
        for col_name in cols_to_edit:
            col_def = schema_map[col_name]
            col_type = col_def.get("type","str")
            # si colonne auto_increment -> interdiction de modifier manuellement
            if col_def.get("auto_increment"):
                print(f" - {col_name} est AUTO_INCREMENT — modification interdite.")
                continue

            current_val = row.get(col_name)
            print(f"Valeur actuelle de {col_name} ({col_type}) : {current_val}")
            new_raw = input(f"Nouveau {col_name} (laisser vide pour ne pas changer, 'NULL' pour None): ").strip()

            if new_raw == "":
                continue
            if new_raw.lower() == "null":
                candidate = None
            else:
                # conversion selon type
                try:
                    conv = convert_input_to_type(new_raw, col_type)
                    candidate = serializable_value(conv)
                except Exception as e:
                    print(f"  Valeur invalide pour {col_name} (type {col_type}) : {e}. Champ non modifié.")
                    continue

            # Vérifier NOT NULL
            if col_def.get("not_null") and (candidate is None or candidate == ""):
                print(f"  Violation NOT NULL pour '{col_name}' — champ non modifié.")
                continue

            # Vérifier UNIQUE 
            if col_def.get("unique") and unique_conflict(col_name, candidate, row_idx):
                print(f"  Violation UNIQUE: la valeur {candidate} existe déjà ailleurs — champ non modifié.")
                continue

            row[col_name] = candidate
            row_changed = True
            print(f"  {col_name} mis à jour -> {candidate}")

        if row_changed:
            # écrire la mise à jour dans data (déjà modifié in-place)
            print("Ligne mise à jour.")
        else:
            print("Aucune modification appliquée à cette ligne.")

    # Après toutes les modifications, sauvegarder
    try:
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print("\nSauvegarde terminée. Modifications enregistrées.")
    except Exception as e:
        print("Erreur lors de la sauvegarde :", e)


# ---------- Gestion des utilisateurs & droits (stockage : databases/users.json) ----------
def write_json_atomic(path: str, obj):
    """
    Écriture atomique : écrire dans un fichier .tmp puis os.replace
    """
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

def load_users() -> Dict[str, Any]:
    """Charge la table users depuis USERS_PATH (retourne {} si absent)."""
    if not os.path.exists(USERS_PATH):
        return {}
    try:
        with open(USERS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}

def save_users(users: Dict[str, Any]):
    """Sauvegarde atomique du dictionnaire users."""
    os.makedirs(os.path.dirname(USERS_PATH), exist_ok=True)
    write_json_atomic(USERS_PATH, users)

def _hash_password(password: str) -> str:
    """Hash simple SHA-256 (pour prototype). Utilise sel+KDF en prod."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

# -------- CRUD Utilisateurs --------
def create_user(username: str, password: str, attrs: Optional[Dict[str, Any]] = None) -> bool:
    """
    Crée un utilisateur.
    Retourne True si créé, False si utilisateur existe déjà.
    """
    users = load_users()
    if username in users:
        print(f"Utilisateur '{username}' existe déjà.")
        return False
    users[username] = {
        "password_hash": _hash_password(password),
        "attrs": attrs or {},
        "rights": {}  # droit par base : { "ma_base": ["read","write"] }
    }
    save_users(users)
    print(f"Utilisateur '{username}' créé.")
    return True

def get_user(username: str) -> Optional[Dict[str, Any]]:
    """Retourne le dict utilisateur ou None si absent."""
    users = load_users()
    return users.get(username)

def list_users() -> List[str]:
    """Retourne la liste des noms d'utilisateurs."""
    return sorted(load_users().keys())

def update_user_attrs(username: str, attrs: Dict[str, Any]) -> bool:
    """Remplace/merge les attrs de l'utilisateur (merge shallow)."""
    users = load_users()
    u = users.get(username)
    if not u:
        print(f"Utilisateur '{username}' introuvable.")
        return False
    u_attrs = u.get("attrs", {})
    u_attrs.update(attrs)
    u["attrs"] = u_attrs
    users[username] = u
    save_users(users)
    print(f"Attributs mis à jour pour '{username}'.")
    return True

def update_user_password(username: str, new_password: str) -> bool:
    """Met à jour le mot de passe (hash)."""
    users = load_users()
    u = users.get(username)
    if not u:
        print(f"Utilisateur '{username}' introuvable.")
        return False
    u["password_hash"] = _hash_password(new_password)
    users[username] = u
    save_users(users)
    print(f"Mot de passe mis à jour pour '{username}'.")
    return True

def delete_user(username: str) -> bool:
    """Supprime un utilisateur."""
    users = load_users()
    if username not in users:
        print(f"Utilisateur '{username}' introuvable.")
        return False
    del users[username]
    save_users(users)
    print(f"Utilisateur '{username}' supprimé.")
    global current_user
    if current_user == username:
        current_user = None
    return True

# -------- Gestion des droits (per db) --------
def grant_rights(username: str, db_name: str, rights: List[str]) -> bool:
    """
    Donne des droits à un utilisateur sur une base.
    rights : liste de chaînes (ex: ["read","write"])
    """
    users = load_users()
    u = users.get(username)
    if not u:
        print(f"Utilisateur '{username}' introuvable.")
        return False
    rmap = u.setdefault("rights", {})
    cur = set(rmap.get(db_name, []))
    cur.update(rights)
    rmap[db_name] = sorted(cur)
    users[username] = u
    save_users(users)
    print(f"Droits {rights} accordés à '{username}' sur la base '{db_name}'.")
    return True

def revoke_rights(username: str, db_name: str, rights: List[str]) -> bool:
    """Retire des droits (si présents)."""
    users = load_users()
    u = users.get(username)
    if not u:
        print(f"Utilisateur '{username}' introuvable.")
        return False
    rmap = u.setdefault("rights", {})
    cur = set(rmap.get(db_name, []))
    for r in rights:
        cur.discard(r)
    rmap[db_name] = sorted(cur)
    users[username] = u
    save_users(users)
    print(f"Droits {rights} retirés à '{username}' sur la base '{db_name}'.")
    return True

def get_user_rights(username: str, db_name: Optional[str] = None) -> Dict[str, List[str]]:
    """Retourne le mapping des droits d'un utilisateur ; si db_name fourni, retourne la liste pour cette base."""
    u = get_user(username)
    if not u:
        return {}
    rights = u.get("rights", {})
    if db_name:
        return {db_name: rights.get(db_name, [])}
    return rights

def check_permission(username: str, db_name: str, right: str) -> bool:
    """
    Vérifie si l'utilisateur a le droit demandé sur la base.
    'admin' droit spécial qui autorise tout.
    """
    u = get_user(username)
    if not u:
        return False
    rights = u.get("rights", {})
    db_rights = set(rights.get(db_name, []))
    if "admin" in db_rights:
        return True
    return right in db_rights

# -------- Auth / session simple --------
def authenticate_user(username: str, password: str) -> bool:
    """Vérifie le mot de passe. Ne change pas la session."""
    u = get_user(username)
    if not u:
        return False
    return u.get("password_hash") == _hash_password(password)

def set_current_user(username: Optional[str]):
    """Définit l'utilisateur courant (session simple)."""
    global current_user
    current_user = username
    if username:
        print(f"Utilisateur courant : {username}")
    else:
        print("Aucun utilisateur connecté.")

# -------- Utilitaires CLI (optionnels) --------
def cli_create_user():
    name = input("username = ").strip()
    pwd = input("password = ").strip()
    create_user(name, pwd)

def cli_list_users():
    for u in list_users():
        print(" -", u)

# ---------- Contrôle de permissions ----------
def require_permission(db_name, perm):
    """
    Vérifie que current_user existe et a la permission `perm` sur db_name.
    Affiche un message et retourne False si la permission est refusée.
    """
    # si tu veux permettre l'administration sans login, adapte ici
    global current_user
    if current_user is None:
        print("Permission refusée : aucun utilisateur connecté.")
        return False
    # check_permission doit exister dans ton code (défini précédemment)
    try:
        ok = check_permission(current_user, db_name, perm)
    except Exception:
        # si check_permission absent ou erreur, refuser par sécurité
        print("Permission refusée (erreur de vérification).")
        return False
    if not ok:
        print(f"Permission refusée : l'utilisateur '{current_user}' n'a pas le droit '{perm}' sur la base '{db_name}'.")
        return False
    return True




def help():
    print("=== Mini SGBD JSON (avec types) ===")
    print("Commandes disponibles :")
    print(" create_db <nom>")
    print(" use <nom>")
    print(" create_table <nom>")
    print(" insert <table>")
    print(" select <col1,col2,...> from <table>  ")
    print(" select * from <table>")
    print(" show_db")
    print(" delete_db <nom>")
    print(" show_tables")
    print(" delete_table <nom>")
    print(" describe_table <nom>")
    print(" exit")
    print(" alter_table <nom> ")
    print(" search <col1,col2|*> from <table> [where <cond>]")
    print(" alter_on_tables <table> [where <cond>]")
    print(" user_create")
    print(" user_list")
    print(" user_delete <name>")
    print(" user_grant <user> <db> <right1,right2,...>")
    print(" user_revoke <user> <db> <right1,right2,...>")
    print(" login <user>")
    print(" logout")
    print("======================")



def prompt():
    global current_db
    print("Bienvenue sur Mini SGBD JSON. Tapez 'help' pour une aide.")
    
    while True:
        prefix = current_db if current_db else "no-db"
        cmd = input(f"{prefix}> ").strip().split()

        if not cmd:
            continue

        command = cmd[0]
        args = cmd[1:]

        if command == "exit":
            print("Fin de session.")
            break

        # Partie db
        elif command == "create_db" and len(args) == 1:
            create_db(args[0])
        elif command == "show_db":
            list_dbs()
        elif command == "delete_db" and len(args) == 1:
            delete_db(args[0])
        elif command == "use" and len(args) == 1:
            use_db(args[0])

        # Partie table
        elif command == "show_tables":
            list_tables()
        elif command == "delete_table" and len(args) == 1:
            delete_table(args[0])
        elif command == "describe_table" and len(args) >= 1:
            describe_table(args[0])
        elif command == "alter_table" and len(args) == 1:
            alter_table(args[0])

        elif command == "create_table" and len(args) == 1:
            create_table(args[0])
        elif command == "insert" and len(args) == 1:
            insert_data(args[0])
        elif command == "select" and len(args) >= 3:
            try:
                if "from" not in args:
                    print("Syntaxe : select <colonnes> from <table>")
                    continue

                from_index = args.index("from")
                cols = args[:from_index]
                table = args[from_index + 1]
                cols = " ".join(cols).replace(" ", "").split(",")

                select_table(table, cols)
            except Exception as e:
                print("Erreur de syntaxe ou d'exécution :", e)
        elif command == "search":
            rest = " ".join(args)

            # regex de vérification 
            m = re.match(r'\s*(?P<cols>[^ ]+)\s+from\s+(?P<table>[A-Za-z0-9_]+)(?:\s+where\s+(?P<where>.+))?$', rest, flags=re.I)

            if not m:
                print("Syntaxe: search <col1,col2|*> from <table> [where <cond>]")
            else:
                cols_txt = m.group("cols")
                table = m.group("table")
                where_txt = m.group("where") or ""
                cols = [c.strip() for c in cols_txt.split(",")] if cols_txt != "*" else ["*"]
                search_table(table, cols, where_txt)
        elif command == "alter_on_table":
            rest = " ".join(args)
            
            m = re.match(r'\s*(?P<table>[A-Za-z0-9_]+)(?:\s+where\s+(?P<where>.+))?$', rest, flags=re.I)
            if not m:
                print("Syntaxe: alter_on_tables <table> [where <cond>]")
            else:
                table = m.group("table")
                where_txt = m.group("where") or ""
                alter_on_tables(table, where_txt)

        # Gestion d'utilisateur
        elif command == "user_create":
            cli_create_user()
        elif command == "user_list":
            cli_list_users()
        elif command == "user_delete" and len(args)==1:
            delete_user(args[0])
        elif command == "user_grant" and len(args)==3:
            user, db, rights_txt = args
            rights = [r.strip() for r in rights_txt.split(",") if r.strip()]
            grant_rights(user, db, rights)
        elif command == "user_revoke" and len(args)==3:
            user, db, rights_txt = args
            rights = [r.strip() for r in rights_txt.split(",") if r.strip()]
            revoke_rights(user, db, rights)
        elif command == "login" and len(args)==1:
            pwd = input("password = ")
            if authenticate_user(args[0], pwd):
                set_current_user(args[0])
            else:
                print("Authentification échouée.")
        elif command == "logout":
            set_current_user(None)
        
        
        elif command == "help":
            help()
        else:
            print("Commande invalide ou arguments manquants.")


if __name__ == "__main__":
    prompt()

