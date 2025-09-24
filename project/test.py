from database import Database
import os
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def test_connection():
    db = Database(CONNECTION_STRING, DATABASE_NAME)
    is_connected = db.test_connection()
    print(f"test_connection: {'OK' if is_connected else 'ERREUR'}")
    return db if is_connected else None

def test_all_operations():
    connection_string = CONNECTION_STRING
    database_name = DATABASE_NAME

    db = Database(connection_string, database_name)

    if not db.test_connection():
        print("Fail")
        return

    print("Success")

    db.db.get_collection('users').delete_many({})
    db.db.get_collection('projects').delete_many({})
    db.db.get_collection('tasks').delete_many({})

    users_data = [
        {"name": "Alice", "email": "alice@test.com", "age": 25, "city": "Paris", "salary": 50000, "skills": ["Python", "MongoDB", "FastAPI"], "department": "Engineering"},
        {"name": "Bob", "email": "bob@test.com", "age": 30, "city": "Lyon", "salary": 45000, "skills": ["JavaScript", "React", "Node.js"], "department": "Frontend"},
        {"name": "Charlie", "email": "charlie@test.com", "age": 35, "city": "Paris", "salary": 60000, "skills": ["Python", "Django", "PostgreSQL"], "department": "Backend"},
        {"name": "Diana", "email": "diana@test.com", "age": 28, "city": "Marseille", "salary": 55000, "skills": ["Java", "Spring", "MySQL"], "department": "Backend"},
        {"name": "Eve", "email": "eve@test.com", "age": 32, "city": "Paris", "salary": 70000, "skills": ["Python", "FastAPI", "MongoDB"], "department": "Engineering"},
        {"name": "Frank", "email": "frank@test.com", "age": 27, "city": "Nice", "salary": 48000, "skills": ["Vue.js", "TypeScript"], "department": "Frontend"},
        {"name": "Grace", "email": "grace@test.com", "age": 29, "city": "Paris", "salary": 65000, "skills": ["Python", "Machine Learning"], "department": "Data Science"}
    ]

    projects_data = [
        {"title": "Projet A", "budget": 100000, "status": "active", "team_size": 5, "priority": "high"},
        {"title": "Projet B", "budget": 150000, "status": "completed", "team_size": 8, "priority": "medium"},
        {"title": "Projet C", "budget": 80000, "status": "active", "team_size": 3, "priority": "low"},
        {"title": "Projet D", "budget": 200000, "status": "planning", "team_size": 10, "priority": "high"},
        {"title": "Projet E", "budget": 120000, "status": "active", "team_size": 6, "priority": "medium"}
    ]

    tasks_data = [
        {"title": "Task 1", "assignee": "Alice", "status": "todo", "priority": "high", "estimated_hours": 8},
        {"title": "Task 2", "assignee": "Bob", "status": "in_progress", "priority": "medium", "estimated_hours": 12},
        {"title": "Task 3", "assignee": "Charlie", "status": "done", "priority": "low", "estimated_hours": 4},
        {"title": "Task 4", "assignee": "Alice", "status": "todo", "priority": "high", "estimated_hours": 16},
        {"title": "Task 5", "assignee": "Diana", "status": "in_progress", "priority": "medium", "estimated_hours": 6}
    ]

    # Test création d'items
    print("\ncreate_items")
    user_pids = db.create_items("users", users_data, "admin")
    project_pids = db.create_items("projects", projects_data, "admin")
    task_pids = db.create_items("tasks", tasks_data, "admin")
    print(f"Créé {len(user_pids)} utilisateurs, {len(project_pids)} projets, {len(task_pids)} tâches")

    # Test récupération
    print("\nget_items")
    paris_users = db.get_items(
        "users",
        {"city": "Paris"},
        fields=["name", "age", "salary", "department"],
        sort={"salary": -1},
        limit=3
    )
    print(f"Top 3 salaires parisiens:")
    for user in paris_users:
        print(f"  - {user['name']}: {user['salary']}€ ({user['department']})")

    # Test get_items avec stats et pagination
    print("\nget_items avec statistiques")
    result_with_stats = db.get_items(
        "users",
        {"age": {"$gte": 25}},
        fields=["name", "age", "salary"],
        sort={"age": 1},
        skip=2,
        limit=3,
        return_stats=True
    )
    print(f"Stats: {result_with_stats['itemsCount']} items totaux, page {result_with_stats['firstIndexReturned']//3 + 1}/{result_with_stats['pagesCount']}")
    print(f"Items retournés: {[u['name'] for u in result_with_stats['items']]}")

    # Test get_item_by_attr
    print("\nget_item_by_attr")
    senior_engineer = db.get_item_by_attr(
        "users",
        {"department": "Engineering"},
        fields=["name", "salary", "skills"],
        pipeline=[
            {"$match": {"age": {"$gte": 30}}},
            {"$sort": {"salary": -1}}
        ]
    )
    print(f"Ingénieur senior le mieux payé: {senior_engineer['name'] if senior_engineer else 'Aucun'} - {senior_engineer['salary'] if senior_engineer else 0}€")

    print("\nget_item_by_pid")
    first_user = db.get_item_by_pid(
        "users",
        user_pids[0],
        fields=["name", "skills", "city"],
        pipeline=[
            {"$addFields": {"skills_count": {"$size": "$skills"}}}
        ]
    )
    print(f"Premier utilisateur: {first_user['name']} de {first_user['city']} avec {first_user.get('skills_count', 0)} compétences")

    # Test statistiques par ville
    print("\naggregate_stats")
    city_stats = db.aggregate_stats("users", "$city")
    print("Statistiques par ville:")
    for stat in city_stats:
        print(f"  {stat['_id']}: {stat.get('count', 0)} employés")

    # Test recherche textuelle
    print("\nsearch_text")
    python_experts = db.search_text("users", "Python", ["skills"], limit=3)
    print("Experts Python:")
    for user in python_experts:
        skills_str = ", ".join(user.get('skills', []))
        print(f"  {user['name']} - {skills_str}")

    # Test mise à jour
    print("\nupdate_items_by_attr")
    updated_count = db.update_items_by_attr(
        "users",
        {"city": "Paris"},
        {"bonus": 1000},
        "admin"
    )
    print(f"Bonus accordé à {updated_count} parisiens")

    # Test array operations
    print("\narray_push_item_by_attr")
    db.array_push_item_by_attr("users", {"name": "Alice"}, "skills", "Docker", "admin")
    alice = db.get_item_by_attr("users", {"name": "Alice"}, fields=["name", "skills"])
    print(f"Nouvelles compétences d'Alice: {alice['skills'] if alice else 'Introuvable'}")

    # Test suppression
    print("\ndelete_items_by_attr")
    deleted_count = db.delete_items_by_attr("projects", {"status": "planning"})
    print(f"Supprimé {deleted_count} projet(s)")

    # Statistiques finales
    print("\nStats")
    final_users = len(db.get_items("users", {}))
    final_projects = len(db.get_items("projects", {}))
    print(f"Utilisateurs: {final_users}")
    print(f"Projets: {final_projects}")

    print("\nTests done")

if __name__ == "__main__":
    test_all_operations()