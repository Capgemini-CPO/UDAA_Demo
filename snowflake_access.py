import json
import os
import snowflake.connector

# Load Snowflake Secrets
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]

# ✅ Multi-user input (comma-separated)
TARGET_USERS = os.environ.get("TARGET_USERS")

if not TARGET_USERS:
    raise Exception("ERROR: TARGET_USERS environment variable is missing.")

# Convert "user1,user2,user3" → ["user1", "user2", "user3"]
TARGET_USERS = [u.strip() for u in TARGET_USERS.split(",")]

print(f"Users to process: {TARGET_USERS}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE
)

cur = conn.cursor()

def grant_permission(permission_obj, username):
    grant_type = permission_obj["grant_type"].upper()

    try:
        if grant_type == "ROLE":
            sql = f"GRANT ROLE {permission_obj['role_name']} TO USER {username}"

        elif grant_type in ["DATABASE", "SCHEMA", "TABLE", "VIEW", "WAREHOUSE"]:
            privileges = ", ".join(permission_obj["privileges"])
            sql = f"GRANT {privileges} ON {grant_type} {permission_obj['object_name']} TO USER {username}"

        else:
            print(f"Unsupported GRANT TYPE: {grant_type}")
            return

        print(f"Executing for {username}: {sql}")
        cur.execute(sql)
        print(f"✅ SUCCESS: Granted {grant_type} to {username}")

    except Exception as e:
        print(f"❌ ERROR granting {grant_type} to {username}: {str(e)}")


# Load permissions from JSON
with open("config.json", "r") as f:
    permissions = json.load(f)["permissions"]

# ✅ Loop through all users
for user in TARGET_USERS:
    print(f"\n=== Processing user: {user} ===")
    for perm in permissions:
        grant_permission(perm, user)

cur.close()
conn.close()

print("\n✅ All users processed successfully.")
