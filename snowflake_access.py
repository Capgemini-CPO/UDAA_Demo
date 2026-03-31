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

# Multi-user list from GitHub Actions
TARGET_USERS = os.environ.get("TARGET_USERS")  

if not TARGET_USERS:
    raise Exception("ERROR: TARGET_USERS environment variable is missing.")

# Convert comma-separated list to Python list
TARGET_USERS = [u.strip() for u in TARGET_USERS.split(",")]

print(f"\n Users to process: {TARGET_USERS}\n")

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

def grant_permission(permission, user):
    """Grant permission to one user."""
    grant_type = permission["grant_type"].upper()

    try:
        if grant_type == "ROLE":
            sql = f"GRANT ROLE {permission['role_name']} TO USER {user}"

        elif grant_type in ["DATABASE", "SCHEMA", "TABLE", "VIEW", "WAREHOUSE"]:
            privs = ", ".join(permission["privileges"])
            sql = f"GRANT {privs} ON {grant_type} {permission['object_name']} TO USER {user}"

        else:
            print(f" Unsupported grant type: {grant_type}")
            return

        print(f"Running: {sql}")
        cur.execute(sql)
        print(f" Granted {grant_type} to {user}")

    except Exception as e:
        print(f" ERROR granting {grant_type} to {user}: {str(e)}")


# Load permissions
with open("config.json") as f:
    permissions = json.load(f)["permissions"]

# Loop through all users and apply all permissions
for user in TARGET_USERS:
    print(f"\n============ Processing {user} ============\n")
    for perm in permissions:
        grant_permission(perm, user)

cur.close()
conn.close()

print("\n ALL USERS PROCESSED SUCCESSFULLY \n")
