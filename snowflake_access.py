import json
import os
import snowflake.connector

# --------------------------------------------------
# CONNECT TO SNOWFLAKE
# --------------------------------------------------
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    role=os.environ.get("SNOWFLAKE_ROLE", "SECURITYADMIN"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE")
)

cur = conn.cursor()


def execute(sql):
    print(f"Executing SQL: {sql}")
    cur.execute(sql)


def exists(sql):
    cur.execute(sql)
    return cur.fetchone() is not None


# --------------------------------------------------
# LOAD CONFIG
# --------------------------------------------------
with open("config.json") as f:
    data = json.load(f)

role = data["role"]
permissions = data["permissions"]
users = data.get("users", [])


# --------------------------------------------------
# SET ADMIN ROLE
# --------------------------------------------------
execute("USE ROLE SECURITYADMIN")


# --------------------------------------------------
# CREATE ROLE IF MISSING
# --------------------------------------------------
execute(f"CREATE ROLE IF NOT EXISTS {role}")


# --------------------------------------------------
# ASSIGN ROLE TO USERS
# --------------------------------------------------
for user in users:
    execute(f'GRANT ROLE {role} TO USER "{user}"')


# --------------------------------------------------
# APPLY PERMISSIONS SAFELY
# --------------------------------------------------
for perm in permissions:
    obj_type = perm["object_type"].upper()
    obj_name = perm["object_name"]
    privileges = perm["privileges"]
    parts = obj_name.split(".")

    skip = False

    if obj_type == "DATABASE":
        if not exists(f"SHOW DATABASES LIKE '{parts[0]}'"):
            print(f" Skipping: Database does not exist -> {parts[0]}")
            skip = True

    elif obj_type == "SCHEMA":
        if not exists(f"SHOW SCHEMAS LIKE '{parts[1]}' IN DATABASE {parts[0]}"):
            print(f" Skipping: Schema does not exist -> {obj_name}")
            skip = True

    elif obj_type == "TABLE":
        if not exists(
            f"SHOW TABLES LIKE '{parts[2]}' IN SCHEMA {parts[0]}.{parts[1]}"
        ):
            print(f" Skipping: Table does not exist -> {obj_name}")
            skip = True

    if skip:
        continue

    for privilege in privileges:
        execute(f"GRANT {privilege} ON {obj_type} {obj_name} TO ROLE {role}")

# --------------------------------------------------
# CLEANUP
# --------------------------------------------------
cur.close()
conn.close()

print(" Snowflake access provisioning completed successfully.")
