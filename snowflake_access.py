import json
import os
import snowflake.connector

# --------------------------------------------------
# READ ENV VARIABLES
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


# --------------------------------------------------
# LOAD CONFIG.JSON
# --------------------------------------------------
with open("config.json", "r") as f:
    data = json.load(f)

role = data["role"]                     # REQUIRED
permissions = data["permissions"]       # REQUIRED
users = data.get("users", [])            # OPTIONAL


# --------------------------------------------------
# USE ADMIN ROLE
# --------------------------------------------------
execute("USE ROLE SECURITYADMIN")


# --------------------------------------------------
# ENSURE ROLE EXISTS
# --------------------------------------------------
execute(f"CREATE ROLE IF NOT EXISTS {role}")


# --------------------------------------------------
# ASSIGN ROLE TO USERS (OPTIONAL)
# --------------------------------------------------
for user in users:
    execute(f'GRANT ROLE {role} TO USER "{user}"')


# --------------------------------------------------
# GRANT PERMISSIONS TO ROLE
# --------------------------------------------------
for perm in permissions:
    object_type = perm["object_type"].upper()
    object_name = perm["object_name"]
    privileges = perm["privileges"]

    for privilege in privileges:
        sql = f"""
        GRANT {privilege}
        ON {object_type} {object_name}
        TO ROLE {role}
        """
        execute(sql.strip())


# --------------------------------------------------
# CLEANUP
# --------------------------------------------------
cur.close()
conn.close()

print("Snowflake access provisioning completed successfully.")
