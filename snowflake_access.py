import json
import os
import snowflake.connector

# Read Snowflake connection details from GitHub Secrets / environment variables
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]

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


def grant_permission(permission_obj, target_user):
    """
    Grants Snowflake permission based on config.json entry
    """

    grant_type = permission_obj["grant_type"].upper()

    try:
        if grant_type == "ROLE":
            role_name = permission_obj["role_name"]
            sql = f"GRANT ROLE {role_name} TO USER {target_user}"

        elif grant_type == "DATABASE":
            object_name = permission_obj["object_name"]
            privileges = permission_obj["privileges"]
            privilege_sql = ", ".join(privileges)
            sql = f"GRANT {privilege_sql} ON DATABASE {object_name} TO USER {target_user}"

        elif grant_type == "SCHEMA":
            object_name = permission_obj["object_name"]
            privileges = permission_obj["privileges"]
            privilege_sql = ", ".join(privileges)
            sql = f"GRANT {privilege_sql} ON SCHEMA {object_name} TO USER {target_user}"

        elif grant_type == "TABLE":
            object_name = permission_obj["object_name"]
            privileges = permission_obj["privileges"]
            privilege_sql = ", ".join(privileges)
            sql = f"GRANT {privilege_sql} ON TABLE {object_name} TO USER {target_user}"

        elif grant_type == "VIEW":
            object_name = permission_obj["object_name"]
            privileges = permission_obj["privileges"]
            privilege_sql = ", ".join(privileges)
            sql = f"GRANT {privilege_sql} ON VIEW {object_name} TO USER {target_user}"

        elif grant_type == "WAREHOUSE":
            object_name = permission_obj["object_name"]
            privileges = permission_obj["privileges"]
            privilege_sql = ", ".join(privileges)
            sql = f"GRANT {privilege_sql} ON WAREHOUSE {object_name} TO USER {target_user}"

        else:
            print(f"Unsupported grant type: {grant_type}")
            return

        print(f"Executing SQL: {sql}")
        cur.execute(sql)
        print(f"SUCCESS: Granted {grant_type} permission to {target_user}")

    except Exception as e:
        print(f"ERROR granting {grant_type} to {target_user}: {str(e)}")


# Read config.json
with open("config.json", "r") as f:
    data = json.load(f)

target_user = data["user"]

# Loop through permissions
for perm in data["permissions"]:
    grant_permission(perm, target_user)

# Close connection
cur.close()
conn.close()
