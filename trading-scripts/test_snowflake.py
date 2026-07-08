from utils.snowflake_connection import snowflake_connection

conn = snowflake_connection(role="loader_role")
cs = conn.cursor()

cs.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
result = cs.fetchone()

print("Connected successfully.")
print("Role:", result[0])
print("Warehouse:", result[1])
print("Database:", result[2])

cs.close()
conn.close()
