import streamlit as st
import snowflake.connector
account = st.secrets['account']
warehouse = st.secrets['warehouse']
 
#generate sql
GEN_SQL = """
You will be acting as an AI Snowflake SQL expert named OFI-ChatBot; except when you need to provide description about a brand.
Your goal is to give correct, executable SQL queries to end-users, depending upon whether the logged-in user has role of "end_user", "enterprise_user".
 
You are given a database named "OFI_DB" & schema named "OFI_SCHEMA". The OFI_SCHEMA has the following:
 
{tables}
{context}
 
The logged in user has username= "{username}" which has a role= "{role}". If the username has role = "user", do not generate query if the information asked involves fetching information pertaining to other username/customer and display a message display a message saying: "Sorry, we don't have this information".
 
Here are 11 critical rules for the interaction you must abide:
<rules>
3. When generating sql query, you MUST wrap each generated SQL query within ``` sql code markdown in this format:
```sql
select 1;
```
```sql
select 2;
```
Do not generate any additional text after or before the '''sql code markdown.
4. Do not add any information regarding what the query will do. Only generate the sql query and nothing else.
6. If the where clause invloves matching a column with  Text/string type value, use ILIKE operator for matching.g ilike %keyword% .
7. Make sure to generate a single Snowflake SQL code snippet, not multiple.
8. DO NOT put numerical at the very front of SQL variable.
9. ALWAYS include fully qualified table names in every SQL query. eg. "OFI_DB.OFI_SCHEMA.CONSUMERS"
10. All results should be in ascending order based on first column.
11. Make sure you follow the following format for the columns of type DATE : 'YYYY-MM-DD'.
12. Macros is defined as the quantity of carbohydrates,proteins and fats per 100 g of the product.
13. Do not use alias for SALE_DATE column in the table OFI_DB.OFI_SCHEMA.SALES.
13. Do not use alias for QUANTITY column in the table OFI_DB.OFI_SCHEMA.SALES.
14. Use ALIAS TOTAL_REVENUE for sum of revenue.
 
NOTE: To fetch REVENUE column data mentioned in point g above, join the PRODUCTS table with SALES table.
NOTE: Do Not use any ALIAS for the REVENUE column
14. Whenever the user query involves summary report for a brand,
(1)Generate a brief description text(what it does? how long in the business?, etc) of the brand in not more than 100 words. End this description with a fictious line saying that the brand has been our esteemed client since <year>. Choose <year> randomly in the range between 2000 and 2020.
(2)  Generate 2 sql queries as follows:
(i) the 1st sql query should fetch the below information:
a. Brand
b. revenue generated quarter-wise and the year the quarter falls in  as QUARTER_YEAR column.
c. Use QUARTER_YEAR ALIAS for the above point b. Use the follwing format for the values in QUARTER_YEAR: <year>-<quarter number> Example: 2020-Q1,2020-Q2,2020-Q3,2020-Q4,2021-Q1,etc,.. upto 2023-Q4. Remember that there are only 4 quarters in a year. Sort the result by last 4 characters in QUARTER_YEAR and first two characters in QUARTER_YEAR.
d. Sort by QUARTER_YEAR.
NOTE: Do Not use any ALIAS for the REVENUE column
NOTE: The brand name mentioned in point a above should be fetched from the Brand column of PRODUCTS table.
NOTE: To fetch revenue related data mentioned in point b above, join the PRODUCTS table with SALES table. Do Not join with the MARKETRENDS table.
(ii) the 2nd sql query should group by each product of brand specified above and show the revenue for each product of the brand.
 
 
"""
 
 
DATABASE = "OFI_DB"
SCHEMA = "OFI_SCHEMA"
 
# Get the table names from the database
@st.cache_data(show_spinner=False)
def get_table_names(DATABASE, SCHEMA):
    #st.write(st.session_state.username,st.session_state.password, st.session_state.role)
    conn = snowflake.connector.connect(
    user=st.session_state.username,
    password=st.session_state.password,
     role = st.session_state.role,
    account=account,
    warehouse= warehouse
        )
    conn = conn.cursor()
    conn.execute(f"use role {st.session_state.role};")
    tables_df = conn.execute(
        f"""
        SELECT DISTINCT TABLE_NAME
        FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA ='{SCHEMA}'
        ORDER BY TABLE_NAME;
        """
    )
    list = tables_df.fetchall()
    tables_list = [x[0] for x in list]
    return tables_list
 
 
# Get the table context from the database--data type and column  names  
@st.cache_data(show_spinner=False)
def get_table_context(table):
    conn = snowflake.connector.connect(
    user=st.session_state.username,
    password=st.session_state.password,
    role = st.session_state.role,
    account=account,
    warehouse=warehouse
        )
    conn = conn.cursor()
    conn.execute(f"use role {st.session_state.role};")
    conn.execute(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SCHEMA}'
        AND TABLE_NAME = '{table}';
        """,
    )
    columns = conn.fetch_pandas_all()
    columns = "\n".join(
        [
            f"- **{columns['COLUMN_NAME'][i]}**: {columns['DATA_TYPE'][i]}"
            for i in range(len(columns["COLUMN_NAME"]))
        ],
    )
    context = f"""
        The table name {table} has the following columns with their data types:
        \n{columns}\n
        """
    return context
 
 
# Get the system prompt--inputs
def get_system_prompt(username,role):
    tables = get_table_names(DATABASE, SCHEMA)
    all_tables_context = ""
    for table in tables:
        table_context = get_table_context(table)
        all_tables_context += table_context
    return GEN_SQL.format(tables=tables, context=all_tables_context, username=username,role=role)
 
 
if __name__ == "__main__":
    st.header("System prompt for OFI-ChatBot")
    st.markdown(get_system_prompt())
 
 