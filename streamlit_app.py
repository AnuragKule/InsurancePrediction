"""Streamlit application: Peggy‑Buddy (Gemini‑only edition)
Updated 15 May 2025 – system‑role removed for Gemini compliance

Key points
──────────
* **Gemini SDK only** – no OpenAI code or secrets.
* **No `system` messages** – initial instruction now sent as a **user** role (Gemini supports only `user` and `model`).
* Streaming replies when SDK supports it.
* Snowflake query handling, login flow and Plotly visual intact.

Secrets required in `.streamlit/secrets.toml`
────────────────────────────────────────────
```toml
GEMINI_API_KEY = "YOUR_GEMINI_KEY"
account        = "<snowflake-account>"
warehouse      = "COMPUTE_WH"
```
"""

#####################
# Imports & config  #
#####################
import os, re, csv
import streamlit as st
import plotly.express as px
import google.generativeai as genai
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from prompts_ofi import get_system_prompt

#####################
# Gemini API config #
#####################
GEMINI_KEY = st.secrets.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
if not GEMINI_KEY:
    st.stop()

genai.configure(api_key=GEMINI_KEY)

#####################
# Constants         #
#####################
DATABASE  = "OFI_DB"
SCHEMA    = "OFI_SCHEMA"
account   = st.secrets["account"]
warehouse = st.secrets["warehouse"]

#####################
# Page setup        #
#####################
st.set_page_config(layout="wide")
st.markdown("""<style>.big-font {font-size:20px !important;} </style>""", unsafe_allow_html=True)

col1, _, col3 = st.columns(3)
with col3:
    st.markdown(
        '<a href="https://hoonartek.com/"><img src="https://hoonartek.com/wp-content/uploads/2022/01/Hoonartek-logo-light.svg" width="300"></a>',
        unsafe_allow_html=True,
    )

#####################
# Helper functions  #
#####################

def isAuthenticated(database: str, schema: str, username: str, role: str, password: str) -> bool:
    try:
        conn = snowflake.connector.connect(
            user=username, password=password, role=role, account=account, warehouse="COMPUTE_WH"
        )
        conn.close()
        st.success("Connection to Snowflake established successfully.")
        return True
    except (ProgrammingError, DatabaseError) as e:
        st.error(f"Snowflake error: {e.msg}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
    return False


def display_login_form():
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")

    roles = []
    try:
        with open("roles.csv", newline="") as f:
            roles = [row["role"] for row in csv.DictReader(f)]
    except Exception as e:
        st.sidebar.error(f"Error loading roles.csv: {e}")

    role = st.sidebar.selectbox("Role", roles)

    if st.sidebar.button("Login"):
        if isAuthenticated(DATABASE, SCHEMA, username, role, password):
            st.session_state.update({"username": username, "password": password, "role": role})
            st.sidebar.success(f"Logged in as {username}")
        else:
            st.sidebar.error("Incorrect credentials.")
            for k in ("username", "password", "role"):
                st.session_state.pop(k, None)


def plot_graph(results, message):
    if not results.empty and {"year", "revenue"}.issubset(results.columns):
        fig = px.bar(results, x="year", y="revenue", title="Revenue by Year")
        st.plotly_chart(fig, use_container_width=True)
        message["fig"] = fig
    return message

#####################
# Chat logic        #
#####################

def chatbot():
    st.title("👨‍💼 Peggy‑Buddy – Gemini edition")

    # Initialise conversation state without system role
    if "messages" not in st.session_state:
        if {"username", "role"}.issubset(st.session_state):
            st.session_state.messages = [{
                "role": "user",
                "content": get_system_prompt(st.session_state.username, st.session_state.role),
            }]
        else:
            st.error("You must log in first.")
            return

    # Greet once
    if not st.session_state.get("assistant_greeted"):
        with st.chat_message("assistant"):
            st.write(
                f"Hello {st.session_state.username}, I am Peggy‑Buddy, your personal assistant for Peggy services. "
                "How may I help you today?"
            )
        st.session_state.assistant_greeted = True

    # Render history
    for msg in st.session_state.messages:
        with st.chat_message("assistant" if msg["role"] == "assistant" else "user"):
            if msg.get("sql"):
                if msg["content"]:
                    with st.expander("Show SQL Query"):
                        st.markdown(msg["content"])
                if msg.get("results") is not None and not msg["results"].empty:
                    st.dataframe(msg["results"])
                if fig := msg.get("fig"):
                    st.write(fig)
            else:
                st.write(msg["content"])

    #########################
    # User prompt & Gemini  #
    #########################
    prompt = st.chat_input("Say something")
    if not prompt:
        return

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.spinner("Thinking..."):
        history_for_gemini = [
            {"role": ("model" if m["role"] == "assistant" else "user"), "parts": [m["content"]]}
            for m in st.session_state.messages[:-1]
        ]

        model = genai.GenerativeModel("gemini-1.5-flash")
        chat_session = model.start_chat(history=history_for_gemini)

        try:
            stream = chat_session.send_message(prompt, stream=True)
            reply = "".join(part.text for part in stream)
        except AttributeError:
            reply = chat_session.send_message(prompt).text
        except Exception as e:
            st.error(f"Gemini API error: {e}")
            reply = "Sorry, I hit an error processing your request."

    assistant_msg = {"role": "assistant", "content": reply}

    ################################################
    # SQL detection & execution                    #
    ################################################
    sql_blocks = re.findall(r"```sql\s+(.*?)\s+```", reply, flags=re.DOTALL)
    sql_blocks = [q.strip() for q in sql_blocks]

    if sql_blocks:
        assistant_msg["sql"] = sql_blocks[0]
        summary_model = genai.GenerativeModel("gemini-1.5-flash")

        for idx, sql in enumerate(sql_blocks, 1):
            try:
                with snowflake.connector.connect(
                    user=st.session_state.username,
                    password=st.session_state.password,
                    role=st.session_state.role,
                    account=account,
                    warehouse=warehouse,
                ) as conn:
                    df = conn.cursor().execute(sql).fetch_pandas_all()

                if df.empty:
                    st.warning("The requested data is not available in the database.")
                    continue

                prompt_desc = (
                    "Provide a concise, reader‑friendly description of the following table. "
                    "For revenue figures, express the unit as 'million dollars'.\n\n" + df.head(20).to_markdown()
                )
                description = summary_model.generate_content(prompt_desc).text.strip()

                st.header("Insights" if len(sql_blocks) == 1 else f"Insights #{idx}", divider="rainbow")
                with st.expander("Show SQL Query"):
                    st.code(sql, language="sql")
                st.markdown(f"<div class='big-font'>{description}</div>", unsafe_allow_html=True)

                assistant_msg["results"] = df
                assistant_msg = plot_graph(df, assistant_msg)

            except (DatabaseError, ProgrammingError) as sn_ex:
                st.error(f"Snowflake error: {sn_ex.msg}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")
    else:
        st.write(reply)

    st.session_state.messages.append(assistant_msg)

#####################
# Main entry point  #
#####################
if __name__ == "__main__":
    if not st.session_state.get("username"):
        display_login_form()
    else:
        chatbot()
