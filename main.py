from snowflake_conn import SnowflakeAccessManager
import streamlit as st


def main_page():
    st.set_page_config(
        layout="centered",
        page_title="Carbon V0.1",
        page_icon="©️"
    )
    st.title("Carbon V0.1")
    st.write("Create a carbon copy of your table in a new account")

    old_db_selected = False
    old_schemas_selected = False
    with st.expander("Existing Account Details"):
        if "e_acc" not in st.session_state.keys():
            old_form = st.form("old_acc")
            old_form.write("Enter your existing account details")
            acc = old_form.text_input(label="Account Identifier", placeholder="Eg. abc412.ap-south-1")
            user_name = old_form.text_input(label="Username", help="Your username used to login to Snowflake")
            password = old_form.text_input(label="Password", type="password")
            warehouse = old_form.text_input(label="Warehouse", placeholder="COMPUTE_WH")
            # op_db = old_form.text_input(label="Database", help="The database that you want to copy")
            role = old_form.text_input(label="Role", placeholder="SYSADMIN")

            if old_form.form_submit_button(label="Submit"):
                st.session_state["e_acc"] = {
                    "account": acc,
                    "user_name": user_name,
                    "password": password,
                    "warehouse": warehouse,
                    # "database": op_db,
                    "role": role
                }
    if "e_acc" in st.session_state.keys():
        st.write("**Existing account details added ✅**")
    with st.expander("New Account Details"):
        if "n_acc" not in st.session_state.keys():
            new_form = st.form("new_acc")
            new_form.write("Enter your existing account details")
            acc = new_form.text_input(label="Account Identifier", placeholder="Eg. abc412.ap-south-1")
            user_name = new_form.text_input(label="Username", help="Your username used to login to Snowflake")
            password = new_form.text_input(label="Password", type="password")
            warehouse = new_form.text_input(label="Warehouse", placeholder="COMPUTE_WH")
            # new_db = new_form.text_input(label="Database", help="The database that you want to copy")
            role = new_form.text_input(label="Role", placeholder="SYSADMIN")

            if new_form.form_submit_button(label="Submit"):
                st.session_state["n_acc"] = {
                    "account": acc,
                    "user_name": user_name,
                    "password": password,
                    "warehouse": warehouse,
                    # "database": new_db,
                    "role": role
                }
    if "n_acc" in st.session_state.keys():
        st.write("**New account details added ✅**")

    # st.write("Select database from old account")
    if "e_acc" in st.session_state.keys():
        selected_db_name = st.selectbox(label="Select database from old account", options=get_db_list(st.session_state.get("e_acc")))
    if "e_acc" in st.session_state.keys():
        schema_list = get_schema_list(st.session_state.get("e_acc"), selected_db_name)
        # removing information schema
        schema_list.remove("INFORMATION_SCHEMA")
        # st.write(schema_list)
        # ***************************************  Multi Select ********************************************************#
        container = st.container()
        all = st.checkbox("Select All")

        if all:
            selected_schemas = container.multiselect("Select the Schemas for which you want to copy the tables",
                                                     schema_list, schema_list)
        else:
            selected_schemas = container.multiselect(label="Select the Schemas for which you want to copy the tables",
                                                     options=schema_list,
                                                     disabled=False,
                                                     placeholder="Choose the schemas for copy")

    go_btn = st.button("Proceed to copy")
    if go_btn:
        if "e_acc" not in st.session_state.keys():
            st.error("Existing account details could not be found")
        elif "n_acc" not in st.session_state.keys():
            st.error("New account details could not be found")
        else:
            copy_tables(selected_db_name, selected_schemas, st.session_state.get("e_acc"),
                        st.session_state.get("n_acc"))


def get_db_list(conn_creds: dict) -> list:
    db_list = []
    with SnowflakeAccessManager(conn_params=conn_creds) as sf_client:
        get_db_res = sf_client.execute("show databases;").fetchall()
        for db_name_row in get_db_res:
            db_list.append(db_name_row[1])
        return db_list


def get_schema_list(conn_creds: dict, selected_db: str) -> list:
    schema_list = []
    with SnowflakeAccessManager(conn_params=conn_creds) as sf_client:
        schemas_sql_res = sf_client.execute(
            f"select schema_name from {selected_db}.INFORMATION_SCHEMA.SCHEMATA where catalog_name = '{selected_db}';")
        for res in schemas_sql_res:
            schema_list.append(res[0])

        return schema_list


def copy_tables(existing_db, schemas: list, old_creds: dict, new_creds: dict) -> None:
    """
    Perform a health check
        1. database exists in new acc
            if not create database
                execute create db sql in new acc
        2. schemas exists in new acc
            if not create schema
                for each schema
                    get ddl
                    execute in new acc

    Main process:
    For a schema
        get all tables
            for each table
                get ddl
                execute in new account
    :param schemas:
    :param old_creds:
    :param new_creds:
    :return:
    """

    generated_ddls = {}
    is_schema_verified = create_db_and_schema(existing_db, schemas, new_creds)
    if is_schema_verified:
        with st.spinner("Generating DDL's from your existing account!"):
            with SnowflakeAccessManager(old_creds) as exist_acc_sf_client:
                for schema in schemas:
                    res = []
                    tables = get_tables(exist_acc_sf_client, existing_db, schema)
                    for table in tables:
                        table_ddl = exist_acc_sf_client.execute(
                            f"select get_ddl('TABLE', '{existing_db}.{schema}.\"{table}\"', TRUE);").fetchall()[0][0]
                        res.append(table_ddl)
                    generated_ddls[schema] = res
        st.toast('Finished generating DDLs')
        with st.spinner("Creating objects in new account!"):
            with SnowflakeAccessManager(new_creds) as new_sf_client:
                for key in generated_ddls.keys():
                    ddl_list = generated_ddls[key]
                    for ddl in ddl_list:
                        res = new_sf_client.execute(ddl).fetchall()
                        print(res)
    st.success("Copied all Tables to new account!")


def get_tables(sf_client, db_name, schema_name):
    res = []
    get_tables_query = f"select table_name FROM {db_name}.INFORMATION_SCHEMA.TABLES where table_schema = '{schema_name}';"
    tables = sf_client.execute(get_tables_query).fetchall()
    for table in tables:
        res.append(table[0])

    return res


def create_db_and_schema(existing_db, schemas, account_creds) -> bool:
    """
    create new db and schemas in the new account if not exists
    :param existing_db:
    :param schemas:
    :param account_creds:
    :return:
    """
    with SnowflakeAccessManager(account_creds) as sf_client:
        # create database
        crete_db_res = sf_client.execute(f"create database if not exists {existing_db};").fetchall()
        for schema in schemas:
            create_schema_res = sf_client.execute(f"create schema if not exists {schema};").fetchone()
        return True


main_page()
