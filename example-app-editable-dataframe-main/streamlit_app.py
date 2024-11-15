import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from snowflake.snowpark import Session
import toml

# Snowflake database, schema, and table name
DATABASE = "OMNI_DATA"
SCHEMA = "PUBLIC"
TABLE_NAME = "SALES_REVENUE"

@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

def create_snowflake_session():
    """Initialize and return the Snowflake session."""
    try:
        snowflake_creds = st.secrets["snowflake"]

        if not snowflake_creds:
            st.error("Snowflake credentials not found in secrets.toml.")
            return None

        session_params = {
            "user": snowflake_creds["user"],
            "password": snowflake_creds["password"],
            "account": snowflake_creds["account"],
            "warehouse": snowflake_creds["warehouse"],
            "database": snowflake_creds["database"],
            "schema": snowflake_creds["schema"]
        }
        session = Session.builder.configs(session_params).create()

        if session is None:
            st.error("Failed to create Snowflake session.")
            return None

        st.success("Snowflake session created successfully.")
        return session

    except Exception as e:
        st.error(f"Error creating Snowflake session: {e}")
        return None

def fetch_and_display_data(query: str) -> pd.DataFrame:
    """Fetch data from Snowflake and return it as a DataFrame."""
    try:
        session = create_snowflake_session()
        if session is None:
            return pd.DataFrame()

        df = session.sql(query).to_pandas()
        return df

    except Exception as e:
        st.error(f"Error fetching data from Snowflake: {e}")
        return pd.DataFrame()

def upsert_data(session, df_sel_row, table_name):
    """Perform an upsert operation on the Snowflake table with the selected data."""
    if not df_sel_row.empty:
        try:
            staging_table = f"{table_name}_STAGING"
            # Create staging table with the same schema as the target table
            session.sql(f"CREATE OR REPLACE TEMP TABLE {staging_table} AS SELECT * FROM {table_name} LIMIT 0").collect()

            # Print selected DataFrame to ensure data is ready for insertion
            st.write("Selected Rows DataFrame:", df_sel_row)

            # Write the selected data to the staging table
            snowpark_df = session.write_pandas(df_sel_row, staging_table, auto_create_table=False, overwrite=True)

            # Prepare and execute the MERGE statement
            merge_query = f"""
            MERGE INTO {table_name} AS target
            USING {staging_table} AS source
            ON target.ORGANIZATIONID = source.ORGANIZATIONID AND target.LEVEL1FORCEID = source.LEVEL1FORCEID
            WHEN MATCHED THEN 
                UPDATE SET 
                    target.SALES = source.SALES,
                    target.REVENUE = source.REVENUE
            WHEN NOT MATCHED THEN 
                INSERT (ORGANIZATIONID, LEVEL1FORCEID, LEVEL1ACCOUNTNAME, LEVEL1DISPLAYACCOUNTNAME, LEVEL1ACCOUNTTYPE,
                        LEVEL1FEDERALTAXID, LEVEL1ADDRESS, LEVEL1ADDRESS2, LEVEL1CITY, LEVEL1STATE, LEVEL1COUNTY,
                        LEVEL1POSTALCODE, LEVEL1COUNTRY, LEVEL1SEGMENTATION, LEVEL1SECTOR, LEVEL1METROAREANAME,
                        LEVEL1METROAREATOTALPOPULATION, SECTORKEY, SECTORID, ACCOUNTORGANIZATIONID, FORCEID, SUPPLIERNAME,
                        CONTRACTID, ACCOUNTSTATUS, RECEIPTDATE, SALES, REVENUE, BUDGETSALESAMOUNT, BUDGETREVENUEAMOUNT,
                        SALESTERRITORYKEY, TERRITORYNAME, TERRITORYMANAGER, TERRITORYSUPERVISOR, ACCOUNTOWNER,
                        SOURCEENTITYNAME, ORGANIZATIONNAME, CONTRACTNUMBER, CONTRACTNAME)
                VALUES (source.ORGANIZATIONID, source.LEVEL1FORCEID, source.LEVEL1ACCOUNTNAME, source.LEVEL1DISPLAYACCOUNTNAME,
                        source.LEVEL1ACCOUNTTYPE, source.LEVEL1FEDERALTAXID, source.LEVEL1ADDRESS, source.LEVEL1ADDRESS2,
                        source.LEVEL1CITY, source.LEVEL1STATE, source.LEVEL1COUNTY, source.LEVEL1POSTALCODE, source.LEVEL1COUNTRY,
                        source.LEVEL1SEGMENTATION, source.LEVEL1SECTOR, source.LEVEL1METROAREANAME, source.LEVEL1METROAREATOTALPOPULATION,
                        source.SECTORKEY, source.SECTORID, source.ACCOUNTORGANIZATIONID, source.FORCEID, source.SUPPLIERNAME,
                        source.CONTRACTID, source.ACCOUNTSTATUS, source.RECEIPTDATE, source.SALES, source.REVENUE,
                        source.BUDGETSALESAMOUNT, source.BUDGETREVENUEAMOUNT, source.SALESTERRITORYKEY, source.TERRITORYNAME,
                        source.TERRITORYMANAGER, source.TERRITORYSUPERVISOR, source.ACCOUNTOWNER, source.SOURCEENTITYNAME,
                        source.ORGANIZATIONNAME, source.CONTRACTNUMBER, source.CONTRACTNAME)
            """
            session.sql(merge_query).collect()
            st.write("Merge Query Executed:", merge_query)

            st.success(f"✔️ Data upserted to `{table_name}` table.")
        except Exception as e:
            st.error(f"Error executing upsert: {e}")
    else:
        st.info("No data to upload.")

def upload_to_snowflake(df: pd.DataFrame, table_name: str):
    """Uploads the edited dataframe to Snowflake using an upsert operation."""
    try:
        session = create_snowflake_session()
        if session is None:
            return

        upsert_data(session, df, table_name)

    except Exception as e:
        st.error(f"Error uploading data to Snowflake: {e}")

def insert_new_row(session, table_name, new_row):
    """Insert a new row into the Snowflake table."""
    try:
        # Insert new row into the table
        columns = ", ".join(new_row.keys())
        values = ", ".join([f"'{value}'" for value in new_row.values()])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        session.sql(insert_query).collect()
        st.success("New row inserted successfully!")
    except Exception as e:
        st.error(f"Error inserting new row: {e}")

st.set_page_config(page_title="Snowflake Data Grid", page_icon="💾")
st.title("Editable Dataframe with Snowflake Integration")

query = f"SELECT * FROM {TABLE_NAME} LIMIT 10"
df = fetch_and_display_data(query)

if df.empty:
    st.info("No data to display.")
else:
    st.subheader("① Edit and select cells")

    gd = GridOptionsBuilder.from_dataframe(df)
    gd.configure_pagination(enabled=True)
    gd.configure_default_column(editable=True, groupable=True)  
    gd.configure_selection(selection_mode="multiple", use_checkbox=True)  
    gridoptions = gd.build()

    grid_table = AgGrid(
        df,
        gridOptions=gridoptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="material",
        allow_unsafe_jscode=True 
    )

    selected_rows = grid_table["selected_rows"]
    df_sel_row = pd.DataFrame(selected_rows)
    csv = convert_df(df_sel_row)

    if not df_sel_row.empty:
        st.write(df_sel_row)

# Collapsible section for "Insert New Row"
st.subheader("② Insert New Row")
with st.expander("Insert New Row", expanded=False):  # Collapsible section
    with st.form("new_row_form"):
        new_row = {}
        for col in df.columns:
            new_row[col] = st.text_input(f"Enter {col}")

        submitted = st.form_submit_button("Insert Row")
        if submitted:
            session = create_snowflake_session()
            if session:
                insert_new_row(session, TABLE_NAME, new_row)

# Upload selected data to Snowflake
st.subheader("③ Upload selected data to Snowflake ❄️")
if st.button("Upload to Snowflake"):
    if not df_sel_row.empty:  
        upload_to_snowflake(df_sel_row, TABLE_NAME)
    else:
        st.warning("Please select rows to upload.")