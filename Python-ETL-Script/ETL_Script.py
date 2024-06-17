import http.client
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

try:
    # EXTRACTING DATA FROM RAPID API. {LINKEDIN JOB DETAILS DATA}.
    def Extract_Data():
        conn = http.client.HTTPSConnection("linkedin-data-api.p.rapidapi.com")
        headers = {
            'x-rapidapi-key': "a6b1e2ceeamsh13d3f5ed213ace3p107805jsn1663e38b1480",
            'x-rapidapi-host': "linkedin-data-api.p.rapidapi.com"
        }

        conn.request("GET", "/search-jobs?keywords=golang&locationId=92000000&datePosted=anyTime&sort=mostRelevant", headers=headers)
        res = conn.getresponse()
        
        if res.status != 200:
            raise Exception(f"API request failed with status code {res.status}")
        
        data = res.read()
        jobs = json.loads(data.decode('utf-8'))  # Ensure proper decoding and JSON loading

        # Creating lists to store extracted data
        job_ids = []
        titles = []
        locations = []
        poster_ids = []
        reference_ids = []
        types = []
        post_dates = []

        # Extract attribute from jobs if it's defined and not empty
        for job in jobs.get("jobs", []):  # Assuming the response structure has a 'jobs' key
            job_ids.append(job.get("id"))
            titles.append(job.get("title"))
            locations.append(job.get("location"))
            poster_ids.append(job.get("posterId"))
            reference_ids.append(job.get("referenceId"))
            types.append(job.get("type"))
            post_dates.append(job.get("postDate"))

        # Create DataFrame
        df = pd.DataFrame({
            'JOB_ID': job_ids,
            'JOB_TITLE': titles,
            'Location': locations,
            'Poster_ID': poster_ids,
            'WORK_TYPE': types,
            'Post_Date': post_dates
        })

        return df

    # TRANSFORMING DATA.
    def Transform_Data():
        df = Extract_Data()

        # Renaming column names
        df = df.rename(columns={'Location': 'WORK_LOCATION', 'Poster_ID': 'POST_ID', 'Post_Date': 'JOB_POST_PUBLISH'})

        # Handling work mode based on work location
        df['WORK_MODE'] = df['WORK_LOCATION'].astype(str).str.extract('(On-site|Hybrid)', expand=False)
        df['WORK_LOCATION'] = df['WORK_LOCATION'].astype(str).str.replace('(On-site|Hybrid)', '', regex=True)

        # Splitting work location into City, State, Country
        split_data = df['WORK_LOCATION'].str.split(',', expand=True)
        
        if len(split_data.columns) == 3:
            df[['CITY', 'STATE', 'COUNTRY']] = split_data
        else:
            df['CITY'] = ""
            df['STATE'] = ""
            df['COUNTRY'] = ""

        # Selecting specific columns
        df = df[['JOB_ID', 'JOB_TITLE', 'CITY', 'STATE', 'COUNTRY', 'POST_ID', 'WORK_TYPE', 'JOB_POST_PUBLISH','WORK_MODE']]

        # Replacing specific job titles
        df['JOB_TITLE'] = df['JOB_TITLE'].replace({'Senior Calculator': 'Senior Consultant','Business Developer Energie': 'Business Developer Engineer','Projectontwikkelaar': 'Project Manager',
            'Calculator bouw en vastgoed': 'Consultant','Calculator': 'Consultant','3D Inventor Tekenaar Engineer (32-40 uur)': '3D Inventor Technical Engineer',
            'Boekhouder/ dossierbeheerder': 'Consultant','Verkoop binnendienst': 'Consultant','Projektleiter:in Elektrotechnik': 'Project Engineer',
            'Einrichtungsleiter (m/w/d)': 'System Engineer','Medewerker Bijzonder Beheer': 'System Engineer','Commercieel Binnendienst Medewerker': 'Consultant',
            'Implementatieconsultant': 'Implementation Consultant','Relatiebeheerder - Hypothecaire kredieten': 'Consultant','Stabiliteitsingenieur': 'Software Engineer',
            'Montør til Driftafdelingen': 'Consultant','Medior medewerker Customer support': 'Customer Support','Pizzachef': 'Consultant','Centrummanager': 'Chef Manager','Revit modelleur': 'System Engineer'
        })

        # Stripping leading/trailing whitespace from columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        return df

    # LOAD DATA INTO SNOWFLAKE DATA WAREHOUSE.
    def Load_Data():
        df = Transform_Data()
        final_df = pd.DataFrame(df)

        # Snowflake credentials
        con_parameter = {
            'user': 'VIJAR12',
            'password': 'Vija@r12',
            'account': 'ynrrnvl-tq54492',
            'warehouse': 'COMPUTE_WH',
            'database': 'RAPIDAPI_PIPELINE_PROJECT',
            'schema': 'TRANSFORMED_DATA'
        }

        try:
            # Establishing connection to Snowflake
            conn = snowflake.connector.connect(**con_parameter)
            cur = conn.cursor()

            # Creating or replacing table in Snowflake
            tablename = 'RAPIDAPI_TRANSFORMED_DATA'
            sql_query = f'''
                CREATE OR REPLACE TABLE {tablename} (
                    JOB_ID BIGINT,
                    JOB_TITLE VARCHAR,
                    CITY VARCHAR,
                    STATE VARCHAR,
                    COUNTRY VARCHAR,
                    POST_ID BIGINT,
                    WORK_TYPE VARCHAR,
                    JOB_POST_PUBLISH VARCHAR,
                    WORK_MODE VARCHAR
                )
            '''
            cur.execute(sql_query)
            
            # Writing DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(conn, final_df, table_name=tablename)
            if success:
                print('ETL Task Completed Successfully!!!!')
            else:
                print(f"ETL Task Failed: Loaded {nrows} rows in {nchunks} chunks")

        except Exception as e:
            print(f"Error loading data to Snowflake: {e}")

        finally:
            cur.close()
            conn.close()

    # Execute the ETL process
    Load_Data()
except Exception as error:
    print('Oops there is an error occured in etl process')
else:
    print('ETL Task Completed Sccuessfully!!')