import pandas as pd
from pyhive import hive
import subprocess
import os
from pyhive import hive

import subprocess
import os


def copy_file_between_containers(source_container, source_path, target_container, target_path, temp_file):
    try:
        subprocess.run(
            ["docker", "cp", f"{source_container}:{source_path}", temp_file],
            check=True,
        )
        subprocess.run(
            ["docker", "cp", temp_file, f"{target_container}:{target_path}"],
            check=True,
        )
        print("File successfully copied between containers.")
    except subprocess.CalledProcessError as e:
        print(f"Error during subprocess execution: {e}")
    except FileNotFoundError as e:
        print(f"FileNotFoundError: {e}")
    finally:
        if os.path.exists(temp_file):
            subprocess.run(["rm", "-f", temp_file])
        else:
            print(f"Temporary file {temp_file} not found for deletion.")


def Load_data_to_csv(final_df, date, AllExtractedData):
    print("-----Load---")
    temp_file = f"/tmp/transformed_data_{date}.csv"
    source_container = "loanrequestvisualization-airflow-webserver-1"
    source_path = f"/tmp/transformed_data_{date}.csv"
    target_container = "hive"
    target_path = f"/opt/transformed_data_{date}.csv"
    print("----------------------------------------before---------------------")
    copy_file_between_containers(source_container, source_path, target_container, target_path,temp_file)
    print("-----------------------------done-----------------------")

    # Save the final dataframe to the transformed path
    #final_df.to_csv(target_path, sep=';', index=False)


    # Connect to Hive and load the transformed data
    conn = hive.Connection(host="hive-server", port=10000, database='m2t')
    cursor = conn.cursor()

    # Truncate the target table in Hive
    cursor.execute("TRUNCATE TABLE M2T.OUTPUT_TABLE_ETL")

    # Load data from the transformed path into the table
    load_data_query = f"LOAD DATA LOCAL INPATH '{target_path}' INTO TABLE M2T.OUTPUT_TABLE_ETL"
    cursor.execute(load_data_query)
    cursor.close()
    conn.close()

def Transform_data(df):
    print("here I am in the transoform process")
    print(df.columns)
    def calculate_date_remboursement(row):
        if (isinstance(row['method'], str) and row['method'].startswith('MANUAL') and
            row['date_demande_credit'] is not None):
            return row['date_demande_credit']
        else:
            return pd.NaT  # or an alternative value


    def calculate_date_reglement(row):
        if (isinstance(row['method'], str) and row['method'].startswith('MANUAL') and row['deadline'] < row['payment_date'] and row['status'] in ['PAID_LS_RECONCILED', 'PAID_LS', 'PAID']):
            return row['payment_date']
        return None

    def calculate_date_prelevement(row):
        if row['method'] and isinstance(row['method'], str) and row['method'].startswith('BATCH'):
        # Your existing logic if 'method' starts with 'BATCH'
            pass
        else:
            return None

    def handle_rejection_reason(reason):
        if pd.isna(reason):  # Check for NaN
            return 'No Reason'
        return reason.strip() if reason.strip() else 'No Reason'


    if 'rejection_reason' in df.columns:
        df['MOTIF_DE_REJET'] = df['rejection_reason'].apply(handle_rejection_reason)
    else:
        print("Column 'rejection_reason' not found")

    if 'nombre_echeance' in df.columns:
        df['nombre_echeance'] = df['nombre_echeance'].fillna('0').astype(str)
    else:
        print("Column 'nombre_echeance' not found")

    # Apply the conditions to create new columns with consistent single-value returns
    df['DATE_REMBOURSEMENT_DERNIERE_ECHEANCE'] = df.apply(calculate_date_remboursement, axis=1)
    df['DATE_REGLEMENT_IMPAYE_DERNIERE_ECHEANCE'] = df.apply(calculate_date_reglement, axis=1)
    df['DATE_PRELEVEMENT_DERNIERE_ECHEANCE'] = df.apply(calculate_date_prelevement, axis=1)

    # Drop and rename columns
    df = df.drop(columns=['id3', 'id4', 'rejection_reason'])  
    df = df.rename(columns={"id1": "client_id", "id2": "loan_request_id", "requested_deadlines": "NOMBRE_ECHEANCE"})

    return df


def extract_data_from_hive(required_date):
    conn = hive.Connection(host="hive-server", port=10000, database='m2t')

    # Query 1
    query1 = f"""
        SELECT
            cl.id as id1,
            lr.id as id2,
            cl.phone_number AS N_GSM,
            cl.first_name  AS NOM,
            cl.last_name AS PRENOM,
            cl.age as age,
            cl.city_label as city,
            cl.gender as gender,
            lr.code_ls AS CODE_DOSSIER,
            lr.requested_amount AS MONTANT_DEMANDE,
            lr.awarded_amount AS MONTANT_ACCORDE,
            lr.created_date AS DATE_DEMANDE_CREDIT,
            SPLIT(lr.created_date, ' ')[1] AS HEURE_DEMANDE_CREDIT,
            lr.status AS STATUT_DOSSIER,
            lr.rejection_reason as rejection_reason,
            lr.requested_deadlines as NOMBRE_ECHEANCE,
            deadlines.deadline1 AS DATE_ECHEANCE1,
            deadlines.deadline2 AS DATE_ECHEANCE2,
            deadlines.deadline3 AS DATE_ECHEANCE3,
            deadlines.deadline4 AS DATE_ECHEANCE4,
            lr.fees AS FREE_DE_SERVICE,
            lr.rate AS TAUX_D_INTERET,
            CASE
                WHEN lr.code_es = '123456' THEN '012006'
                ELSE lr.code_es
            END AS CODE_AGENCE
        FROM clients cl
        LEFT JOIN loan_request lr ON cl.id = lr.fk_client
        LEFT JOIN (
            SELECT 
                fk_loan_request AS loan_request_id, 
                MAX(CASE WHEN deadline_row = 1 THEN deadline END) AS deadline1,
                MAX(CASE WHEN deadline_row = 2 THEN deadline END) AS deadline2,
                MAX(CASE WHEN deadline_row = 3 THEN deadline END) AS deadline3,
                MAX(CASE WHEN deadline_row = 4 THEN deadline END) AS deadline4
            FROM (
                SELECT 
                    fk_loan_request, 
                    deadline, 
                    ROW_NUMBER() OVER (PARTITION BY fk_loan_request ORDER BY deadline) AS deadline_row
                FROM payment_deadline
            ) deadline_subquery
            GROUP BY fk_loan_request
        ) AS deadlines ON lr.id = deadlines.loan_request_id
        WHERE lr.produit = 'TAYSSIR' AND date(lr.created_date) >= '2024-06-13'
        ORDER BY DATE_DEMANDE_CREDIT DESC
    """
    df = pd.read_sql(query1, conn)

    # Query 2
    query2 = f"""
        SELECT
            cl.id as id3,
            lr.id as id4,
            lr.code_es as code_es,
            lr.produit as produit,
            pd.method as method,
            pd.deadline as deadline,
            pd.payment_date as payment_date,
            pd.status as status
        FROM clients cl
        LEFT JOIN loan_request lr ON cl.id = lr.fk_client
        LEFT JOIN payment_deadline pd ON lr.id = pd.fk_loan_request
        WHERE lr.produit = 'TAYSSIR' AND date(lr.created_date) >= '2024-06-13'
    """
    df2 = pd.read_sql(query2, conn)
    conn.close()

    # Merge DataFrames
    merged_df = pd.merge(df, df2, how='right', left_on=['id1', 'id2'], right_on=['id3', 'id4'])
    merged_df = merged_df.sort_values(by='date_demande_credit', ascending=False)
    print(merged_df)
    return merged_df, df

#extract_data_from_hive("2024-06-13")
