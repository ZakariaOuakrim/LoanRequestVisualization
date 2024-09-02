import pandas as pd
from pyhive import hive
import subprocess

import subprocess
import os
from pyhive import hive


def Load_data_to_csv(final_df, date, AllExtractedData):
    temp_csv_path = "/tmp/loan_request--" + date + ".csv"
    final_df.to_csv(temp_csv_path, sep=';', index=False)

    container_csv_path = "/opt/data.csv"  
    container_name = "hive"  
    subprocess.run(["docker", "cp", temp_csv_path, f"{container_name}:{container_csv_path}"])

    conn = hive.Connection(host="hive-server", port=10000, database='m2t')
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE M2T.OUTPUT_TABLE_ETL")

    load_data_query = f"LOAD DATA LOCAL INPATH '{container_csv_path}' INTO TABLE M2T.OUTPUT_TABLE_ETL"
    cursor.execute(load_data_query)

    backup_table_query = """
    INSERT INTO TABLE M2T.OUTPUT_TABLE_ETL_backup14
    SELECT * FROM M2T.OUTPUT_TABLE_ETL
    """
    cursor.execute(backup_table_query)

    cursor.close()
    conn.close()
    os.remove(temp_csv_path)


def Transform_data(df):
    def calculate_date_remboursement(row):
        if (row['method'] is not None and row['method'].startswith('MANUAL') and
                row['deadline'] >= row['payment_date'] and
                row['status'] in ['PAID_LS_RECONCILED', 'PAID_LS', 'PAID']):
            return row['payment_date']
        return None

    def calculate_date_reglement(row):
        if (row['method'] is not None and row['method'].startswith('MANUAL') and
                row['deadline'] < row['payment_date'] and
                row['status'] in ['PAID_LS_RECONCILED', 'PAID_LS', 'PAID']):
            return row['payment_date']
        return None

    def calculate_date_prelevement(row):
        if (row['method'] is not None and row['method'].startswith('BATCH') and
                row['status'] in ['PAID_LS_RECONCILED', 'PAID_LS', 'PAID']):
            return row['payment_date']
        return None

    def handle_rejection_reason(reason):
        if reason is None or reason.strip() == '':
            return 'No Reason'
        return reason

    if 'rejection_reason' in df.columns:
        df['MOTIF_DE_REJET'] = df['rejection_reason'].apply(handle_rejection_reason)
    else:
        print("col Rejection reasons makaynch")

    if 'nombre_echeance' in df.columns:
        df['nombre_echeance'] = df['nombre_echeance'].fillna('0').astype(str)
    else:
        print("col Requested deadlines makaynch")

    # Apply the conditions to create new columns
    df['DATE_REMBOURSEMENT_DERNIERE_ECHEANCE'] = df.apply(calculate_date_remboursement, axis=1)
    df['DATE_REGLEMENT_IMPAYE_DERNIERE_ECHEANCE'] = df.apply(calculate_date_reglement, axis=1)
    df['DATE_PRELEVEMENT_DERNIERE_ECHEANCE'] = df.apply(calculate_date_prelevement, axis=1)

    df=df.drop(columns=['id3','id4','rejection_reason'])  
    df=df.rename(columns={"id1":"client_id","id2":"loan_request_id","requested_deadlines":"NOMBRE_ECHEANCE"})
        
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
        WHERE lr.produit = 'TAYSSIR' AND date(lr.created_date) >= '{required_date}'
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
        WHERE lr.produit = 'TAYSSIR' AND date(lr.created_date) >= '{required_date}'
    """
    df2 = pd.read_sql(query2, conn)
    conn.close()
    
    # Merge DataFrames
    merged_df = pd.merge(df, df2, how='right', left_on=['id1', 'id2'], right_on=['id3', 'id4'])
    merged_df = merged_df.sort_values(by='date_demande_credit', ascending=False)
    
    return merged_df, df

    
#extract_data_from_hive("2024-06-13")