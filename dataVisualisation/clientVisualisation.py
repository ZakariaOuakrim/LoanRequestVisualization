import pandas as pd
import matplotlib.pyplot as plt

def gender_status(df):
    fig7, ax7 = plt.subplots(figsize=(5, 4))
    df_unique_clients= df.drop_duplicates(subset='short_id')
    df_result=pd.crosstab(df_unique_clients['gender'],df_unique_clients['statut_dossier'])
    df_result=df_result.rename(index={"1":"Male","2":"Female"})
    df_result.plot(kind='bar', ax=ax7, legend=True)
    return fig7

def age_status(df):
    # Crosstab for age and status
    fig8, ax8 = plt.subplots(figsize=(5, 4))
    df_unique_clients= df.drop_duplicates(subset='short_id')
    df_result_age_status = pd.crosstab(df_unique_clients['age'], df_unique_clients['statut_dossier'])
    plot=df_result_age_status.plot(kind='bar',ax=ax8,legend=True)
    plot.set_xlabel("Age")
    return fig8

def rejectionReason_Gender(df):
    fig9, ax9 = plt.subplots(figsize=(5, 4))
    df_unique_clients= df.drop_duplicates(subset='short_id')
    df_result_rr_gender=pd.crosstab(df_unique_clients['MOTIF_DE_REJET'],df_unique_clients['gender'])
    df_result_rr_gender=df_result_rr_gender.rename(index={"01":"Male","02":"Female"})
    df_result_rr_gender=df_result_rr_gender.rename(index={"insufficient.score":"IS","level.1.client":"L1C","loan.already.in.progress":"LAIP","offer.with.lower.amount":"OWLA"})
    plot=df_result_rr_gender.plot(kind='bar', ax=ax9, legend=True,color=['pink','blue','gray'])
    plot.set_xlabel("Rejection Reason")
    return fig9

def rejectionReason_Age(df):
    fig10, ax10 = plt.subplots(figsize=(5, 4))
    df_unique_clients= df.drop_duplicates(subset='short_id')
    df_result_rr_age=pd.crosstab(df_unique_clients['MOTIF_DE_REJET'],df_unique_clients['age'])
    df_result_rr_age=df_result_rr_age.rename(index={"insufficient.score":"IS","level.1.client":"L1C","loan.already.in.progress":"LAIP","offer.with.lower.amount":"OWLA"})
    plot=df_result_rr_age.plot(kind='bar', ax=ax10, legend=True)
    plot.set_xlabel("Rejection Reason")
    return fig10


def client_city(df):    
    fig11, ax11 = plt.subplots(figsize=(5, 4))
    df_unique_clients= df.drop_duplicates(subset='short_id')
    df_result_city_client = df_unique_clients['city'].value_counts()
    ax11.pie(df_result_city_client, labels=df_result_city_client.index, autopct='%1.1f%%')
    return fig11 


