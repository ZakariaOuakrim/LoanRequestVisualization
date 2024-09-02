import pandas as pd
import matplotlib.pyplot as plt

custom_colors = ['#87CEEB', '#4682B4', '#1E90FF', '#6495ED', '#7B68EE']

def shorten_label(label):
    return label[:5] if len(label) > 5 else label

def client_sumEcheance(df):
    df['short_id'] = df['client_id'].str[:5]

    df_grouped_client = df.groupby('short_id')['nombre_echeance'].sum()
    fig1, ax1 = plt.subplots(figsize=(6, 4))  
    ax1.bar(df_grouped_client.index, df_grouped_client.values)
    ax1.set_title("Client & Nombre Total d'échéance", fontsize=10)
    ax1.set_xlabel("Client ID (shortened)")
    ax1.set_ylabel("Nombre echeance")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig1

def codeEs_sumEcheance(df):
    # Chart 2: code_es & Total Nombre echeance
    df_code_es_total_Nombre_eacheance = pd.crosstab(df['code_es'], df['nombre_echeance'])
    fig2, ax2 = plt.subplots(figsize=(5, 4))
    df_code_es_total_Nombre_eacheance.plot(kind='bar', ax=ax2, color=custom_colors)
    ax2.set_title("Espace service & Total Nombre échéance", fontsize=10)
    ax2.set_xlabel("Espace service")
    ax2.set_ylabel("Total Nombre echeance")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig2

def produitPercentage(df):
    fig3, ax3 = plt.subplots(figsize=(5, 4))
    df_produit = df.dropna(subset=['produit'])
    df_result_produit = df_produit['produit'].value_counts()
    ax3.pie(df_result_produit, labels=df_result_produit.index, autopct='%1.1f%%')
    ax3.set_title("Loan Request by Produit", fontsize=10)
    plt.tight_layout()
    return fig3

def status_sumLoanRequest(df):
    df_result = pd.crosstab(df['loan_request_id'], df['statut_dossier'])
    df_status_totals = df_result.sum(axis=0)
    fig4, ax4 = plt.subplots(figsize=(5, 4))
    ax4.bar(df_status_totals.index, df_status_totals.values, color=custom_colors)
    ax4.set_title("Status to the number of loan requests", fontsize=10)
    ax4.set_xlabel("Status")
    ax4.set_ylabel("Number of loan requests")
    # Shorten labels for x-ticks
    ax4.set_xticklabels([shorten_label(label) for label in df_status_totals.index], rotation=45, ha='right')
    plt.tight_layout()
    return fig4

def rejectionReason_sumLoanRequest(df):
    df_result = pd.crosstab(df['loan_request_id'], df['MOTIF_DE_REJET'])
    df_rejection_reason_totals = df_result.sum(axis=0)
    fig5, ax5 = plt.subplots(figsize=(5, 4))
    ax5.bar(df_rejection_reason_totals.index, df_rejection_reason_totals.values, color=custom_colors)
    ax5.set_title("Reject Reason to the number of loan requests", fontsize=10)
    ax5.set_xlabel("Reject Reason")
    ax5.set_ylabel("Number of loan requests")
    # Shorten labels for x-ticks
    ax5.set_xticklabels([shorten_label(label) for label in df_rejection_reason_totals.index], rotation=45, ha='right')
    plt.tight_layout()
    return fig5

def codeEs_rejectionReason(df):
    df_result = pd.crosstab(df['code_es'], df['statut_dossier'])
    fig6, ax6 = plt.subplots(figsize=(5, 4))
    df_result.plot(kind='bar', ax=ax6, legend=True, color=custom_colors)
    ax6.set_title("code_es & status", fontsize=10)
    ax6.set_xlabel("code_es")
    ax6.set_ylabel("Number of loan requests")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig6

    

