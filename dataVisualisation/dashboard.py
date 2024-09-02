import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import pandas as pd
from clientVisualisation import *
from echeanceVisualisation import *


df = pd.read_csv('loan_request--2024-06-13.csv', delimiter=';')

#-------------------------------------------------------------------------------------------------------------------

#open echeance details 
def open_echeance_details():
    main_frame_client.destroy()
    main_frame_echeance = tk.Frame(root)
    main_frame_echeance.grid(row=0, column=1, sticky="nsew", rowspan=2)

    upper_frame1 = tk.Frame(main_frame_echeance)
    upper_frame1.grid(row=0, column=0, sticky="nsew")

    upper_frame2 = tk.Frame(main_frame_echeance)
    upper_frame2.grid(row=0, column=1, sticky="nsew")

    upper_frame3 = tk.Frame(main_frame_echeance)
    upper_frame3.grid(row=0, column=2, sticky="nsew")

    lower_frame1 = tk.Frame(main_frame_echeance)
    lower_frame1.grid(row=1, column=0, sticky="nsew")

    lower_frame2 = tk.Frame(main_frame_echeance)
    lower_frame2.grid(row=1, column=1, sticky="nsew")

    lower_frame3 = tk.Frame(main_frame_echeance)
    lower_frame3.grid(row=1, column=2, sticky="nsew")

    root.grid_rowconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(1, weight=3)  

    # Configure grid layout for charts_frame
    main_frame_echeance.grid_rowconfigure(0, weight=1)
    main_frame_echeance.grid_rowconfigure(1, weight=1)
    main_frame_echeance.grid_columnconfigure(0, weight=1)
    main_frame_echeance.grid_columnconfigure(1, weight=1)
    main_frame_echeance.grid_columnconfigure(2, weight=1)

    canvas1 = FigureCanvasTkAgg(client_sumEcheance(df), upper_frame1)
    canvas1.draw()
    canvas1.get_tk_widget().pack(fill="both", expand=True)

    canvas2 = FigureCanvasTkAgg(codeEs_sumEcheance(df), upper_frame2)
    canvas2.draw()
    canvas2.get_tk_widget().pack(fill="both", expand=True)

    canvas3 = FigureCanvasTkAgg(produitPercentage(df), upper_frame3)
    canvas3.draw()
    canvas3.get_tk_widget().pack(fill="both", expand=True)

    canvas4 = FigureCanvasTkAgg(status_sumLoanRequest(df), lower_frame1)
    canvas4.draw()
    canvas4.get_tk_widget().pack(fill="both", expand=True)

    canvas5 = FigureCanvasTkAgg(rejectionReason_sumLoanRequest(df), lower_frame2)
    canvas5.draw()
    canvas5.get_tk_widget().pack(fill="both", expand=True)

    canvas6 = FigureCanvasTkAgg(codeEs_rejectionReason(df), lower_frame3)
    canvas6.draw()
    canvas6.get_tk_widget().pack(fill="both", expand=True)

def open_client_details():
    main_frame_echeance.destroy()
    main_frame_client = tk.Frame(root)
    main_frame_client.grid(row=0, column=1, sticky="nsew", rowspan=2)
    
    upper_frame1 = tk.Frame(main_frame_client)
    upper_frame1.grid(row=0, column=0, sticky="nsew")

    upper_frame2 = tk.Frame(main_frame_client)
    upper_frame2.grid(row=0, column=1, sticky="nsew")

    upper_frame3 = tk.Frame(main_frame_client)
    upper_frame3.grid(row=0, column=2, sticky="nsew")

    lower_frame1 = tk.Frame(main_frame_client)
    lower_frame1.grid(row=1, column=0, sticky="nsew")

    lower_frame2 = tk.Frame(main_frame_client)
    lower_frame2.grid(row=1, column=1, sticky="nsew")

    lower_frame3 = tk.Frame(main_frame_client)
    lower_frame3.grid(row=1, column=2, sticky="nsew")


    root.grid_rowconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(1, weight=3)

    main_frame_client.grid_rowconfigure(0, weight=1)
    main_frame_client.grid_rowconfigure(1, weight=1)
    main_frame_client.grid_columnconfigure(0, weight=1)
    main_frame_client.grid_columnconfigure(1, weight=1)
    main_frame_client.grid_columnconfigure(2, weight=1)


    canvas1 = FigureCanvasTkAgg(gender_status(df), upper_frame1)
    canvas1.draw()
    canvas1.get_tk_widget().pack(fill="both", expand=True)

    canvas2 = FigureCanvasTkAgg(age_status(df), upper_frame2)
    canvas2.draw()
    canvas2.get_tk_widget().pack(fill="both", expand=True)
    
    canvas3 = FigureCanvasTkAgg(rejectionReason_Gender(df), upper_frame3)
    canvas3.draw()
    canvas3.get_tk_widget().pack(fill="both", expand=True)

    canvas4 = FigureCanvasTkAgg(rejectionReason_Age(df), lower_frame1)
    canvas4.draw()
    canvas4.get_tk_widget().pack(fill="both", expand=True)

    canvas5 = FigureCanvasTkAgg(client_city(df), lower_frame2)
    canvas5.draw()
    canvas5.get_tk_widget().pack(fill="both", expand=True)
    
    canvas6 = FigureCanvasTkAgg(gender_status(df), lower_frame3)
    canvas6.draw()
    canvas6.get_tk_widget().pack(fill="both", expand=True)

# Main window and charts
root = tk.Tk()
root.title("Loan Request Dashboard")
main_frame_client=tk.Frame(root)
main_frame_echeance=tk.Frame(root)

side_frame = tk.Frame(root, bg="#0000CD")
side_frame.grid(row=0, column=0, rowspan=2, sticky="nsew")

button_echeance = tk.Button(side_frame, text="Échéance détaillée", bg="#0000CD", fg="#FFF", font=25,command=open_echeance_details)
button_echeance.pack(pady=50, padx=20)

button_client = tk.Button(side_frame,text="Détails des clients",bg="#0000CD", fg="#FFF", font=25,command=open_client_details)
button_client.pack(pady=80,padx=20)



# Create a frame for charts

open_echeance_details()
root.mainloop()