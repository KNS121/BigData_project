import data_from_csv_to_bd

def export_coord_df_to_server():
    data_from_csv_to_bd.process_csv_files(
        ['df_oil_for_bd_with_coord_red.csv','df_ppd_for_bd_with_coord_red.csv'])
    

    