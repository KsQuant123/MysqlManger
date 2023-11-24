import os
import pandas as pd
from zipfile import ZipFile

fin_columns_list = ['Open_Interest_All', 'Dealer_Positions_Long_All',
                    'Dealer_Positions_Short_All', 'Dealer_Positions_Spread_All',
                    'Asset_Mgr_Positions_Long_All', 'Asset_Mgr_Positions_Short_All',
                    'Asset_Mgr_Positions_Spread_All', 'Lev_Money_Positions_Long_All',
                    'Lev_Money_Positions_Short_All', 'Lev_Money_Positions_Spread_All',
                    'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All',
                    'Other_Rept_Positions_Spread_All', 'Tot_Rept_Positions_Long_All',
                    'Tot_Rept_Positions_Short_All', 'NonRept_Positions_Long_All',
                    'NonRept_Positions_Short_All', 'Change_in_Open_Interest_All',
                    'Change_in_Dealer_Long_All', 'Change_in_Dealer_Short_All',
                    'Change_in_Dealer_Spread_All', 'Change_in_Asset_Mgr_Long_All',
                    'Change_in_Asset_Mgr_Short_All', 'Change_in_Asset_Mgr_Spread_All',
                    'Change_in_Lev_Money_Long_All', 'Change_in_Lev_Money_Short_All',
                    'Change_in_Lev_Money_Spread_All', 'Change_in_Other_Rept_Long_All',
                    'Change_in_Other_Rept_Short_All', 'Change_in_Other_Rept_Spread_All',
                    'Change_in_Tot_Rept_Long_All', 'Change_in_Tot_Rept_Short_All',
                    'Change_in_NonRept_Long_All', 'Change_in_NonRept_Short_All',
                    'Pct_of_Open_Interest_All', 'Pct_of_OI_Dealer_Long_All',
                    'Pct_of_OI_Dealer_Short_All', 'Pct_of_OI_Dealer_Spread_All',
                    'Pct_of_OI_Asset_Mgr_Long_All', 'Pct_of_OI_Asset_Mgr_Short_All',
                    'Pct_of_OI_Asset_Mgr_Spread_All', 'Pct_of_OI_Lev_Money_Long_All',
                    'Pct_of_OI_Lev_Money_Short_All', 'Pct_of_OI_Lev_Money_Spread_All',
                    'Pct_of_OI_Other_Rept_Long_All', 'Pct_of_OI_Other_Rept_Short_All',
                    'Pct_of_OI_Other_Rept_Spread_All', 'Pct_of_OI_Tot_Rept_Long_All',
                    'Pct_of_OI_Tot_Rept_Short_All', 'Pct_of_OI_NonRept_Long_All',
                    'Pct_of_OI_NonRept_Short_All', 'Traders_Tot_All',
                    'Traders_Dealer_Long_All', 'Traders_Dealer_Short_All',
                    'Traders_Dealer_Spread_All', 'Traders_Asset_Mgr_Long_All',
                    'Traders_Asset_Mgr_Short_All', 'Traders_Asset_Mgr_Spread_All',
                    'Traders_Lev_Money_Long_All', 'Traders_Lev_Money_Short_All',
                    'Traders_Lev_Money_Spread_All', 'Traders_Other_Rept_Long_All',
                    'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
                    'Traders_Tot_Rept_Long_All', 'Traders_Tot_Rept_Short_All',
                    'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All',
                    'Conc_Gross_LE_8_TDR_Long_All', 'Conc_Gross_LE_8_TDR_Short_All',
                    'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All',
                    'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All', ]

dea_columns_lsit = ['Open_Interest_All', 'NonComm_Positions_Long_All',
                    'NonComm_Positions_Short_All', 'NonComm_Postions_Spread_All',
                    'Comm_Positions_Long_All', 'Comm_Positions_Short_All',
                    'Tot_Rept_Positions_Long_All', 'Tot_Rept_Positions_Short_All',
                    'NonRept_Positions_Long_All', 'NonRept_Positions_Short_All',
                    'Open_Interest_Other', 'NonComm_Positions_Long_Other',
                    'NonComm_Positions_Short_Other', 'NonComm_Positions_Spread_Other',
                    'Comm_Positions_Long_Other', 'Comm_Positions_Short_Other',
                    'Tot_Rept_Positions_Long_Other', 'Tot_Rept_Positions_Short_Other',
                    'NonRept_Positions_Long_Other', 'NonRept_Positions_Short_Other',
                    'Change_in_Open_Interest_All', 'Change_in_NonComm_Long_All',
                    'Change_in_NonComm_Short_All', 'Change_in_NonComm_Spead_All',
                    'Change_in_Comm_Long_All', 'Change_in_Comm_Short_All',
                    'Change_in_Tot_Rept_Long_All', 'Change_in_Tot_Rept_Short_All',
                    'Change_in_NonRept_Long_All', 'Change_in_NonRept_Short_All',
                    'Pct_of_Open_Interest_All', 'Pct_of_OI_NonComm_Long_All',
                    'Pct_of_OI_NonComm_Short_All', 'Pct_of_OI_NonComm_Spread_All',
                    'Pct_of_OI_Comm_Long_All', 'Pct_of_OI_Comm_Short_All',
                    'Pct_of_OI_Tot_Rept_Long_All', 'Pct_of_OI_Tot_Rept_Short_All',
                    'Pct_of_OI_NonRept_Long_All', 'Pct_of_OI_NonRept_Short_All',
                    'Pct_of_Open_Interest_Other', 'Pct_of_OI_NonComm_Long_Other',
                    'Pct_of_OI_NonComm_Short_Other', 'Pct_of_OI_NonComm_Spread_Other',
                    'Pct_of_OI_Comm_Long_Other', 'Pct_of_OI_Comm_Short_Other',
                    'Pct_of_OI_Tot_Rept_Long_Other', 'Pct_of_OI_Tot_Rept_Short_Other',
                    'Pct_of_OI_NonRept_Long_Other', 'Pct_of_OI_NonRept_Short_Other',
                    'Traders_Tot_All', 'Traders_NonComm_Long_All',
                    'Traders_NonComm_Short_All', 'Traders_NonComm_Spread_All',
                    'Traders_Comm_Long_All', 'Traders_Comm_Short_All',
                    'Traders_Tot_Rept_Long_All', 'Traders_Tot_Rept_Short_All',
                    'Traders_Tot_Other', 'Traders_NonComm_Long_Other',
                    'Traders_NonComm_Short_Other', 'Traders_NonComm_Spread_Other',
                    'Traders_Comm_Long_Other', 'Traders_Comm_Short_Other',
                    'Traders_Tot_Rept_Long_Other', 'Traders_Tot_Rept_Short_Other',
                    'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All',
                    'Conc_Gross_LE_8_TDR_Long_All', 'Conc_Gross_LE_8_TDR_Short_All',
                    'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All',
                    'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All',
                    'Conc_Gross_LE_4_TDR_Long_Other', 'Conc_Gross_LE_4_TDR_Short_Other',
                    'Conc_Gross_LE_8_TDR_Long_Other', 'Conc_Gross_LE_8_TDR_Short_Other',
                    'Conc_Net_LE_4_TDR_Long_Other', 'Conc_Net_LE_4_TDR_Short_Other',
                    'Conc_Net_LE_8_TDR_Long_Other', 'Conc_Net_LE_8_TDR_Short_Other', ]

disagg_columns_list = ['Open_Interest_All', 'Prod_Merc_Positions_Long_ALL',
                       'Prod_Merc_Positions_Short_ALL', 'Swap_Positions_Long_All',
                       'Swap__Positions_Short_All', 'Swap__Positions_Spread_All',
                       'M_Money_Positions_Long_ALL', 'M_Money_Positions_Short_ALL',
                       'M_Money_Positions_Spread_ALL', 'Other_Rept_Positions_Long_ALL',
                       'Other_Rept_Positions_Short_ALL', 'Other_Rept_Positions_Spread_ALL',
                       'Tot_Rept_Positions_Long_All', 'Tot_Rept_Positions_Short_All',
                       'NonRept_Positions_Long_All', 'NonRept_Positions_Short_All',
                       'Open_Interest_Other', 'Prod_Merc_Positions_Long_Other',
                       'Prod_Merc_Positions_Short_Other', 'Swap_Positions_Long_Other',
                       'Swap_Positions_Short_Other', 'Swap_Positions_Spread_Other',
                       'M_Money_Positions_Long_Other', 'M_Money_Positions_Short_Other',
                       'M_Money_Positions_Spread_Other', 'Other_Rept_Positions_Long_Other',
                       'Other_Rept_Positions_Short_Other', 'Other_Rept_Positions_Spread_Othr',
                       'Tot_Rept_Positions_Long_Other', 'Tot_Rept_Positions_Short_Other',
                       'NonRept_Positions_Long_Other', 'NonRept_Positions_Short_Other',
                       'Change_in_Open_Interest_All', 'Change_in_Prod_Merc_Long_All',
                       'Change_in_Prod_Merc_Short_All', 'Change_in_Swap_Long_All',
                       'Change_in_Swap_Short_All', 'Change_in_Swap_Spread_All',
                       'Change_in_M_Money_Long_All', 'Change_in_M_Money_Short_All',
                       'Change_in_M_Money_Spread_All', 'Change_in_Other_Rept_Long_All',
                       'Change_in_Other_Rept_Short_All', 'Change_in_Other_Rept_Spread_All',
                       'Change_in_Tot_Rept_Long_All', 'Change_in_Tot_Rept_Short_All',
                       'Change_in_NonRept_Long_All', 'Change_in_NonRept_Short_All',
                       'Pct_of_Open_Interest_All', 'Pct_of_OI_Prod_Merc_Long_All',
                       'Pct_of_OI_Prod_Merc_Short_All', 'Pct_of_OI_Swap_Long_All',
                       'Pct_of_OI_Swap_Short_All', 'Pct_of_OI_Swap_Spread_All',
                       'Pct_of_OI_M_Money_Long_All', 'Pct_of_OI_M_Money_Short_All',
                       'Pct_of_OI_M_Money_Spread_All', 'Pct_of_OI_Other_Rept_Long_All',
                       'Pct_of_OI_Other_Rept_Short_All', 'Pct_of_OI_Other_Rept_Spread_All',
                       'Pct_of_OI_Tot_Rept_Long_All', 'Pct_of_OI_Tot_Rept_Short_All',
                       'Pct_of_OI_NonRept_Long_All', 'Pct_of_OI_NonRept_Short_All',
                       'Pct_of_Open_Interest_Other', 'Pct_of_OI_Prod_Merc_Long_Other',
                       'Pct_of_OI_Prod_Merc_Short_Other', 'Pct_of_OI_Swap_Long_Other',
                       'Pct_of_OI_Swap_Short_Other', 'Pct_of_OI_Swap_Spread_Other',
                       'Pct_of_OI_M_Money_Long_Other', 'Pct_of_OI_M_Money_Short_Other',
                       'Pct_of_OI_M_Money_Spread_Other', 'Pct_of_OI_Other_Rept_Long_Other',
                       'Pct_of_OI_Other_Rept_Short_Other', 'Pct_of_OI_Other_Rept_Spread_Othr',
                       'Pct_of_OI_Tot_Rept_Long_Other', 'Pct_of_OI_Tot_Rept_Short_Other',
                       'Pct_of_OI_NonRept_Long_Other', 'Pct_of_OI_NonRept_Short_Other',
                       'Traders_Tot_All', 'Traders_Prod_Merc_Long_All',
                       'Traders_Prod_Merc_Short_All', 'Traders_Swap_Long_All',
                       'Traders_Swap_Short_All', 'Traders_Swap_Spread_All',
                       'Traders_M_Money_Long_All', 'Traders_M_Money_Short_All',
                       'Traders_M_Money_Spread_All', 'Traders_Other_Rept_Long_All',
                       'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
                       'Traders_Tot_Rept_Long_All', 'Traders_Tot_Rept_Short_All',
                       'Traders_Tot_Other', 'Traders_Prod_Merc_Long_Other',
                       'Traders_Prod_Merc_Short_Other', 'Traders_Swap_Long_Other',
                       'Traders_Swap_Short_Other', 'Traders_Swap_Spread_Other',
                       'Traders_M_Money_Long_Other', 'Traders_M_Money_Short_Other',
                       'Traders_M_Money_Spread_Other', 'Traders_Other_Rept_Long_Other',
                       'Traders_Other_Rept_Short_Other', 'Traders_Other_Rept_Spread_Other',
                       'Traders_Tot_Rept_Long_Other', 'Traders_Tot_Rept_Short_Other',
                       'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All',
                       'Conc_Gross_LE_8_TDR_Long_All', 'Conc_Gross_LE_8_TDR_Short_All',
                       'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All',
                       'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All',
                       'Conc_Gross_LE_4_TDR_Long_Other', 'Conc_Gross_LE_4_TDR_Short_Other',
                       'Conc_Gross_LE_8_TDR_Long_Other', 'Conc_Gross_LE_8_TDR_Short_Other',
                       'Conc_Net_LE_4_TDR_Long_Other', 'Conc_Net_LE_4_TDR_Short_Other',
                       'Conc_Net_LE_8_TDR_Long_Other', 'Conc_Net_LE_8_TDR_Short_Other', ]

columns_dict = {'Traders': fin_columns_list, 'Report': dea_columns_lsit, 'Disagg': disagg_columns_list}


def read_zip_excel(path):
    with ZipFile(path, 'r') as z:  # 打开zip
        result = {}
        for file_info in z.infolist():
            f = z.open(file_info)
            result[file_info.filename] = pd.read_excel(f)
    f.close()
    z.close()
    return result


def create_file(path):
    if not os.path.exists(path):
        os.mkdir(path)


def raw_data_load(term_df, data_path):
    raw_data = term_df.copy()
    year = raw_data['Report_Date_as_MM_DD_YYYY'].iloc[0].year
    raw_data.index = raw_data['Report_Date_as_MM_DD_YYYY']
    raw_data_filename = os.path.join(data_path, 'raw_data_{}.csv'.format(year))
    raw_data.index.name = 'date'
    raw_data = raw_data.sort_values(by='date', ascending=True)
    raw_data.to_csv(raw_data_filename, date_format='%Y-%m-%d')
    return raw_data


def raw_factor_load(term_df, data_path, columns_list):
    raw_factor = term_df.copy()
    year = raw_factor['Report_Date_as_MM_DD_YYYY'].iloc[0].year
    raw_factor.columns = [x.replace('_ALL', '_All').replace('__', '_') for x in raw_factor.columns]
    raw_factor = raw_factor[[x for x in raw_factor.columns if '_All' in x]]
    # raw_factor = raw_factor[columns_list]
    raw_factor_filename = os.path.join(data_path, 'raw_factor_{}.csv'.format(year))
    raw_factor.to_csv(raw_factor_filename, date_format='%Y-%m-%d')
    return raw_factor


# def cot_factor_load(term_df, n_week=156):
#     result = ff.calculate_index(df=term_df, n_week=n_week)
#     return result
