import pandas as pd
from sas7bdat import SAS7BDAT
import pyreadstat
#import saspy
import sys
import os 
from datetime import datetime
cwd = os.getcwd()
root_dir = os.path.dirname(cwd)
sys.path.insert(0, root_dir)


Fiber_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\fiber_swietlik.sas7bdat'
#CRM_MOB_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\crm_mobile.sas7bdat'
CRM_FIX_prod_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\zainstalowane_produkty.sas7bdat'
CRM_FIX_facorder_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\fac_order_customer.sas7bdat'
CRM_FIX_orderlines_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\order_lines.sas7bdat'
 
decyzje_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\decyzje.sas7bdat'
potencjal_ret_sas = 'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\potencjal_retencja_am_202504.sas7bdat'
 
#UWAGA FIBER JEST WIDOKIEM I MOŻE BYĆ Z TYM PROBLEM.... 
# Fiber_sas =/pa/data/sharedlocal/detal/workspace/JZRR/fiber_swietlik.sas7bdat 
#jednak robimy kopie fibera bo tam jest za duzo dubli to długo trwa '/pa/data/shared/detal/pa_pirb2b/public/fiber/v_baza_fiber_all.sas7bdat'
# CRM_MOB_sas = '/pa/data/sharedlocal/detal/workspace/JZRR/crm_mobile.sas7bdat'
# CRM_FIX_prod_sas = '/pa/data/sharedlocal/detal/workspace/JZRR/zainstalowane_produkty.sas7bdat'
# CRM_FIX_facorder_sas = '/pa/data/sharedlocal/detal/workspace/JZRR/fac_order_customer.sas7bdat'
# CRM_FIX_orderlines_sas = '/pa/data/sharedlocal/detal/workspace/JZRR/order_lines.sas7bdat'
 
# decyzje_sas = '/pa/data/shared/detal/pa_digit/public/maszynka_formatki/tabele_posrednie/decyzje.sas7bdat'
# potencjal_ret_sas = '/pa/data/shared/detal/workspace/SME_SOHO/potencjal_retencja_am_202504.sas7bdat'
#tu wazne pytanie kiedy powstaje plik z nową datą


 #???
kolumb_sas = 'CZEKAMY NA RADKA'


#dla duzych zbiorów działa to jak krew z nosa ja nie wytrzymałam napięcia ;D
# def get_data(sas_file):
    
#     try:
#         data = SAS7BDAT(sas_file).to_data_frame()
#         df = pd.DataFrame(data)  
#         return df    
#     except Exception as e:
#         print(f"Error loading SAS file: {e}")
#         return None

def get_data(sas_file):
    try:
        df, meta = pyreadstat.read_sas7bdat(sas_file)
        return df
    except Exception as e:
        print(f"Error loading SAS file: {e}")
        return None

def get_modification_date(sas_file):
    """Zwraca datę modyfikacji pliku - żebyśmy mogli dać znać ludziom z jak świeżych danych korzystają."""
    try:
        timestamp = os.path.getmtime(sas_file)
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error getting modification date: {e}")
        return None
 
def get_data2(sas_file, columns=None):
    """
    Reads data from a SAS file, optionally specifying the columns to read.

    Args:
        sas_file (str): The path to the SAS file.
        columns (list, optional): A list of column names to read. If None, all columns will be read.

    Returns:
        pd.DataFrame: The DataFrame containing the data from the SAS file.
    """

    try:
        if columns is None:
            # Read all columns
            df, meta = pyreadstat.read_sas7bdat(sas_file)
        else:
            # Read only the specified columns
            df, meta = pyreadstat.read_sas7bdat(sas_file, usecols=columns)
        return df
    except Exception as e:
        print(f"Error loading SAS file: {e}")
        return None


######################################## CRM MOB ########################################
#CRM po MSISDNach z Easy_Msisdn -> kol. "(zam ite) Nr MSISDN" uwaga ich może być wiele 
#    np. dla "(umo) Nr/nazwa umowy"n="UM/00760832/2024" jest 31 MSISDN
MSISDNy_z_EASY = {
    '(zam ite) Nr MSISDN': ['500201002','500201022','501800362','501982548','504215729','505148451','505148452','505175018','505175677','505175696','505228212','505766182','506580508','509377361','509377362','509377364','510173266','512428165','515468347','515724392','519167817','519167866','572348571','572348573','572815498','573788872','690173436','690475561','786865742','797437098','797492322']
}
MSISDNy_z_EASY = pd.DataFrame(MSISDNy_z_EASY)
MSISDNy_z_EASY2 = {
    '(zam ite) Nr MSISDN': ['214081693','214077864','214078707','214080736','214076863','214082690','214074830','214075344','214079219','214080233','214076353','214081217','214074320','214082190','214077375','214073310','214078196','214078451',]
}
 
msisdny_set2 = set(MSISDNy_z_EASY2['(zam ite) Nr MSISDN'])



def CRM_MOB(sas_file ,msisdny):
        dane=get_data(sas_file)   
        if dane is None:
            return None  # Zwróć None, jeśli nie udało się wczytać danych   
        
        if msisdny:
            data_filtered=dane[(dane['KON_MSISDN'].isin(msisdny)) ]
            return data_filtered

        return None



CRM_filtered=CRM_MOB(CRM_MOB_sas,msisdny_set2)
if CRM_filtered is not None:
    print(CRM_filtered)
    print(get_modification_date(CRM_MOB_sas))


######################################## CRM FIXED ########################################
#szukanie po uzupełnionych telkach w KLIK - 3 częś Lista reklamowanych zamowień: 
#jak jest tam PKB no to EASY,
#Jak jest tam msisdn to w crm mobile no i tam jest tez przypisany rodzaj reklamacji
#jak TEL to filtr w CRM FIXED

def CRM_FIXED( sas_facorder,sas_orderline,sas_prod ,telka):
        facorder=get_data(sas_facorder)   
        if facorder is None:
            return None  
        orderline=get_data(sas_orderline)   
        if orderline is None:
            return None     
        prod=get_data(sas_prod)   
        if prod is None:
            return None  
        
        facorder_f=facorder[(facorder['EXTERNAL_ID']==telka) ] 
        orderline_f=orderline[(orderline['EXTERNAL_ID']==telka) ] 
        prod_f=prod[(prod['cust_bo_id_txt'].isin(facorder_f['cust_bo_id_txt']) ) ] 
        
        return facorder_f, orderline_f, prod_f

        return None



facorder_filtered, orderline_filtered, prod_filtered = CRM_FIXED(CRM_FIX_facorder_sas,CRM_FIX_orderlines_sas,CRM_FIX_prod_sas,'TEL000142746547')
if facorder_filtered is not None:
    print(facorder_filtered)
    print(orderline_filtered)  
    print(prod_filtered)       
    print(min(get_modification_date(CRM_FIX_facorder_sas),get_modification_date(CRM_FIX_orderlines_sas),get_modification_date(CRM_FIX_prod_sas)))



######################################## ŚWIETLIK -> U NAS FIBER ########################################
#po adresie z CRM FIXED tabela prod_filtered - potrzebne tylko unikalne adresy

def FIBER(sas_file ,  unique_addresses  ):
        dane=get_data(sas_file )   
        if dane is None:
            return None  # Zwróć None, jeśli nie udało się wczytać danych   
        
        # Filter the FIBER DataFrame based on the unique addresses
        # Dopasuj dokładne wiersze za pomocą merge
        unique_addresses = unique_addresses.rename(
            columns={
                'installation_town': 'miasto',
                'installation_street': 'ulica',
                'installation_number': 'nr_domu',
                'installation_zipcode': 'KODPOCZT'
            }
        )

        # Wykonaj wewnętrzne łączenie (inner join), aby dopasować dokładne wiersze
        data_filtered = pd.merge(dane, unique_addresses, on=['miasto', 'ulica', 'nr_domu', 'KODPOCZT'], how='inner')

        return data_filtered

        return None

 
CRM_adresy=prod_filtered[['installation_town', 'installation_street', 'installation_number', 'installation_zipcode']].drop_duplicates()
CRM_adresy['installation_zipcode']=CRM_adresy['installation_zipcode'].str.replace('-', '', regex=False)

FIBER_filtered=FIBER(Fiber_sas, CRM_adresy)
if FIBER_filtered is not None:
    print(FIBER_filtered)
    print(get_modification_date(Fiber_sas))
    
######################################## OCHRONA ZAMÓWIEŃ -DECYZJE ########################################
#tu filtr po telce jak w crm - ona powinna byc uzupelniona w KLIK
def OCHRONA_ZAM(sas_file ,telka):
        dane=get_data(sas_file)   
        if dane is None:
            return None  # Zwróć None, jeśli nie udało się wczytać danych   
        

        data_filtered=dane[(dane['CRMFIX']==telka)]
        return data_filtered[['CRMFIX','sales_channel_1','salesman_code_1','sales_channel_2','salesman_code_2','miesiac_skompletowania']]

        return None



# ochrona_zam_filtered=OCHRONA_ZAM(decyzje_sas,'TEL000142746547')
# if ochrona_zam_filtered is not None:
#     print(ochrona_zam_filtered)
#     print(get_modification_date(decyzje_sas))

ochrona_zam_filtered=OCHRONA_ZAM(decyzje_sas,'TEL000142707763')
if ochrona_zam_filtered is not None:
    print(ochrona_zam_filtered)
    print(get_modification_date(decyzje_sas))


######################################## POTENCJAL AM ########################################
#no dobra, w kliku jest podana data miesiac i trzeba wziac zbior z ta data jak w tym mies.
#filtrujemy po MSISDN
miesiac = '04/2025'
miesiac_str, rok_str = miesiac.split('/')
# Tworzenie ścieżki do pliku
potencjal_ret_sas = f'C:\\Users\\komarka1\\Documents\\01_ZADANIA\\07_jedno_zrodlo_reklamacji\\pa_sas\\potencjal_retencja_am_{rok_str}{miesiac_str}.sas7bdat'
MSISDNy_z_EASY2 = {
    'MSISDN': ['453683125','453683126','453685200','453685201']
}

msisdny_set2 = set(MSISDNy_z_EASY2['MSISDN'])


def POTENCJAL_AM(sas_file ,msisdny):
        dane=get_data(sas_file)   
        if dane is None:
            return None  # Zwróć None, jeśli nie udało się wczytać danych   
        


        data_filtered=dane[(dane['MSISDN'].isin(msisdny)) ]
        return data_filtered

        return None



potencjal_am_filtered=POTENCJAL_AM(potencjal_ret_sas,msisdny_set2)
if potencjal_am_filtered is not None:
    print(potencjal_am_filtered)
    print(get_modification_date(potencjal_ret_sas))

