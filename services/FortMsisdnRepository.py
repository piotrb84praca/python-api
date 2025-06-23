import pymssql
from datetime import datetime
import config.config  as config

class FortMsisdnRepository:
     def __init__(self):
        pass

     def validate(self, params):
        required_params = ['date_from','date_to']  
        optional_params = ['um_number', 'ord_number','msisdn']
         
        for param in required_params:
            if param not in params:
                return False, f"Missing required parameter: {param}"

            if not (params.get('um_number') or params.get('ord_number') or params.get('msisdn') ):
                return False, "At least one of 'um_number' or 'ord_number' or 'msisdn' is required."
        try:
            date_from = datetime.strptime(params.get('date_from'), '%Y-%m-%d')
            date_to   = datetime.strptime(params.get('date_to'), '%Y-%m-%d')
        except ValueError:
            return False, "Invalid date format. Expected YYYY-MM-DD for 'date_from' and 'date_to'."

        # Validate that the 'um_number' starts with 'um' and is 16 characters long
        um_number = params.get('um_number')
         
        #if (params.get('um_number') is None or not (params['um_number'].startswith('UM') and len(params['um_number']) == 16)):
        #    return False, "Invalid UM number. It must start with 'UM' and be 16 characters long."
        
        return True, None
         
     def get_data(self, params):
         
        is_valid, error_message = self.validate(params)
        if not is_valid:
            return {'error': error_message}
            
        try:                         
                conn = pymssql.connect(host=config.MSSQLHOST,
                                       user=config.MSSQLLOGIN,
                                       password=config.MSSQLPASSWORD,
                                       charset='UTF-8', database=config.MSSQLDB, tds_version=r'7.0')  
    
                cursor = conn.cursor()       
                
                where=""
                if (params.get('um_number') and len(params.get('um_number')) > 6):
                    where += """ AND (c.ContractNumber ='""" + params['um_number'] + """')"""
                if (params.get('ord_number') and len(params.get('ord_number')) > 6):
                    where += """ AND (mo.elob_mobileordernumber ='""" + params['ord_number'] + """')"""
                if (params.get('msisdn')):
                    msisdn_value = params['msisdn'].replace('|', ',').rstrip(',')
                    msisdn_value = ', '.join(["'" + value.strip() + "'" for value in msisdn_value.split(',')])                   
                    where += """ AND (moi.elob_msisdnnumber in (""" + msisdn_value + """))""" 

                    
                query = """
                SELECT DISTINCT CAST( GETDATE() AS Date ) AS date_generated
            		, c.ContractNumber AS [(umo) Nr/nazwa umowy]
            		, o.elob_OpportunityID AS [(Pro) Nr projektu]
            		, mo.elob_mobileordernumber AS [(zam) Nr zamówienia mob]
            		, mop.elob_contractduration AS [(zam) Czas trwania w miesiącach]
            		, ifsicard.elob_name AS [(zam ite) Karta SIM]
            		, ifsiterm.elob_name AS [(zam ite) Model sprzętu]
            		, moi.elob_imeiterminal AS [(zam ite) Nr IMEI]
            		, moi.elob_msisdnnumber AS [(zam ite) Nr MSISDN]
            		, moi.elob_fullsimcardnumber AS [(zam ite) Pełny numer karty SIM]
            		, ifsicard.elob_sapnumber AS [(zam ite) Pozycja (Karta SIM)]
            		, ifsiterm.elob_sapnumber AS [(zam ite) Pozycja (Model sprzętu)]
            		, mop.elob_name AS [(zam ite) Produkt zamówieina mobile]
            		, moi.elob_previousmsisdnumber AS [(zam ite) Przenoszony numer]
            		, mapmosimtypact.value AS [(zam ite) Typ aktywacji karty SIM]
            		, ifs.elob_ckpcode AS [(zam ite) Kod BSCS terminala]
            		, ckp.elob_name AS [(zam ite) Nazwa terminala]
		FROM contractbase c
			JOIN accountbase a ON c.CustomerId=a.AccountId
			JOIN SystemUserBase sua ON a.ownerid=sua.SystemUserId --właściciel klienta
			INNER JOIN elob_mobileorderBase mo ON c.ContractId=mo.elob_contract -- 4554
			INNER JOIN elob_mobileorderproductBase mop ON mo.elob_mobileorderId=mop.elob_mobileorder
			INNER JOIN elob_mobileorderitemBase moi ON mop.elob_mobileorderproductId= moi.elob_mobileorderproductId
			LEFT JOIN elob_ifsitemBase ifsicard ON moi.elob_simcard = ifsicard.elob_ifsitemId
			LEFT JOIN elob_ifsitemBase ifsiterm ON moi.elob_terminalmodel = ifsiterm.elob_ifsitemId
			LEFT JOIN StringMapBase mapmosimtypact ON moi.elob_simcardtypeactivation=mapmosimtypact.Attributevalue AND mapmosimtypact.[langid]=1033 AND mapmosimtypact.ObjectTypeCode=10035 AND mapmosimtypact.AttributeName='elob_simcardtypeactivation'
			LEFT JOIN OpportunityBase o ON mo.elob_project=o.OpportunityId
			LEFT JOIN elob_ifsitemBase ifs ON moi.elob_terminalmodel=ifs.elob_ifsitemId
			LEFT JOIN elob_ckpitemBase ckp ON ifs.elob_ckpitemid=ckp.elob_ckpitemId
		WHERE 1=1 
		AND c.elob_revisiondate >='"""+ params['date_from'] +"""'
        AND c.elob_revisiondate <'"""+ params['date_to'] +"""' 
        """+where+"""
        """
                
                #print(query)
            
                cursor.execute(query)    
                results = cursor.fetchall()

                data = []
                # Define keys for the columns
                keys = [column[0] for column in cursor.description]
                
                # Iterate through the result and map values to keys
                for i, row in enumerate(results):
                    # Create a dictionary for each row
                    row_dict = {keys[j]: row[j] for j in range(len(row))}
                    
                    # Append the dictionary to the data list
                    data.append(row_dict)
                
                # Print the formatted data
                print(data)            
                
                return ({"data": data})
        
        except Exception as e:
            return {'error': str(e)}
        finally:
            conn.close()