import pymssql
from datetime import datetime
import config.config  as config

class FortFixRepository:
     def __init__(self):
        pass

     def validate(self, params):
         
        required_params = ['date_from','date_to']       
         
        for param in required_params:
            if param not in params:
                return False, f"Missing required parameter: {param}"
        
        try:
            date_from = datetime.strptime(params.get('date_from'), '%Y-%m-%d')
            date_to = datetime.strptime(params.get('date_to'), '%Y-%m-%d')
        except ValueError:
            return False, "Invalid date format. Expected YYYY-MM-DD for 'date_from' and 'date_to'."       
        
         #Validate optional params um_number / ord_number and set data binding 
         
        return True, None
         
     def get_data(self, params):       

        um_number = params.get('um_number')
        ord_number =params.get('ord_number')
        
        if params.get('um_number') is None:
            um_number  = ""
        if params.get('ord_number') is None:
            ord_number = ""
            
        is_valid, error_message = self.validate(params)
        if not is_valid:
            return {'error': error_message}
            
        try:                         
                conn = pymssql.connect(host=config.MSSQLHOST,
                                       user=config.MSSQLLOGIN,
                                       password=config.MSSQLPASSWORD,
                                       charset='UTF-8', database=config.MSSQLDB, tds_version=r'7.0')  
    
                cursor = conn.cursor()                
            
                query = f"""
                    SELECT CAST( GETDATE() AS Date ) AS date_generated
                    , [(umo) Nr/nazwa umowy]
                    , [(umo) Stan umowy]
                    , [(umo) Czas trwania w miesiącach]
                    , [(umo) Data archiwizacji umowy]
                    , [(umo) Data utworzenia umowy]
                    , [(umo) Data weryfikacji umowy]
                    , [(umo) Klient]
                    , [(umo) NIP]
                    , [(umo) Nr dołączonego P&La/Bcasea]
                    , [(umo) REGON]
                    , [(umo) Stawki na umowie]
                    , [(umo) Umowa utworzona przez id/bscs]
                    , [(umo) Umowa utworzona przez]
                    , [(umo) Właściciel umowy id/bscs]
                    , [(umo) Właściciel umowy]
                    , [(Pro) Nr projektu]
                    , [(Pro) Nr P&L ostateczny]
                    , [(Pro) Właściciel projektu id/bscs]
                    , [(Pro) Właściciel projektu]
                    , [(zam) Nr zamówienia fix]
                    , [(zam) Stan zamówienia]
                    , [(zam) ID w syst CRM]
                    , [(zam) Data utworzenia zamówienia fix]
                    , [(zam) Sprzedawca]
                    , [(zam) Id handlowca]
                    , [(zam) DB]
                    , [(zam) DB id/bscs]
                    , [(zam) Lokalizacja]
                    , [(zam) Operacja]
                    , [(zam) Proces]
                    , [(zam) Produkt]
                    , [(zam) Szczegóły (Komentarze)]
                    , [(zam) Szczegóły (Numer usługi)]
                    , [(zam) Właściciel zamówienia fix id/bscs]
                    , [(zam) Właściciel zamówienia fix]
                    , [(zam) Administrator sprzedaży umowy fix]
                    , [(zam) Administrator sprzedaży umowy fix id/bscs]
                    , [(zam) Zlecenia (data utworzenia AU)]
                    , [(zam zlec) Data modyfikacji]
                    , [(zam zlec) Data utworzenia]
                    , [(zam zlec) Data zwrotu]
                    , [(zam zlec) Decyzja]
                    , [(zam zlec) Nr zlecenia Sokx]
                    , [(zam zlec) Stan]
                    , [(zam zlec) Typ zlecenia SOKX]
                    , [(zam par) Czas trwania umowy]
                    , [(zam par) Link do obrazu umowy]
                    , [(zam par) Numer zgody]
                    , [(zam par) Opłata abonamentowa netto - kwota PLN]
                    , [(zam par) Opłata instalacyjna netto- kwota PLN]
                    , [(P&L) Numer P&L]
                    , [(P&L) Marża rozliczeniowa do prowizji]
                    , [(P&L) Max długość kontraktu]
                    , [(P&L) Całkowity przychód z nowego kontraktu]
                    FROM (
                    SELECT 
                     pt.*
                    , [elob_margin_commision] AS [(P&L) Marża rozliczeniowa do prowizji]
                    , [elob_fix_max_contract_lenght] AS [(P&L) Max długość kontraktu]
                    , [elob_fix_total_revenue_new] AS [(P&L) Całkowity przychód z nowego kontraktu]
                     FROM ( 
                    SELECT pls.*
                    , pt.field_code
                    , pt.field_value
                    FROM (
                    SELECT pr.* 
                    , pr.elob_PL_Id_z AS [(P&L) Numer P&L]
                    , [Czas trwania umowy] AS [(zam par) Czas trwania umowy]
                    , [Link do obrazu umowy] AS [(zam par) Link do obrazu umowy]
                    , [Numer zgody] AS [(zam par) Numer zgody]
                    , [Opłata abonamentowa netto - kwota PLN] AS [(zam par) Opłata abonamentowa netto - kwota PLN]
                    , [Opłata instalacyjna netto- kwota PLN] AS [(zam par) Opłata instalacyjna netto- kwota PLN]
                    FROM (  
                    SELECT pr.*
                    , COALESCE(pr.elob_PL_Id,plz.elob_PL_Id) AS elob_PL_Id_z
                    , COALESCE(pr.elob_PLstatistic,plz.elob_PLstatistic) AS elob_PLstatistic_z
                    FROM (
                    SELECT t.*
                    , pr.elob_name
                    , pr.elob_value
                    FROM (
                    SELECT DISTINCT CAST( GETDATE() AS Date ) AS date_generated
                    , c.ContractNumber AS [(umo) Nr/nazwa umowy]
                    , mapcstatu.value AS [(umo) Stan umowy]
                    , c.elob_durationtimeinmonths AS [(umo) Czas trwania w miesiącach]
                    , ca.elob_ArchivingDate+1 AS [(umo) Data archiwizacji umowy]
                    , c.CreatedOn AS [(umo) Data utworzenia umowy]
                    , c.elob_revisiondate AS [(umo) Data weryfikacji umowy]
                    , REPLACE(REPLACE(REPLACE(REPLACE(a.name, CHAR(13)+CHAR(10),' '),CHAR(13),' '),CHAR(10),' '),CHAR(9),' ') AS [(umo) Klient]
                    , a.elob_NIP AS [(umo) NIP]
                    , COALESCE(plc.elob_PL_Id,c.elob_attachedplnumber) AS [(umo) Nr dołączonego P&La/Bcasea]
                    , a.elob_regon AS [(umo) REGON]
                    , mapcratetyp.value AS [(umo) Stawki na umowie]
                    , succr.elob_salespersonid AS [(umo) Umowa utworzona przez id/bscs]
                    , succr.fullname AS [(umo) Umowa utworzona przez]
                    , suc.elob_salespersonid AS [(umo) Właściciel umowy id/bscs]
                    , suc.fullname AS [(umo) Właściciel umowy]
                    , o.elob_OpportunityID AS [(Pro) Nr projektu]
                    , plo.elob_PL_Id AS [(Pro) Nr P&L ostateczny]
                    , suo.elob_salespersonid AS [(Pro) Właściciel projektu id/bscs]
                    , suo.fullname AS [(Pro) Właściciel projektu]
                    , fo.elob_ordernumber AS [(zam) Nr zamówienia fix]
                    , mapfostatu.value AS [(zam) Stan zamówienia]
                    , fo.elob_crmps_order_number AS [(zam) ID w syst CRM]
                    , fo.CreatedOn AS [(zam) Data utworzenia zamówienia fix]
                    , sus.fullname AS [(zam) Sprzedawca]
                    , sus.elob_salespersonid AS [(zam) Id handlowca]
                    , sufodb.fullname AS [(zam) DB]
                    , sufodb.elob_salespersonid AS [(zam) DB id/bscs]
                    , adr.elob_name AS [(zam) Lokalizacja]
                    , mapfoopera.value AS [(zam) Operacja]
                    , ppro.elob_name AS [(zam) Proces]
                    , sp.elob_name AS [(zam) Produkt]
                    , REPLACE(REPLACE(REPLACE(REPLACE(fo.elob_comments, CHAR(13)+ CHAR(10),' '),CHAR(13),' '),CHAR(10),' '),CHAR(9),' ') AS [(zam) Szczegóły (Komentarze)]
                    , REPLACE(REPLACE(REPLACE(REPLACE(fo.elob_servicenumber, CHAR(13)+ CHAR(10),' '),CHAR(13),' '),CHAR(10),' '),CHAR(9),' ') AS [(zam) Szczegóły (Numer usługi)]
                    , sufoow.elob_salespersonid AS [(zam) Właściciel zamówienia fix id/bscs]
                    , sufoow.fullname AS [(zam) Właściciel zamówienia fix]
                    , sufoasu.fullname AS [(zam) Administrator sprzedaży umowy fix]
                    , sufoasu.elob_salespersonid AS [(zam) Administrator sprzedaży umowy fix id/bscs]
                    , wrkordlast.CreatedOn AS [(zam) Zlecenia (data utworzenia AU)]
                    , wrkordlast.ModifiedOn AS [(zam zlec) Data modyfikacji]
                    , wrkordlast.CreatedOn AS [(zam zlec) Data utworzenia]
                    , wrkordlast.elob_returndate AS [(zam zlec) Data zwrotu]
                    , mapwrkordlastdeci.value AS [(zam zlec) Decyzja]
                    , wrkordlast.elob_sokxnumber AS [(zam zlec) Nr zlecenia Sokx]
                    , mapwrkordlaststat.value AS [(zam zlec) Stan]
                    , mapwrkordlasttype.value AS [(zam zlec) Typ zlecenia SOKX]
                    , COALESCE(plc.elob_PL_Id,plo.elob_PL_Id) AS elob_PL_Id
                    , COALESCE(plc.elob_PLstatistic,plo.elob_PLstatistic) AS elob_PLstatistic
                    , fo.elob_sokxorder2Id AS elob_sokxorder2Id
                    FROM contractbase c
                    	JOIN accountbase a ON c.CustomerId=a.AccountId
                    	JOIN SystemUserBase sua ON a.ownerid=sua.SystemUserId --właściciel klienta
                    	INNER JOIN elob_sokxorder2Base fo ON c.ContractId=fo.elob_Contract AND fo.elob_ordernumber IS NOT NULL
                    	LEFT JOIN OpportunityBase o ON fo.elob_project=o.OpportunityId --bezposr z umowa wychodzi mniej projektow (c.contractid=o.elob_contract)
                    	LEFT JOIN elob_contractarchive ca ON c.ContractId=ca.elob_Contract AND ca.elob_ArchivingDate IS NOT NULL 
                    	LEFT JOIN SystemUserBase suc ON c.OwnerId=suc.SystemUserId -- wlasciciel umowy
                    	LEFT JOIN SystemUserBase suo ON o.OwnerId=suo.SystemUserId --wlasciciel projektu
                    	LEFT JOIN SystemUserBase sufodb ON fo.elob_doublebooking = sufodb.SystemUserId -- double booking
                    	LEFT JOIN SystemUserBase sufoow ON fo.OwnerId = sufoow.SystemUserId -- double booking
                    	LEFT JOIN SystemUserBase succr ON c.CreatedBy=succr.SystemUserId --twórca umowy
                    	LEFT JOIN SystemUserBase sus ON fo.elob_salesperson=sus.SystemUserId -- sprzedawca
                        LEFT JOIN SystemUserBase sufoasu ON fo.elob_salesadministrationuser=sufoasu.SystemUserId -- Administrator sprzedaży umowy
                    	LEFT JOIN StringMapBase mapcstatu ON c.statuscode=mapcstatu.Attributevalue AND mapcstatu.[langid]=1045 AND mapcstatu.ObjectTypeCode=1010 AND mapcstatu.AttributeName='statuscode'
                    	LEFT JOIN StringMapBase mapcratetyp ON c.elob_contractratetype=mapcratetyp.Attributevalue AND mapcratetyp.[langid]=1045 AND mapcratetyp.ObjectTypeCode=1010 AND mapcratetyp.AttributeName='elob_contractratetype'
                    	LEFT JOIN StringMapBase mapfostatu ON fo.statuscode=mapfostatu.Attributevalue AND mapfostatu.[langid]=1045 AND mapfostatu.ObjectTypeCode=10069 AND mapfostatu.AttributeName='statuscode'
                    	left join stringmapbase mapfoopera on fo.elob_operation=mapfoopera.AttributeValue and mapfoopera.AttributeName='elob_operation' and mapfoopera.[LangId]=1033 and mapfoopera.ObjectTypeCode=10069
                    	left join stringmapbase mapforemod on fo.elob_Realizationmode=mapforemod.AttributeValue and mapforemod.AttributeName='elob_Realizationmode' and mapforemod.[LangId]=1033 and mapforemod.ObjectTypeCode=10069
                    	left join stringmapbase mapfo on fo.statuscode=mapfo.AttributeValue and mapfo.AttributeName='statuscode' and mapfo.[langid]=1033 and mapfo.ObjectTypeCode=10069
                    	LEFT JOIN elob_plbase plo ON o.OpportunityId=plo.elob_Project AND plo.elob_FinalPL=1 AND plo.elob_PLType=1 -- 4555
                    	left join elob_soldproductBase sp on fo.elob_soldproduct=sp.elob_soldproductId	
                    	left join elob_soldproductBase sp2 on sp.elob_soldproductId=sp2.elob_ParentSoldProduct
                    	left join elob_productprocessBase ppro on fo.elob_process=ppro.elob_productprocessId
                    	left join elob_addressBase adr on fo.elob_location=adr.elob_addressId
                    	left join elob_workorder2Base wrkordlast on fo.elob_LastWorkWorder=wrkordlast.elob_workorder2Id AND SUBSTRING(wrkordlast.elob_sokxnumber,1,7)='AU/PKB/' 
                    	LEFT JOIN StringMapBase mapwrkordlasttype ON wrkordlast.new_SOKXOrderType=mapwrkordlasttype.AttributeValue AND mapwrkordlasttype.AttributeName='new_sokxordertype' AND mapwrkordlasttype.LangId=1045
                    	LEFT JOIN StringMapBase mapwrkordlaststat ON wrkordlast.statuscode=mapwrkordlaststat.AttributeValue AND mapwrkordlaststat.AttributeName='statuscode' AND mapwrkordlaststat.LangId=1045 AND mapwrkordlaststat.ObjectTypeCode=10080
                    	LEFT JOIN StringMapBase mapwrkordlastdeci ON wrkordlast.elob_sokxwodecision=mapwrkordlastdeci.AttributeValue AND mapwrkordlastdeci.AttributeName='elob_sokxwodecision' AND mapwrkordlastdeci.LangId=1045
                    	LEFT JOIN elob_plbase plc ON c.elob_plnumber=plc.ActivityId  
                    	LEFT JOIN elob_plbase plh ON c.elob_attachedplnumber=plh.elob_PL_Id COLLATE Polish_CI_AS
                    WHERE 1=1 
                    	AND (
                    		(c.elob_revisiondate >='{params['date_from']}' AND c.elob_revisiondate <'{params['date_to']}')
                    		OR
                    		(fo.elob_sokxorder2Id IN (SELECT elob_SOKXOrderWorkOrderId FROM elob_workorder2Base WHERE elob_returndate >='{params['date_from']}' AND elob_returndate <'{params['date_to']}' ) )
                    	)
                    AND (c.ContractNumber='{um_number}' OR fo.elob_ordernumber='{ord_number}') 
                    ) t
                    LEFT JOIN elob_parameterBase pr ON t.elob_sokxorder2Id=pr.elob_SOKXOrder AND pr.elob_name IN('Czas trwania umowy', 'Link do obrazu umowy', 'Numer zgody', 'Opłata abonamentowa netto - kwota PLN', 'Opłata instalacyjna netto- kwota PLN')
                    ) pr
                    PIVOT (	MIN(pr.elob_value)
                    	FOR pr.elob_name IN([Czas trwania umowy], [Link do obrazu umowy], [Numer zgody], [Opłata abonamentowa netto - kwota PLN], [Opłata instalacyjna netto- kwota PLN])
                    ) AS pr
                    LEFT JOIN elob_plbase plz ON pr.[Numer zgody]=plz.elob_PL_Id COLLATE Polish_CI_AS
                    ) pr
                    LEFT JOIN elob_plresultBase pls ON pr.elob_PLstatistic_z=pls.elob_plresultId--stat 
                    ) pls
                    LEFT JOIN FORT_Produkcja_V2.dbo.piltool_miscellaneous_field pt ON pls.elob_PL_Id COLLATE Polish_CI_AS=pt.sfa_pil_id 
                    	AND pt.field_code IN('elob_margin_commision','elob_fix_max_contract_lenght','elob_fix_total_revenue_new')
                    ) pt
                    PIVOT (
                        MIN(pt.field_value)
                        FOR pt.field_code IN ([elob_margin_commision],[elob_fix_max_contract_lenght],[elob_fix_total_revenue_new])
                    ) AS pt
                    ) AS pt
                		"""            
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
                #print(data)            
                
                return ({"data": data})
        
        except Exception as e:
            return {'error': str(e)}
        finally:
            conn.close()