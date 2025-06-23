import pymssql
from datetime import datetime
import config.config  as config

class FortMobileRepository:
     def __init__(self):
        pass

     def validate(self, params):
        
        required_params = ['date_from','date_to']  
        optional_params = ['um_number', 'ord_number','ident_number']
         
        for param in required_params:
            if param not in params:
                return False, f"Missing required parameter: {param}"
            if not (params.get('um_number') or params.get('ord_number') or params.get('ident_number')):
                return False, "At least one of 'um_number' or 'ord_number' or 'ident_number' is required."
        try:
            date_from = datetime.strptime(params.get('date_from'), '%Y-%m-%d')
            date_to = datetime.strptime(params.get('date_to'), '%Y-%m-%d')
        except ValueError:
            return False, "Invalid date format. Expected YYYY-MM-DD for 'date_from' and 'date_to'."

       
        # Validate that the 'um_number' starts with 'um' and is 16 characters long
       # um_number = params.get('um_number')
       # if not (um_number.startswith('UM') and len(um_number) == 16):
        #    return False, "Invalid UM number. It must start with 'UM' and be 16 characters long."
        
        return True, None
         
     def get_data(self, params):
         
        is_valid, error_message = self.validate(params)
        if not is_valid:
            return {'error': error_message}
            
        try:                         

                # Assign to param or set to empty string
                if params.get('um_number') is not None:
                    um_number = params.get('um_number')
                else:
                    um_number = ''
                
                if params.get('ord_number') is not None:
                    ord_number =  params.get('ord_number')
                else:
                    ord_number = ''

                if params.get('ident_number') is not None:
                    ident_number = params.get('ident_number')
                else:
                    ident_number = ''
            
                conn = pymssql.connect(host=config.MSSQLHOST,
                                       user=config.MSSQLLOGIN,
                                       password=config.MSSQLPASSWORD,
                                       charset='UTF-8', database=config.MSSQLDB, tds_version=r'7.0')  
    
                cursor = conn.cursor()                
            
                query = f"""SELECT CAST( GETDATE() AS Date ) AS date_generated
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
                , [(zam) Nr zamówienia mob]
                , [(zam) Status zamówienia (delivery/activation)]
                , [(zam) Typ zamówienia mob]
                , [(zam) Zamówienie easy]
                , [(zam) Activation Type]
                , [(zam) Data aktywacji]
                , [(zam) Data utworzenia zamówienia mob]
                , [(zam) Sprzedawca]
                , [(zam) Id handlowca]
                , [(zam) Flexi active]
                , [(zam) Kanał Sprzedaży]
                , [(zam) MNP]
                , [(zam) NowaAktywacja_KartaSIMSprzet]
                , [(zam) NowaAktywacja_PozniejszaWysylkaSprzetu]
                , [(zam) NowaAktywacja_TylkoKartaSIM]
                , [(zam) Nr CDT]
                , [(zam) Powód anulowania]
                , [(zam) SprzeWypos_NowaAktywacjaBnSOpoznionaDostawa]
                , [(zam) SprzeWypos_SamTerminal]
                , [(zam) SprzeWypos_UtrzymanieBnSOpoznionaDostawa]
                , [(zam) Utrzym_DokumentyZWyposazeniem]
                , [(zam) Utrzym_PozniejszaWysylkaSprzetuZBnSUtrz]
                , [(zam) Utrzym_TylkoDokumenty]
                , [(zam) Właściciel zamówienia mob id/bscs]
                , [(zam) Właściciel zamówienia mob]
                , [(zam) Nr zlecenia dostawy]
                , [(zam) Zlecenie dostawy glowne - dystrybucja]
                , [(zam) Zlecenie dostawy glowne - sposob dostawy]
                , [(zam) Dostawa - status delivery]
                , [(zam) Dostawa ost data modyfikacji (dostarczenia)]
                , [(zam) Dostawa - link do śledzenia]
                , [(zam) Autorzy zamłączników]
                , [(P&L) % marży]
                , [(P&L) Lojalki]
                , [(P&L) Nr P&L]
                , [(P&L) Zobowiązania]
                , [Mobille|BOX_1|Nowy_BOX|abo] AS [(P&L) abonament SIM]
                , [Mobile|NBOX_1|zobow_numeru|wartosc] AS [(P&L) zobowiązanie SIM]
                , [MOBILE|KRYTERIA|NOWE_K2|eVLR_MV_wartosc] AS [(P&L) eVLR MV]
                , [MOBILE|KRYTERIA|NOWE_K2|eVLR_MBB_wartosc] AS [(P&L) eVLR MBB]
                		FROM (
                			SELECT pt.sfa_pil_id, pt.field_code, pt.field_value, plo.*
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
                , mo.elob_mobileordernumber AS [(zam) Nr zamówienia mob]
                , mapmostatu.value  AS [(zam) Status zamówienia (delivery/activation)]
                , mapmotyp.value  AS [(zam) Typ zamówienia mob]
                , so.name AS [(zam) Zamówienie easy]
                , mapmoacttype.value  AS [(zam) Activation Type]
                , mo.elob_activationdate AS [(zam) Data aktywacji]
                , mo.CreatedOn AS [(zam) Data utworzenia zamówienia mob]
                , sus.fullname AS [(zam) Sprzedawca]
                , sus.elob_salespersonid AS [(zam) Id handlowca]
                , mo.elob_flexiactive  AS [(zam) Flexi active]
                , mapmoseg.value  AS [(zam) Kanał Sprzedaży]
                , mo.elob_mnp  AS [(zam) MNP]
                , mo.elob_isactivationsimcardequipment  AS [(zam) NowaAktywacja_KartaSIMSprzet]
                , mo.elob_isactivationbntequipmentdelayed  AS [(zam) NowaAktywacja_PozniejszaWysylkaSprzetu]
                , mo.elob_isactivationsimcardonly  AS [(zam) NowaAktywacja_TylkoKartaSIM]
                , mo.elob_dtnumber  AS [(zam) Nr CDT]
                , mo.elob_cancelreason  AS [(zam) Powód anulowania]
                , mo.elob_issalesequipmentonlyactivation  AS [(zam) SprzeWypos_NowaAktywacjaBnSOpoznionaDostawa]
                , mo.elob_issalesequipmentonly  AS [(zam) SprzeWypos_SamTerminal]
                , mo.elob_issalesequipmentonlydelayedretention  AS [(zam) SprzeWypos_UtrzymanieBnSOpoznionaDostawa]
                , mo.elob_isretentiondocumentswithequipment  AS [(zam) Utrzym_DokumentyZWyposazeniem]
                , mo.elob_isretentionbntequipmentdelayed  AS [(zam) Utrzym_PozniejszaWysylkaSprzetuZBnSUtrz]
                , mo.elob_isretentiondocumentsonly AS [(zam) Utrzym_TylkoDokumenty]
                , sumoow.elob_salespersonid AS [(zam) Właściciel zamówienia mob id/bscs]
                , sumoow.fullname AS [(zam) Właściciel zamówienia mob]
                , dr.elob_name AS [(zam) Nr zlecenia dostawy]
                , mapdrshco.Value AS [(zam) Zlecenie dostawy glowne - dystrybucja]
                , mapdrdepr.value AS [(zam) Zlecenie dostawy glowne - sposob dostawy]
                , mapdstatu.value AS [(zam) Dostawa - status delivery]
                , d.ModifiedOn AS [(zam) Dostawa ost data modyfikacji (dostarczenia)]
                , wb.elob_trackinglink AS [(zam) Dostawa - link do śledzenia]
                , att.CreatedByName AS [(zam) Autorzy zamłączników]
                , pls.elob_mobile_ContributionMargin AS [(P&L) % marży]
                , pls.elob_mobile_Loyalty AS [(P&L) Lojalki]
                , ts.sfa_pil_id AS [(P&L) Nr P&L]
                , pls.elob_mobile_CommittedRevenuekPLN AS [(P&L) Zobowiązania]
                , plo.elob_PL_Id
                FROM contractbase c
                	JOIN accountbase a ON c.CustomerId=a.AccountId
                	JOIN SystemUserBase sua ON a.ownerid=sua.SystemUserId --właściciel klienta
                	INNER JOIN elob_mobileorderBase mo ON c.ContractId=mo.elob_contract -- 4554
                	LEFT JOIN salesorder so ON so.SalesOrderId=mo.elob_order
                	LEFT JOIN OpportunityBase o ON mo.elob_project=o.OpportunityId
                	LEFT JOIN elob_contractarchive ca ON c.ContractId=ca.elob_Contract AND ca.elob_ArchivingDate IS NOT NULL 
                	LEFT JOIN elob_deliveryrequestBase dr ON mo.elob_mobileorderId=dr.elob_mobileorderid and elob_ismain=1 --branie info z głównego
                	LEFT JOIN elob_deliverybase d ON dr.elob_deliveryrequestid=d.elob_deliveryrequestid
                    LEFT JOIN elob_waybill wb ON wb.elob_deliveryid=d.elob_deliveryid
                	LEFT JOIN SystemUserBase suc ON c.OwnerId=suc.SystemUserId -- wlasciciel umowy
                	LEFT JOIN SystemUserBase sumoow ON mo.OwnerId=sumoow.SystemUserId -- wlasciciel zamowienia easy
                	LEFT JOIN SystemUserBase sub ON so.OwnerId=sub.SystemUserId -- wlasciciel zamowienia easy
                	LEFT JOIN SystemUserBase suo ON o.OwnerId=suo.SystemUserId --wlasciciel projektu
                	LEFT JOIN SystemUserBase succr ON c.CreatedBy=succr.SystemUserId --twórca umowy
                	LEFT JOIN SystemUserBase sus ON mo.elob_salesperson=sus.SystemUserId -- sprzedawca
                	LEFT JOIN StringMapBase mapmostatu ON mo.statuscode=mapmostatu.Attributevalue AND mapmostatu.[langid]=1045 AND mapmostatu.ObjectTypeCode=10034 AND mapmostatu.AttributeName='statuscode'
                	LEFT JOIN StringMapBase mapmoacttype ON mo.elob_activationtype=mapmoacttype.AttributeValue AND mapmoacttype.ObjectTypeCode='10034' AND mapmoacttype.[langid]='1033' AND mapmoacttype.AttributeName='elob_activationtype'
                	LEFT JOIN StringMapBase mapcstatu ON c.statuscode=mapcstatu.Attributevalue AND mapcstatu.[langid]=1045 AND mapcstatu.ObjectTypeCode=1010 AND mapcstatu.AttributeName='statuscode'
                    LEFT JOIN StringMapBase mapcratetyp ON c.elob_contractratetype=mapcratetyp.Attributevalue AND mapcratetyp.[langid]=1045 AND mapcratetyp.ObjectTypeCode=1010 AND mapcratetyp.AttributeName='elob_contractratetype'
                	LEFT JOIN StringMapBase mapmoseg ON mo.elob_salespersonsegment=mapmoseg.Attributevalue AND mapmoseg.[langid]=1033 AND mapmoseg.ObjectTypeCode=10034 AND mapmoseg.AttributeName='elob_salespersonsegment'
                	LEFT JOIN StringMapBase mapmotyp ON mo.elob_mobileordertype=mapmotyp.Attributevalue AND mapmotyp.[langid]=1033 AND mapmotyp.ObjectTypeCode=10034 AND mapmotyp.AttributeName='elob_mobileordertype'
                    LEFT JOIN StringMapBase mapdrshco on dr.elob_shippingcondition=mapdrshco.AttributeValue and mapdrshco.ObjectTypeCode=10211 and mapdrshco.[LangId]=1045 and mapdrshco.AttributeName='elob_shippingcondition'
                    LEFT JOIN StringMapBase mapdrdepr ON dr.elob_deliverypriority=mapdrdepr.AttributeValue and mapdrdepr.ObjectTypeCode=10211 and mapdrdepr.[LangId]=1045 and mapdrdepr.AttributeName='elob_deliverypriority'
                    LEFT JOIN StringMapBase mapdstatu ON d.statuscode=mapdstatu.attributevalue and mapdstatu.objecttypecode=10224 and mapdstatu.[langid]=1045 and mapdstatu.attributename='statuscode'
                	LEFT JOIN elob_plbase plo ON o.OpportunityId=plo.elob_Project AND plo.elob_FinalPL=1 AND plo.elob_PLType=2 -- 4555
                 	LEFT JOIN FORT_Produkcja_V2.dbo.piltool_miscellaneous_field ts ON plo.elob_PL_Id COLLATE Polish_CI_AS=ts.sfa_pil_id AND ts.field_code IN ('MOBILE_M2M|TOTAL|CF|TOTAL_CF_ZL_DISCOUNTED_CASH_FLOW_DPV') -- 4555
                	LEFT JOIN elob_plresultBase pls ON plo.elob_PLstatistic=pls.elob_plresultId--stat 
                	LEFT JOIN elob_plbase plc ON c.elob_plnumber=plc.ActivityId
                	CROSS APPLY (
                		SELECT 
                			STUFF((
                				SELECT DISTINCT ', ' + att.CreatedByName
                				FROM elob_attachment att
                				WHERE att.elob_mobileorder = mo.elob_mobileorderId
                				FOR XML PATH(''), TYPE
                			).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS CreatedByName
                	) att    
                
                
                WHERE 1=1 
               /* AND c.elob_revisiondate >='{datetime.strptime(params.get('date_from'), '%Y-%m-%d')}' AND c.elob_revisiondate <'{datetime.strptime(params.get('date_to'), '%Y-%m-%d')}'*/
                AND (c.ContractNumber='{um_number}' OR mo.elob_mobileordernumber='{ord_number}' OR ts.sfa_pil_id='{ident_number}') 
                
                GROUP BY 
                 c.ContractNumber
                , mapcstatu.value 
                , c.elob_durationtimeinmonths
                , ca.elob_ArchivingDate
                , c.CreatedOn
                , c.elob_revisiondate
                , att.CreatedByName
                , a.name
                , a.elob_NIP
                , plc.elob_PL_Id
                , c.elob_attachedplnumber
                , a.elob_regon
                , mapcratetyp.value
                , succr.elob_salespersonid
                , succr.fullname
                , suc.elob_salespersonid
                , suc.fullname
                , o.elob_OpportunityID
                , plo.elob_PL_Id
                , suo.elob_salespersonid
                , suo.fullname
                , mo.elob_mobileordernumber
                , mapmostatu.value
                , mapmotyp.value
                , so.name
                , mapmoacttype.value
                , mo.elob_activationdate
                , mo.CreatedOn
                , sus.fullname
                , sus.elob_salespersonid
                , mo.elob_flexiactive
                , mapmoseg.value
                , mo.elob_mnp
                , mo.elob_isactivationsimcardequipment
                , mo.elob_isactivationbntequipmentdelayed
                , mo.elob_isactivationsimcardonly
                , mo.elob_dtnumber
                , mo.elob_cancelreason
                , mo.elob_issalesequipmentonlyactivation
                , mo.elob_issalesequipmentonly
                , mo.elob_issalesequipmentonlydelayedretention
                , mo.elob_isretentiondocumentswithequipment
                , mo.elob_isretentionbntequipmentdelayed
                , mo.elob_isretentiondocumentsonly
                , sumoow.elob_salespersonid
                , sumoow.fullname
                , dr.elob_name
                , mapdrshco.Value
                , mapdrdepr.value
                , mapdstatu.value
                , d.ModifiedOn
                , wb.elob_trackinglink
                , pls.elob_mobile_ContributionMargin
                , pls.elob_mobile_Loyalty
                , ts.sfa_pil_id
                , pls.elob_mobile_CommittedRevenuekPLN
                
                		-- , plo.elob_PL_Id
                			) plo
                			LEFT JOIN FORT_Produkcja_V2.dbo.piltool_miscellaneous_field pt ON plo.elob_PL_Id COLLATE Polish_CI_AS=pt.sfa_pil_id  
                			AND pt.field_code IN('Mobille|BOX_1|Nowy_BOX|abo','Mobile|NBOX_1|zobow_numeru|wartosc','MOBILE|KRYTERIA|NOWE_K2|eVLR_MV_wartosc','MOBILE|KRYTERIA|NOWE_K2|eVLR_MBB_wartosc')
                		) AS pt
                		PIVOT (
                			MIN(pt.field_value)
                			FOR pt.field_code IN ([Mobille|BOX_1|Nowy_BOX|abo], [Mobile|NBOX_1|zobow_numeru|wartosc], [MOBILE|KRYTERIA|NOWE_K2|eVLR_MV_wartosc], [MOBILE|KRYTERIA|NOWE_K2|eVLR_MBB_wartosc])
                		) AS pt
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