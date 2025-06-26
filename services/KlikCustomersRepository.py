import cx_Oracle
import config.config  as config
from sqlalchemy import create_engine, text

class KlikCustomersRepository:
     def __init__(self):
        self.username = config.ORACLE_DEV_LOGIN
        self.password = config.ORACLE_DEV_PASSWORD
        self.host = config.ORACLE_DEV_HOST
        self.port = config.ORACLE_DEV_PORT
        self.sid = config.ORACLE_DEV_SID
        self.connection = None

     def validate(self, params):
         required_params = ['id_sow']  
         
         for param in required_params:
                if param not in params:
                    return False, f"Missing required parameter: {param}"

         id_sow = params.get('id_sow')

         return True, None

     def get_data(self, params):
         
        is_valid, error_message = self.validate(params)
        if not is_valid:
            return {'error': error_message}
            
        try: 
                oracle_engine = create_engine(f'oracle+cx_oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.sid}')
                self.connection = oracle_engine.connect()
                id_sow_list = params['id_sow']             
                id_sow_str = ",".join(f"'{str(id)}'" for id in id_sow_list)     
                numbers_list = id_sow_list.split(',')                
                quoted_numbers = [f"'{num.strip()}'" for num in numbers_list]
                id_sow_str = ",".join(quoted_numbers)
            
                query = f""" SELECT distinct *
        FROM 
            (SELECT sektor, segment, channel,
                            region, 
                            zespol ||''|| superior_name ||' '||superior_surname AS zespol, 
                            portfel ||' '||okb_name||' '||okb_surname AS portfel,
                            portfel as portfel_name,
                            gk_name || ' '||gk_flag AS gk,
                            gk_flag,
                            CASE WHEN customer_attribute_val is not null THEN customer_attribute_val ELSE '-' END NAVIO,
                            isa.customer,  isa.nip, isa.id_sow, isa.regon 
                            FROM klik.import_sow_actual isa
                            LEFT JOIN (select distinct customer_id, global_id from navio_cddo.va_customer ) va_customer on isa.nip = va_customer.global_id
                    		LEFT JOIN (select distinct customer_id, customer_attribute_val from  navio_cddo.va_customer_attribute where customer_attribute_type_cd ='NAVIO_CLIENTSOURCESYSTEM')va_customer_attribute on va_customer.customer_id = va_customer_attribute.customer_id
                            LEFT JOIN klik.import_sow_potential pot ON (pot.id_sow = isa.id_sow) 
                            WHERE isa.id_sow in ({id_sow_str})
                            GROUP BY
                                sektor, segment, channel, region,
                                zespol ||''|| superior_name ||' '||superior_surname ,
                                portfel ||' '||okb_name||' '||okb_surname , portfel , gk_name || ' '||gk_flag , gk_flag,
                                customer_attribute_val,
                                isa.customer, isa.nip, isa.id_sow, isa.regon 
                            )"""
                
                result_proxy = self.connection.execute(text(query));

                results = result_proxy.fetchall()           
              
                keys = list(result_proxy.keys())

                data = []                      
                
                # Iterate through the result and map values to keys
                for row in results:
                    row_dict = {keys[i]: row[i] for i in range(len(row))}
                    data.append(row_dict)
        
                
                return ({"data": data})
        
        except Exception as e:
            return {'error': str(e)}
        finally:
            if self.connection:
                    self.connection.close()