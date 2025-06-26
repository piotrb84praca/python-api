import cx_Oracle
import config.config  as config
from sqlalchemy import create_engine, text

class KlikSaleApprovalsRepository:
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
            
                query = f"""  SELECT
                motion_sale_approvals.motion_id as id,
                 users.surname||' '||users.name as WNIOSKUJACY,
                customers.name as KLIENT,
                -- motion_sale_approvals.author_channel as KANAL,
                to_char(motion_sale_approvals.fromdate,'yyyy-mm-dd') ZGODA_OD, 
                to_char(motion_sale_approvals.todate,'yyyy-mm-dd') ZGODA_DO,
                 motion_sale_approvals.id_sow ID_SOW,
                motion_sale_approvals.process_stage as  ETAP_PROCESU
                FROM
                   klik.v_motion_sale_approvals   motion_sale_approvals
                   join klik.v_customers customers on customers.id_sow =motion_sale_approvals.id_sow and type ='ECO_SUB'
                   join klik.vt_users users on motion_sale_approvals.login = users.login
                WHERE
                    motion_sale_approvals.id_sow in ({id_sow_str}) order by motion_sale_approvals.todate desc"""
                
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