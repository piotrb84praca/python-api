from services.FortFixRepository import FortFixRepository
from services.FortMobileRepository import FortMobileRepository
from services.FortMsisdnRepository import FortMsisdnRepository
from services.LmeMobKwsRepository import LmeMobKwsRepository
from services.LmeMobMsisdnRepository import LmeMobMsisdnRepository
from services.LmeMobElasticRepository import LmeMobElasticRepository
from services.LmeMobTboRepository import LmeMobTboRepository
from services.MssCrmFixProdRepository import MssCrmFixProdRepository
from services.MssCrmFixFacOrderRepository import MssCrmFixFacOrderRepository
from services.MssCrmFixOrderLinesRepository import MssCrmFixOrderLinesRepository
from services.MssFiberRepository import MssFiberRepository
from services.MssOrderProtectionRepository import MssOrderProtectionRepository
from services.MssPotentialRepository import MssPotentialRepository
from services.WlSOKxRepository import WlSOKxRepository
from services.GoOneOffRepository import GoOneOffRepository
from services.FileUpdatesRepository import FileUpdatesRepository

#from services.KlikCustomersRepository import KlikCustomersRepository
#from services.KlikSaleApprovalsRepository import KlikSaleApprovalsRepository
#from services.TestRepository import TestRepository

class ServiceFactory:
    @staticmethod
    def getService(service_name):
        if service_name == 'Fix':
            return FortFixRepository()
        elif service_name == 'Mobile':
            return FortMobileRepository() 
        elif service_name == 'Msisdn':
            return FortMsisdnRepository()     
        elif service_name == 'LmeMobKws':
            return LmeMobKwsRepository()        
        elif service_name == 'LmeMobMsisdn':
            return LmeMobMsisdnRepository()    
        elif service_name == 'LmeMobElastic':
            return LmeMobElasticRepository()     
        elif service_name == 'LmeMobTbo':
            return LmeMobTboRepository() 
        elif service_name == 'MssCrmFixProd':
            return MssCrmFixProdRepository()    
        elif service_name == 'MssCrmFixFacOrder':
            return MssCrmFixFacOrderRepository() 
        elif service_name == 'MssCrmFixOrderLines':
            return MssCrmFixOrderLinesRepository() 
        elif service_name == 'MssFiber':
            return MssFiberRepository()
        elif service_name == 'MssOrderProtection':
            return MssOrderProtectionRepository()
        elif service_name == 'MssPotential':
            return MssPotentialRepository()
        elif service_name == 'WlSOKx':
            return WlSOKxRepository()
        elif service_name == 'GoOneOff':
            return GoOneOffRepository()     
            
        elif service_name == 'FileUpdatesTime':
            return FileUpdatesRepository()                
       #\ elif service_name == 'Test':
       #     return TestRepository()
       # elif service_name == 'KlikCustomers':
       #     return KlikCustomersRepository()   
       # elif service_name == 'KlikSaleApprovals':
       #     return KlikSaleApprovalsRepository()     
        else:
            return None
            