#%% Template

import os
import dlogging
from demail.gmail import SendEmail


package_name = os.path.basename(__file__)
logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


try:

        import main

        logger.info('Done, No Problems!')

except Exception as e:
    e = str(e)
    logger.critical(f'{e}\n', exc_info=True)
    to_email_addresses = os.getenv('email_fail')
    SendEmail(to_email_addresses=to_email_addresses
                        , subject=f'Python Error - {package_name}'
                        , body=e
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('email_uid')
                        , password=os.getenv('email_pwd')
                        )