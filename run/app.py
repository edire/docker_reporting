#%% Template

import os
import dlogging
from demail.gmail import SendEmail


logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


try:

        import main

        logger.info('Done, No Problems!')


except Exception as e:
    e = str(e)
    logger.critical(f'{e}\n', exc_info=True)
    SendEmail(to_email_addresses=os.getenv('EMAIL_FAIL')
                        , subject=f'Error - MM Daily Dash'
                        , body=e
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('EMAIL_UID')
                        , password=os.getenv('EMAIL_PWD')
                        )