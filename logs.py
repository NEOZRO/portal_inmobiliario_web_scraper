import os
import datetime
from datetime import datetime
import traceback


def log_msg(exception, path, url):
    """Para tener un archivo/log que registre los procesos
    :param msg: mensaje que se guardará

    """
    # tb_list = traceback.format_exception(None, exception, exception.__traceback__)
    # tb_extract = tb_list[:1] + tb_list[-3:]
    tracebacks_msg = '\n'.join(traceback.format_exception(None, exception, exception.__traceback__))
    error_msg = (f"\n ------------------------------------------------------------------------------------------------------------------\n"
                 f"||||{url}||||\n"
                 f"-----------------------------------------------------------------------------------------------------------------\n"
                 f"\nError: {exception} \n {tracebacks_msg}")

    msg = "\n \n                [{0}]: {1}\n".format(datetime.strftime(datetime.now(), # NOQA
                                                                         "%d-%m-%Y %H:%M"), error_msg) # NOQA


    with open((os.path.join(path, "log.txt")), "a", encoding="utf-8") as logFile:

        logFile.write(msg)




