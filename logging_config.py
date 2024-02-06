import logging

def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Check if the logger already has handlers, there should not be more than 1 handler
    if not logger.handlers:
        file_handler = logging.FileHandler('app.log', mode='w')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                           '%d-%m-%Y %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                              '%d-%m-%Y %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger
