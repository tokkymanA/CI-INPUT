import logging
import sys

class ErrorLogFile:
    def __init__(self, log_file='error_log.txt'):
        """
        Initialize the logger with a specific log file.
        """
        self.log_file = log_file
        self.setup_logger()

    def setup_logger(self):
        """
        Configure the logging settings.
        """
        logging.basicConfig(
            filename=self.log_file,
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # Set custom exception hook for unhandled exceptions
        sys.excepthook = self.log_unhandled_exception

    @staticmethod
    def log_error(message):
        """
        Log an error message.
        """
        logging.error(message)

    @staticmethod
    def log_unhandled_exception(exc_type, exc_value, exc_traceback):
        """
        Log unhandled exceptions automatically.
        """
        logging.error("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))