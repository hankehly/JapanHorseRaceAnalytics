import os
import structlog
import logging
from structlog.stdlib import LoggerFactory
from structlog.processors import JSONRenderer, TimeStamper, format_exc_info


def setup_structlog(level=logging.INFO):
    logging.basicConfig(format="%(message)s", level=level)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,  # Adds log level
            TimeStamper(fmt="ISO"),  # Adds timestamp
            structlog.stdlib.add_logger_name,  # Adds logger name
            format_exc_info,  # Formats exception info, if any
            JSONRenderer(),  # Renders log as JSON
        ],
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


setup_structlog(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()))

logger = structlog.get_logger()

# Example usage within the same file
# logger.info("Test log message", example_key="example value")
