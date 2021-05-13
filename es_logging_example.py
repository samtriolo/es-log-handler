# Simple example to log a message to both stdout and an ES instance. User needs to populate ES connection vars below.

import logging
import sys

from es_log_handler import ElasticsearchStdlibHandler


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(stdout_formatter)
log.addHandler(stdout_handler)

# *user needs to populate these*
es_endpoint, es_index = '', ''
es_user, es_pw = '', ''

es_handler = ElasticsearchStdlibHandler(**{'es_endpoint': es_endpoint, 'es_index': es_index, 'es_user': es_user, 'es_pw': es_pw})
log.addHandler(es_handler)

log.info('Hello World')