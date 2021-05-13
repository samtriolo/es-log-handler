import logging
import sys

from elasticsearch import Elasticsearch, AuthenticationException, NotFoundError, AuthorizationException, ConflictError
from ecs_logging import StdlibFormatter


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(stdout_formatter)
log.addHandler(stdout_handler)


class ElasticsearchStdlibHandler(logging.Handler):
    """
    A logging handler that sends ECS-formatted logs to Elasticsearch individually, with each log event. As a result,
    this is NOT suitable for high volume logging.

    Notes
    1) make sure we do super init before we set formatter, or you will trigger formatter NoneType exceptions

    """

    def __init__(self, level=logging.NOTSET, **kwargs):

        try:
            es_endpoint, es_user, es_pw, es_index = kwargs['es_endpoint'], kwargs['es_user'], kwargs['es_pw'], kwargs['es_index']
        except KeyError:
            raise ValueError('ES host, index, username, and password are required')

        self.es = Elasticsearch(**{'host': es_endpoint, 'use_ssl': True, 'verify_certs': False, 'ssl_show_warn': False,
                                   'http_auth': (es_user, es_pw), 'cloud_id': kwargs.get('cloud_id'), 'port': 9243})
        self.index = es_index
        self.init_es()

        super().__init__(level=level)
        self.formatter: StdlibFormatter = StdlibFormatter()  # see note 1

        return

    def init_es(self):
        """
        Attempt to connect to ES, and then confirm or create index.

        Notes
        1) It seems like es.indices.exists() above would implicitly check for an unkown host, and if so, return
        elasticsearch.NotFoundError, however it will simply return False in that case, so we need to check for it here
        2) ES index already exists. My understanding is, this should probably only happen if eg two separate python apps
        attempt to create the same index, at the same time, which doesn't already exist.

        :return:
        """

        try:  # this will be our first time actually connecting to ES, so check for various exceptions
            index_exists = self.es.indices.exists(self.index)
        except AuthenticationException:
            log.critical('ES Authn Failed! We cannot log any events to ES (check your creds)!')
            self.es = None
        except AuthorizationException:
            log.critical('ES unauthorized action! We cannot log any events to ES! ES log handler requires the following '
                         'index permissions: read, write, create_index, view_index_metadata')
            self.es = None
        except Exception as e:
            log.critical(f'Unhandled ES exception! We will not log any events to ES! Additional info: {e}')
            self.es = None
        else:
            if not index_exists:  # either the index needs to be created, or the host is unreachable (see note 1)
                try:
                    self.es.indices.create(index=self.index)
                except AuthorizationException:
                    log.critical('ES unauthorized action! We cannot log any events to ES! ES log handler requires the following '
                                 'index permissions: read, write, create_index, view_index_metadata')
                    self.es = None
                except NotFoundError:  # see note 1
                    log.critical('ES Host Not Found (check ES hostname)! We will not log any events to ES!')
                    self.es = None
                except ConflictError:  # see note 2
                    log.warning('ES index already exists.')
                except Exception as e:
                    log.critical(f'Unhandled ES exception! We will not log any events to ES! Additional info: {e}')
                    self.es = None

        return

    def emit(self, record: logging.LogRecord) -> None:
        """
        Write log to es instance.
        """

        if self.es is not None:
            document = self.formatter.format_to_ecs(record)

            try:
                self.es.index(self.index, body=document)
            except Exception as e:
                log.critical(f'Unhandled exception while attempting to log this event to ES! Additional info: {e}')

        return

    def flush(self) -> None:
        """
        Multiple references for logging.Handler or related mention flush(), so I'm including it to avoid exceptions if
        this method is called.
        """
        return

    def close(self) -> None:
        """
        Multiple references for logging.Handler or related mention close(), so I'm including it to avoid exceptions if
        this method is called.

        Notes
        1) Not sure what ultimately causes this error ('Elasticsearch' object has no attribute 'close'), which is
        technically inaccurate.
        """

        if self.es is not None:
            try:
                self.es.close()  # close es connection
            except AttributeError:  # see note 1
                pass

            self.es = None

        return
