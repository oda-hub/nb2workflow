import logging
import os

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None
except Exception as e:
    logging.debug(f"an issue occurred while importing sentry: {repr(e)}")
    sentry_sdk = None


class Sentry:
    def __init__(self) -> None:
        self.logger = logging.getLogger(repr(self))

    @property
    def sentry_url(self):
        if not hasattr(self, '_sentry_url'):
            self._sentry_url = os.environ.get('SENTRY_URL', "https://63ae106793010d836c74830fa75b300c@o264756.ingest.sentry.io/4506186624335872")
        return self._sentry_url

    @property
    def have_sentry(self):
        if self.sentry_url is None or self.sentry_url == '' or sentry_sdk is None:
            return False
        else:
            try:
                sentry_sdk.init(dsn=self._sentry_url)
            except Exception as ex:
                self.logger.warning(f"can not setup sentry with URL {self.sentry_url} due to {repr(ex)}")

            return True

    def capture_message(self, message: str):
        if self.have_sentry:
            self.logger.warning(message)
            sentry_sdk.capture_message(message)
        else:
            self.logger.warning("sentry not used, dropping %s", message)

    def capture_exception(self, exception):
        if self.have_sentry:
            self.logger.warning(repr(exception))
            sentry_sdk.capture_exception(exception)
        else:
            self.logger.warning("sentry not used, dropping %s", repr(exception))


sentry = Sentry()
