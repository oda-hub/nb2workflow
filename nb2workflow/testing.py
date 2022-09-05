import argparse
import logging
from .nbadapter import find_notebooks, NotebookAdapter, setup_logging, validate_oda_dispatcher

logger = logging.getLogger(__name__)

def nbtest(source: str):
    logger.info('searching for tests in %s', source)
    
    for nba in find_notebooks(source).values():
        if nba.name.startswith('test_'):
            logger.info('found test notebook %s', nba)

            output = nba.execute(
                dict(location="."),
                log_output=True,
                progress_bar=False
            )
            


def main():
    parser = argparse.ArgumentParser(description='Test some notebooks') # run locally, remotely, semantically
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('--debug', action="store_true")
        
    args = parser.parse_args()

    setup_logging(args.debug)

    nbtest(args.repository)


if __name__ == "__main__":
    main()

