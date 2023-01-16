from typing import Optional, Tuple
import requests
import argparse
import subprocess
import logging
import re
import tempfile
from .logging_setup import setup_logging
from .nbadapter import find_notebooks

logger = logging.getLogger(__name__)

import contextlib, os

@contextlib.contextmanager
def remember_cwd(chdir):
    curdir= os.getcwd()
    try:
        os.chdir(chdir)
        yield
    finally: os.chdir(curdir)



def patch_add_tests() -> Optional[str]:
    # TODO: extract this F(repo) to workflow database    

    patch_summary = []

    found_tests = []
    found_not_tests = []
    for nb_name, nb in find_notebooks(".").items():
        logger.info("found notebook %s: %s", nb_name, nb)
        if nb_name.startswith("test_"):
            found_tests.append(nb)
        else:
            found_not_tests.append(nb)

    new_files = []

    if len(found_tests) == 0:
        import nbformat as nbf

        for nb in found_not_tests:
            new_nb = nbf.v4.new_notebook()

            text = f"""\
# This is an auto-generated test for notebook {nb.name}
            """

            code = f"""    
from nb2workflow import nbadapter

nba = nbadapter.NotebookAdapter("{nb.name}.ipynb")
nba.execute(dict(
    ),
    log_output=True,
    progress_bar=False)
output = nba.extract_output()

for k in output.keys():
    if '_content' in k:
        output[k]=""

print(output)

# assert output
            """

            new_nb['cells'] = [nbf.v4.new_markdown_cell(text),
                        nbf.v4.new_code_cell(code) ]

            
            fn = f'test_{nb.name}.ipynb'
            nbf.write(new_nb, fn)
            new_files.append(fn)
   

    if len(new_files) == 0:
        return
    else:
        return f"proposing {len(new_files)} new tests", new_files



def patch_normalized_uris() -> Optional[Tuple[str, list]]:
    # TODO: extract this F(repo) to workflow database    

    patch_summary = []

    for nb_name, nb in find_notebooks(".").items():
        logger.info("found notebook %s: %s", nb_name, nb)

        # we just want to change all mentions, not only in types
        text = open(nb.notebook_fn, "rt").read()
        uri_re = re.compile(r"(https?://odahub.io/ontology[#/]+)([a-zA-Z0-9_/]*)")
        for uri_base, uri_remainder in uri_re.findall(text):
            uri_found = uri_base + uri_remainder
            uri_normalized = "http://odahub.io/ontology#" + uri_remainder            
            logger.info("found: %s, normalized %s", uri_found, uri_normalized)
            if uri_found != uri_normalized:
                logger.info("replacing %s with normalized %s", uri_found, uri_normalized)

                text = text.replace(uri_found, uri_normalized)
                patch_summary.append(f"normalized odahub URI: {uri_found} => {uri_normalized}")
                
        with open(nb.notebook_fn, "wt") as f:
            f.write(text)

    if len(patch_summary) == 0:
        return
    else:
        return f"patched {len(patch_summary)} odahub URIs", []

def validate(repository, patch, gitlab_project=None) -> Optional[str]:
    logger.info('will validiate %s', repository)

    validation_result = None

    with tempfile.TemporaryDirectory(suffix="nb2w-validate") as td:
        logger.info("cloning to %s", td)
        subprocess.check_call(["git", "clone", repository, td])
    
        with remember_cwd(td):
            branch = f"nb2w-validate-" + patch.__name__
            subprocess.check_call(["git", "checkout", "-b", branch])
            m = patch()

            if m is None:
                logging.info("validation proposes no patches!")
            else:
                m, new_files = m

                subprocess.check_call(["git", "add"] + new_files)
                subprocess.check_call(["git", "diff"])
                subprocess.check_call(["git", "status"])
                subprocess.check_call(["git", "commit", "-a", "-m", m])

                try:
                    subprocess.check_call(["git", "push", "origin", branch])
                except subprocess.CalledProcessError:
                    logger.warning("unable to propose validation")
                    # validation_result = ("validation found that something should be changed but is unable to propose what:"
                    #                      " please revise the existing branches, there is likely already some change proposed.")
                    validation_result = ""
                else:
                    logger.info("done!")

                if gitlab_project is not None:
                    r = requests.post(
                        gitlab_project['_links']['merge_requests'],
                        headers={'PRIVATE-TOKEN': subprocess.check_output(['pass', 'renkulab-gitlab']).decode().strip()},
                        params={
                            "title": f"ODA Bot proposes: {m}",
                            "source_branch": branch,
                            "target_branch": gitlab_project["default_branch"]
                        }
                    )
                    print(r.text)
            
                    try:
                        validation_result = f"Validation proposes Merge Request: {r.json()['web_url']}"
                    except:
                        validation_result = ""
                else:
                    validation_result = f"Validation proposes to merge branch {branch}"

    return validation_result
        


        
def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('--debug', action="store_true", default=False)
    parser.add_argument('--namespace', metavar='namespace', type=str, default="oda-staging")
    parser.add_argument('--local', action="store_true", default=False)
    
    args = parser.parse_args()

    setup_logging(args.debug)
    
    validate(args.repository, patch_add_tests)
    validate(args.repository, patch_normalized_uris)


if __name__ == "__main__":
    main()
