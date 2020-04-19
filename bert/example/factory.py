#!/usr/env/bin python

import argparse
import enum
import git
import os
import logging
import shutil
import sys
import tempfile

from bert.example import exceptions as example_exceptions

from urllib.parse import ParseResult, urlparse

logger = logging.getLogger(__name__)

class ProjectName(enum.Enum):
    SimpleProject = 'simple-project'
    TESSExoplanetSearch = 'tess-exoplanet-search'

def capture_options() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--project-name', type=ProjectName)
    parser.add_argument('-r', '--project-repo', type=str,
        help="Github Repository of Project")
    parser.add_argument('-l', '--list-example-projects', action='store_true', default=False,
        help="List available Example Projects")
    parser.add_argument('-d', '--directory', type=str, default='/tmp/{project-name}',
        help="Create the project relative to this directory")
    return parser.parse_args()

def validate_options(options: argparse.Namespace) -> None:
    members: str = ', '.join([value.value for value in ProjectName.__members__.values()])
    if options.list_example_projects:
        logger.info(f'Available Projects: {members}')
        sys.exit(0)

    if options.project_name is None and options.project_repo is None:
        raise example_exceptions.ProjectNameRequiredException(f'Available Projects: {members}')

    if not options.project_repo is None:
        if not options.project_repo.startswith('https://github.com/') and \
            not options.project_repo.startswith('git@github.com'):
            raise example_exceptions.ProjectRepoInvalidFormatException(f'Project Repo[{options.project_repo}] must be github.com reference of `git` or `http`')

    if options.directory == '/tmp/{project-name}':
        if options.project_name is None:
            url_parts: ParseResult = urlparse(options.project_repo)
            render_dir_name: str = url_parts.path.rsplit('/', 1)[-1]
            render_dir: str = f'/tmp/{render_dir_name}'

        else:
            render_dir: str = os.path.join('/tmp', options.project_name.value)

        if os.path.exists(render_dir):
            raise example_exceptions.DirectoryExistsException(f"""
  Directory Already Exists: {render_dir}
  Please delete and bert-example.py again""")
        os.makedirs(render_dir)
        options.directory = render_dir

def clone_repo(options: argparse.Namespace) -> None:
    repo_clone_path: str = tempfile.NamedTemporaryFile().name
    if options.project_name == ProjectName.SimpleProject:
        repo_url: str = 'https://github.com/jbcurtin/bert-etl-simple-project'
        repo_jobs_dir: str = 'simple_project'

    elif options.project_name == ProjectName.TESSExoplanetSearch:
        repo_url: str = 'https://github.com/jbcurtin/bert-etl-tess-exoplanet-search'
        repo_jobs_dir: str = 'simple_project'

    elif options.project_name is None and not options.project_repo is None:
        repo_url: str = options.project_repo

    logger.info(f'Cloning Repo[{repo_url}]')
    repo: git.repo.base.Repo = git.Repo.clone_from(repo_url, repo_clone_path)
    shutil.move(f'{repo_clone_path}/{repo_jobs_dir}', options.directory)
    logger.info(f'Installed project[{options.project_name.value}] to directory[{options.directory}]')

def run_example_module(options: argparse.Namespace) -> None:
    validate_options(options)
    clone_repo(options)

def run_from_cli() -> None:
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    run_example_module(options)

if __name__ in ['__main__']:
    run_from_cli()

