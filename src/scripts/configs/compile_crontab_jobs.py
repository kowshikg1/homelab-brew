from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Any

import click

from src.handlers.env_manager import EnvManager
from src.utils.file import load_yaml
from src.utils.path_variables import (
    ENV_FILE_GLOBAL,
    PATH_INGESTION_CONFIG,
    PATH_MONITOR_FOLDER,
)

env = EnvManager(ENV_FILE_GLOBAL)

DEFAULT_PROJECT_ROOT = env.get(
    'PROJECT_ROOT', Path(__file__).resolve().parents[3]
)
DEFAULT_CRONTAB_TASKS_FOLDER = Path('./configs/crontab_tasks')
DEFAULT_OUTPUT_FILE = DEFAULT_CRONTAB_TASKS_FOLDER / 'compiled.crontab'
DEFAULT_LOG_FOLDER = env.get('LOG_FOLDER', Path('./logs'))

"""
parameters for each job config:
- schedule (required): the cron schedule string (e.g. "0 0 * * *")
- command / commands / script (required): the command(s) to run, or a script to execute
- extra_command: if set, this command will be appended to the main command without any operator (useful for custom logging)
- operator: if using "commands", the operator to join them (default "&&")
- python: if true and using "script", run the script with Python (default:  true if script ends with .py)
- python_bin: the Python binary to use if running with Python (default: .venv/bin/python)
- use_lock: whether to wrap the command with flock for concurrency control (default: true for minute-level schedules <15min)
- logging
    - append_log: if true, append to the log file instead of overwriting (default: true )
    - log_file: if set, the file to log output to (default: logs/{handler/file_name}-cron.log)
- wrap_parentheses: if true, wrap the command in parentheses (default: false)
- wrap_braces: if true, wrap the command in braces (default: false)
"""


def _slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')


def _is_frequent_schedule(schedule: str) -> bool:
    """Treat minute-level schedules as frequent and protect them with flock by default."""
    schedule = schedule.strip()
    parts = schedule.split()
    if len(parts) < 5:
        return False

    minute_field = parts[0]
    if minute_field == '*':
        return True
    if minute_field.startswith('*/'):
        try:
            return int(minute_field[2:]) <= 15
        except ValueError:
            return True
    return ',' in minute_field or '-' in minute_field


def _with_logging(
    command: str, job_name: str, job_config: dict[str, Any]
) -> str:
    log_file = job_config.get('log_file')
    append_log = bool(job_config.get('append_log', bool(log_file)))
    if not append_log:
        return command

    if not log_file.endswith('.log'):
        log_file = DEFAULT_LOG_FOLDER / f'{_slug(log_file)}-cron.log'
    else:
        log_file = DEFAULT_LOG_FOLDER / log_file
    return f'{command} >> {shlex.quote(str(log_file))} 2>&1'


def _with_lock(command: str, job_name: str, job_config: dict[str, Any]) -> str:
    use_lock = job_config.get('use_lock')
    if use_lock is None:
        use_lock = _is_frequent_schedule(
            str(job_config.get('schedule', '')).strip()
        )

    if not use_lock:
        return command

    lock_file = job_config.get('lock_file') or f'/tmp/{_slug(job_name)}.lock'
    return f'/usr/bin/flock -n {shlex.quote(str(lock_file))} {command}'


def _build_ingestion_command(job_name: str, job_config: dict[str, Any]) -> str:
    python_bin = str(job_config.get('python_bin', '.venv/bin/python'))
    runner_module = 'src.ingestion.base_ingestion'
    command = f'{python_bin} -m {runner_module} {job_name}'
    command = _with_lock(command, job_name, job_config)
    command = _with_logging(command, job_name, job_config)
    return command


def _build_general_command(job_name: str, job_config: dict[str, Any]) -> str:
    command: str | None = job_config.get('command')

    if not command:
        commands = job_config.get('commands')
        if isinstance(commands, list) and commands:
            operator = str(job_config.get('operator', '&&')).strip() or '&&'
            command = f' {operator} '.join(
                str(item).strip() for item in commands if str(item).strip()
            )

    if not command:
        script = job_config.get('script')
        if script:
            script = str(script)
            args = [
                shlex.quote(token)
                for arg in job_config.get('args', [])
                for token in shlex.split(str(arg))
            ]
            run_with_python = bool(
                job_config.get('python', script.endswith('.py'))
            )
            if run_with_python:
                python_bin = str(
                    job_config.get('python_bin', '.venv/bin/python')
                )
                command = f'{python_bin} {shlex.quote(script)}'
            else:
                command = script

            if args:
                command = f'{command} {" ".join(args)}'

    if not command:
        raise ValueError(
            f"General cron job '{job_name}' requires one of: command, commands, or script"
        )

    if job_config.get('wrap_parentheses'):
        command = f'( {command} )'
    if job_config.get('wrap_braces'):
        command = f'{{ {command}; }}'
    if extra_command := job_config.get('extra_command'):
        command = f'{command} {extra_command}'

    command = _with_lock(command, job_name, job_config)
    command = _with_logging(command, job_name, job_config)
    return command


def _build_monitor_command(job_name: str, job_config: dict[str, Any]) -> str:
    python_bin = str(job_config.get('python_bin', '.venv/bin/python'))
    runner_module = 'src.monitor.base_monitor'
    command = f'{python_bin} -m {runner_module} {job_name}'
    command = _with_lock(command, job_name, job_config)
    command = _with_logging(command, job_name, job_config)
    return command


def _iter_yaml_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(folder.rglob('*.yml'))


def compile_crontab_jobs(
    compiled_ingestion_file: Path = PATH_INGESTION_CONFIG,
    general_jobs_folder: Path = DEFAULT_CRONTAB_TASKS_FOLDER,
    output_file: Path = DEFAULT_OUTPUT_FILE,
    project_root: Path = DEFAULT_PROJECT_ROOT,
) -> list[str]:
    lines: list[str] = []
    normalized_project_root = shlex.quote(str(project_root.resolve()))

    # Process ingestion jobs
    for job_name, job_config in load_yaml(compiled_ingestion_file).items():
        if not isinstance(job_config, dict):
            continue
        if str(job_name).startswith('base_'):
            continue
        if not job_config.get('is_active', True):
            continue

        schedule = str(job_config.get('schedule', '')).strip()
        if not schedule:
            continue
        logging = job_config.get('logging', {})
        job_config['log_file'] = logging.get('log_file', job_config['handler'])
        job_config['append_log'] = logging.get(
            'append_log', bool(job_config.get('log_file'))
        )
        command = _build_ingestion_command(str(job_name), job_config)
        lines.append(f'{schedule} cd {normalized_project_root} && {command}')

    # Process general jobs
    for config_file in _iter_yaml_files(Path(general_jobs_folder)):
        if (
            config_file.resolve() == Path(output_file).resolve()
            or config_file.stem == 'example'
        ):
            continue

        config_data = load_yaml(config_file) or {}
        for job_name, job_config in config_data.items():
            if not isinstance(job_config, dict):
                continue
            if not job_config.get('is_active', True):
                continue

            schedule = str(job_config.get('schedule', '')).strip()
            if not schedule:
                continue
            logging = job_config.get('logging', {})
            job_config['log_file'] = logging.get('log_file', config_file.stem)
            job_config['append_log'] = logging.get(
                'append_log', bool(job_config.get('log_file'))
            )
            command = _build_general_command(str(job_name), job_config)
            lines.append(
                f'{schedule} cd {job_config.get("repo") or normalized_project_root} && {command}'
            )

    # Process monitor jobs
    for config_file in _iter_yaml_files(PATH_MONITOR_FOLDER):
        if (
            config_file.resolve() == Path(output_file).resolve()
            or config_file.stem == 'example'
        ):
            continue

        config_data = load_yaml(config_file) or {}
        for job_name, job_config in config_data.items():
            if not isinstance(job_config, dict):
                continue
            if not job_config.get('is_active', True):
                continue

            schedule = str(job_config.get('schedule', '')).strip()
            if not schedule:
                continue
            logging = job_config.get('logging', {})
            job_config['log_file'] = logging.get('log_file', config_file.stem)
            job_config['append_log'] = logging.get(
                'append_log', bool(job_config.get('log_file'))
            )
            command = _build_monitor_command(str(job_name), job_config)
            lines.append(
                f'{schedule} cd {job_config.get("repo") or normalized_project_root} && {command}'
            )

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        '\n'.join(lines) + ('\n' if lines else ''), encoding='utf-8'
    )
    return lines


@click.command()
@click.option(
    '--ingestion-config',
    default=str(PATH_INGESTION_CONFIG),
    help='Path to the compiled ingestion YAML file',
)
@click.option(
    '--general-jobs-folder',
    default=str(DEFAULT_CRONTAB_TASKS_FOLDER),
    help='Path to the folder containing general cron job YAML files',
)
@click.option(
    '--output',
    default=str(DEFAULT_OUTPUT_FILE),
    help='Path to the output crontab file',
)
@click.option(
    '--project-root',
    default=str(DEFAULT_PROJECT_ROOT),
    help='Path to the project root',
)
def run(ingestion_config, general_jobs_folder, output, project_root) -> None:
    # parser = argparse.ArgumentParser(
    #     description='Compile YAML cron jobs to crontab command lines'
    # )
    # parser.add_argument(
    #     '--ingestion-config', default=str(PATH_INGESTION_CONFIG)
    # )
    # parser.add_argument(
    #     '--general-jobs-folder', default=str(DEFAULT_CRONTAB_TASKS_FOLDER)
    # )
    # parser.add_argument('--output', default=str(DEFAULT_OUTPUT_FILE))
    # parser.add_argument('--project-root', default=str(DEFAULT_PROJECT_ROOT))
    # args = parser.parse_args()

    lines = compile_crontab_jobs(
        compiled_ingestion_file=Path(ingestion_config),
        general_jobs_folder=Path(general_jobs_folder),
        output_file=Path(output),
        project_root=Path(project_root),
    )
    if lines:
        # pass
        print('\n'.join(lines))


if __name__ == '__main__':
    run()
