"""
Deploy script that applies prefect.yaml schedules after deployment.

Usage:
    python deploy.py                     # deploy all
    python deploy.py vol_table_flow      # deploy one by name
"""

import asyncio
import subprocess
import sys

from ruamel.yaml import YAML

from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.actions import DeploymentScheduleCreate


def load_deployments_from_yaml():
    yaml = YAML(typ='safe')
    with open('prefect.yaml') as f:
        config = yaml.load(f)
    return config.get('deployments', [])


def deploy_one(entrypoint: str):
    cmd = [
        'uvx', 'prefect-cloud', 'deploy',
        entrypoint,
        '--from', 'mh-guess/data-flow',
        '--with-requirements', 'requirements.txt',
    ]
    print(f'  $ {" ".join(cmd)}')
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f'  FAILED: {result.stderr.strip()}')
        return False
    print(f'  Deployed.')
    return True


async def apply_schedule(flow_name: str, dep_name: str, schedules: list):
    async with get_client() as client:
        slug = f'{flow_name}/{dep_name}'
        deployment = await client.read_deployment_by_name(slug)

        for s in deployment.schedules:
            await client.delete_deployment_schedule(deployment.id, s.id)

        if not schedules:
            print(f'  Schedule: none (on-demand)')
            return

        for s in schedules:
            cron = s['cron']
            tz = s.get('timezone', 'UTC')
            active = s.get('active', True)
            await client.create_deployment_schedules(
                deployment.id,
                [DeploymentScheduleCreate(
                    schedule=CronSchedule(cron=cron, timezone=tz),
                    active=active,
                )]
            )
            print(f'  Schedule: {cron} ({tz}), active={active}')


def main():
    filter_name = sys.argv[1] if len(sys.argv) > 1 else None
    deployments = load_deployments_from_yaml()

    for dep in deployments:
        name = dep['name']
        if filter_name and name != filter_name:
            continue

        entrypoint = dep['entrypoint']
        schedules = dep.get('schedules', [])
        flow_name = entrypoint.split(':')[1]

        print(f'\n[{name}]')
        if deploy_one(entrypoint):
            asyncio.run(apply_schedule(flow_name, name, schedules))

    print('\nDone.')


if __name__ == '__main__':
    main()
