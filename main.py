import re
import time
import json
import logging
import argparse
import datetime
import logging.config

import dateutil.parser
import googleapiclient.errors
import googleapiclient.discovery

log = logging.getLogger('snapshooter')
LOG_FORMAT = '[%(asctime)s] %(name)s:%(levelname)s: %(message)s'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', help='Your Google Cloud project ID.', required=True)
    parser.add_argument('--zone', help='Compute Engine zone to deploy to.', required=True)
    parser.add_argument('--async', action='store_true', default=False, dest='is_async',
                        help="Don't wait for operations to finish")
    parser.add_argument('--verbose', action='store_true', default=False,
                        help="Enable verbose logging")
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help="Just print what going to do")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    else:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'loggers': {
            'googleapiclient.discovery': {
                'level': 'WARN',
            },
            'googleapiclient.discovery_cache': {
                'level': 'ERROR',
            },
        }
    })

    snapshooter = Snapshooter(args.project, args.zone, args.is_async, args.dry_run)
    snapshooter.do_routine()


class Snapshooter:
    bias = datetime.timedelta(hours=1)
    min_age = datetime.timedelta(days=1)
    max_age = datetime.timedelta(days=30)
    description_prefix = '[auto] '

    def __init__(self, project, zone, is_async=False, dry_run=False):
        self.project = project
        self.zone = zone
        self.is_async = is_async
        self.dry_run = dry_run
        self.compute = googleapiclient.discovery.build('compute', 'v1')
        self.operations = []

    def do_routine(self):
        disks = self.compute.disks().list(project=self.project, zone=self.zone).execute().get('items', [])

        for disk in disks:
            try:
                self.handle_disk(disk)
            except Exception:
                log.exception('Failed to handle disk %s', disk['name'])

    def handle_disk(self, disk):
        log.info('Checking disk %s', disk['name'])

        if not self.is_snapshots_enabled(disk):
            return

        if not self.is_recent_snapshot_exists(disk):
            self.make_snapshot(disk)

        self.delete_obsolete_snapshots(disk)

    def is_snapshots_enabled(self, disk):
        if not disk['name'].startswith('gke-'):
            log.info('Skipping disk without "gke-" prefix %s', disk['name'])
            return False

        try:
            disk['_meta'] = json.loads(disk.get('description', ''))
        except ValueError:
            log.info('Skipping disk with non JSON description %s', disk['name'])
            return False

        if not disk['_meta'].get('kubernetes.io/created-for/pv/name'):
            log.info('Skipping disk without PV name in description %s', disk['name'])
            return False

        if not disk['_meta'].get('kubernetes.io/created-for/pvc/name'):
            log.info('Skipping disk without PVC name in description %s', disk['name'])
            return False

        if not disk['_meta'].get('kubernetes.io/created-for/pvc/namespace'):
            log.info('Skipping disk without PVC namespace in description %s', disk['name'])
            return False

        # TODO: get PVC using kubeapi, check annotations (like smp.io/backup=true/false). default to true
        return True

    def is_recent_snapshot_exists(self, disk):
        now = datetime_now()

        for snap in self.get_snapshots(disk):
            if now - snap['_ts'] <= self.min_age - self.bias:
                log.info('There is snapshot from %s already', snap['_ts'])
                return True

        return False

    def make_snapshot(self, disk):
        now = datetime_now()

        name = self.generate_snapshot_name(disk, now)
        description = self.generate_snapshot_description(disk, now)

        log.info('Creating snapshot %s for disk %s', name, disk['name'])

        if self.dry_run:
            return

        operation = self.compute.disks().createSnapshot(disk=disk['name'], project=self.project, zone=self.zone, body={
            'name': name,
            'description': description,
        }).execute()

        if not self.is_async:
            self._wait_for_operation(operation['name'])

        self.operations.append(operation)

    def generate_snapshot_name(self, disk, ts):
        basename = disk['_meta']['kubernetes.io/created-for/pv/name']
        ts = ts.replace(tzinfo=None).isoformat('T', 'seconds').replace(':', '-').replace('T', '--')
        return '{}--{}'.format(basename, ts)

    def generate_snapshot_description(self, disk, ts):
        return '{}{}'.format(self.description_prefix, disk['description'])

    def delete_obsolete_snapshots(self, disk):
        now = datetime_now()

        for snap in self.get_snapshots(disk, only_ours=True):
            if now - snap['_ts'] > self.max_age + self.bias:
                self.delete_snapshot(snap)

    def get_snapshots(self, disk, only_ours=False):
        disk_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}/disks/{}'.format(self.project,
                                                                                                self.zone,
                                                                                                disk['name'])

        f = 'sourceDisk eq {}'.format(disk_uri)
        if only_ours:
            f = '({}) (description eq {}.*)'.format(f, re.escape(self.description_prefix))

        # https://cloud.google.com/compute/docs/reference/beta/snapshots/list
        snaps = self.compute.snapshots().list(project=self.project, filter=f).execute().get('items', [])

        for snap in snaps:
            snap['_ts'] = dateutil.parser.parse(snap['creationTimestamp'])

        return snaps

    def delete_snapshot(self, snap):
        log.info('Deleting snapshot %s', snap['name'])

        if self.dry_run:
            return

        operation = self.compute.snapshots().delete(snapshot=snap['name'], project=self.project).execute()

        if not self.is_async:
            self._wait_for_operation(operation['name'])

        self.operations.append(operation)

    def _wait_for_operation(self, operation):
        log.debug('Waiting for operation to finish...')
        while True:
            try:
                result = self.compute.zoneOperations().get(project=self.project, zone=self.zone,
                                                           operation=operation).execute()
            except googleapiclient.errors.HttpError as e:
                # handle instant operations
                if e.resp.status == 404:
                    return None
                else:
                    raise e

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)


def datetime_now():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


if __name__ == '__main__':
    main()
