import time
import json
import logging
import argparse
import datetime
import logging.config

import dateutil.parser
import googleapiclient.errors
import googleapiclient.discovery

log = logging.getLogger(__name__)
LOG_FORMAT = '[%(asctime)s] %(name)s:%(levelname)s: %(message)s'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', help='Your Google Cloud project ID.', required=True)
    parser.add_argument('--zone', help='Compute Engine zone to deploy to.', required=True)
    parser.add_argument('--async', action='store_true', default=False,
                        help="Don't wait for operations to finish")
    parser.add_argument('--verbose', action='store_true', default=False,
                        help="Enable verbose logging")
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
            }
        }
    })

    snapshooter = Snapshooter(args.project, args.zone, args.async)
    snapshooter.do_routine()


class Snapshooter:
    min_age = datetime.timedelta(days=1)
    max_age = datetime.timedelta(days=7)

    def __init__(self, project, zone, async=False):
        self.project = project
        self.zone = zone
        self.async = async
        self.compute = googleapiclient.discovery.build('compute', 'v1')
        self.operations = []

    def do_routine(self):
        disks = self.compute.disks().list(project=self.project, zone=self.zone).execute().get('items', [])

        for disk in disks:
            try:
                self.handle_disk(disk)
            except Exception as e:
                log.exception('Failed to handle disk %s', disk['name'])

    def handle_disk(self, disk):
        log.info('Checking disk %s', disk['name'])

        if 'description' not in disk:
            log.info('Skipping disk without description %s', disk['name'])
            return

        try:
            disk['_meta'] = json.loads(disk.get('description'))
        except ValueError:
            log.info('Skipping disk with non JSON description %s', disk['name'])
            return

        if 'kubernetes.io/created-for/pv/name' not in disk['_meta']:
            log.info('Skipping disk without PV name in description %s', disk['name'])
            return

        if 'kubernetes.io/created-for/pvc/name' not in disk['_meta']:
            log.info('Skipping disk without PVC name in description %s', disk['name'])
            return

        if 'kubernetes.io/created-for/pvc/namespace' not in disk['_meta']:
            log.info('Skipping disk without PVC namespace in description %s', disk['name'])
            return

        disk['_meta']['_pvc'] = '{}--{}'.format(disk['_meta']['kubernetes.io/created-for/pvc/namespace'],
                                                disk['_meta']['kubernetes.io/created-for/pvc/name'])

        if not self.is_snapshots_enabled(disk):
            log.info('Skipping')
            return

        if self.is_recent_snapshot_exists(disk):
            log.info('Skipping')
            return

        self.make_snapshot(disk)
        self.delete_obsolete_snapshots(disk)

    def is_snapshots_enabled(self, disk):
        # TODO: get PVC using kubeapi, check annotations (like smp.io/backup=true/false). default to true
        return disk['name'].startswith('gke-')

    def is_recent_snapshot_exists(self, disk):
        now = datetime_now()

        for snap in self.get_snapshots(disk):
            if now - snap['_ts'] <= self.min_age:
                log.info('There is snapshot from %s already', snap['_ts'])
                return True

        return False

    def make_snapshot(self, disk):
        now = datetime_now()

        name = self.generate_snapshot_name(disk, now)
        description = self.generate_snapshot_description(disk, now)

        log.info('Creating snapshot %s for disk %s', name, disk['name'])

        operation = self.compute.disks().createSnapshot(disk=disk['name'], project=self.project, zone=self.zone, body={
            'name': name,
            'description': description,
        }).execute()

        if not self.async:
            self._wait_for_operation(operation['name'])

        self.operations.append(operation)

    def generate_snapshot_name(self, disk, ts):
        basename = disk['_meta']['kubernetes.io/created-for/pv/name']
        ts = ts.replace(tzinfo=None).isoformat('T', 'seconds').replace(':', '-').replace('T', '--')
        return '{}--{}'.format(basename, ts)

    def generate_snapshot_description(self, disk, ts):
        return disk['description']

    def delete_obsolete_snapshots(self, disk):
        now = datetime_now()

        for snap in self.get_snapshots(disk):
            if now - snap['_ts'] > self.max_age:
                self.delete_snapshot(snap)

    def get_snapshots(self, disk):
        disk_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}/disks/{}'.format(self.project,
                                                                                                self.zone,
                                                                                                disk['name'])
        f = 'sourceDisk eq {}'.format(disk_uri)
        snaps = self.compute.snapshots().list(project=self.project, filter=f).execute().get('items', [])

        for snap in snaps:
            snap['_ts'] = dateutil.parser.parse(snap['creationTimestamp'])

        return snaps

    def delete_snapshot(self, snap):
        log.info('Deleting snapshot %s', snap['name'])

        operation = self.compute.snapshots().delete(snapshot=snap['name'], project=self.project).execute()

        if not self.async:
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
