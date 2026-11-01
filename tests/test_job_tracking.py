"""Job tracking hooks in app.main. Run: python -m unittest discover -s tests (from /app)."""
import asyncio
import contextlib
import io
import unittest
from unittest import mock

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import app.main as m


def cron(**kw) -> str:
    return m.cron_expr(CronTrigger(timezone='Asia/Dhaka', **kw))


class CronExprTest(unittest.TestCase):
    def test_conversions(self):
        self.assertEqual(cron(hour='*/1'), '0 */1 * * *')
        self.assertEqual(cron(minute='*/1'), '*/1 * * * *')
        self.assertEqual(cron(month=1, day=1, hour=0, minute=0, second=0), '0 0 1 1 *')
        self.assertEqual(cron(hour=2, minute=0), '0 2 * * *')
        self.assertEqual(cron(day='1-5', hour=0, minute=0), '0 0 1-5 * *')

    def test_day_of_week_numbers_become_names(self):
        # APScheduler 0 = Monday, cron 0 = Sunday: names avoid the off-by-one
        self.assertEqual(cron(day_of_week='0-4', hour=9), '0 9 * * mon-fri')
        self.assertEqual(cron(day_of_week='6', hour=9), '0 9 * * sun')
        self.assertEqual(cron(day_of_week='sat,sun', hour=9), '0 9 * * sat,sun')

    def test_env_config_round_trip(self):
        with mock.patch.dict('os.environ', {'CRON_X': 'hour=0,minute=0,second=0'}):
            self.assertEqual(m.cron_expr(CronTrigger(**m.parse_cron_config('CRON_X', ''), timezone='Asia/Dhaka')), '0 0 * * *')


class RegistryTest(unittest.TestCase):
    def test_publishes_every_cron_job_by_queue_name(self):
        async def run():
            s = AsyncIOScheduler(timezone='Asia/Dhaka')
            noop = mock.AsyncMock()
            s.add_job(noop, 'cron', minute='*/1', id='confirmProvisionalEmployees_job')
            s.add_job(noop, 'cron', hour=2, minute=0, id='absentBridgeMerge_job')
            s.add_job(noop, 'interval', seconds=60, id='crossWatch_job')
            s.start(paused=True)
            publish = mock.AsyncMock()
            with mock.patch.object(m, 'scheduler', s), mock.patch.object(m, 'publish_message', publish):
                await m.publish_registry()
            s.shutdown(wait=False)
            return publish

        publish = asyncio.run(run())
        queue, payload = publish.call_args.args
        self.assertEqual(queue, 'schedulerRegistry_trigger_queue')
        jobs = {j['job']: j for j in payload['jobs']}
        self.assertEqual(set(jobs), {'confirmProvisionalEmployees', 'processSandwichedHolidays'})
        self.assertEqual(jobs['confirmProvisionalEmployees']['cron'], '*/1 * * * *')
        self.assertEqual(jobs['processSandwichedHolidays']['timezone'], 'Asia/Dhaka')

    def test_publish_failure_does_not_raise(self):
        async def run():
            s = AsyncIOScheduler()
            with mock.patch.object(m, 'scheduler', s), \
                    mock.patch.object(m, 'publish_message', mock.AsyncMock(side_effect=ConnectionError('down'))), \
                    contextlib.redirect_stdout(io.StringIO()) as out:
                await m.publish_registry()
            return out.getvalue()

        self.assertIn('[registry] publish failed: down', asyncio.run(run()))


class CrossWatchTest(unittest.TestCase):
    def setUp(self):
        m.cross_watch_failures = 0
        m.cross_watch_alerting = False

    def tick(self, health=None, error=None) -> str:
        fetch = mock.Mock(side_effect=error) if error else mock.Mock(return_value=health)
        with mock.patch.object(m, '_fetch_health', fetch), contextlib.redirect_stdout(io.StringIO()) as out:
            asyncio.run(m.cross_watch())
        return out.getvalue()

    def test_alerts_once_after_two_misses_then_resolves(self):
        healthy = {'heartbeats': {'watchdog': 30}}
        self.assertEqual(self.tick(healthy), '')

        first = self.tick(error=OSError('Name or service not known'))
        self.assertIn('check failed (1x)', first)
        self.assertNotIn('ALERT', first)

        self.assertIn('[cross-watch] ALERT hr-bg-service not answering', self.tick(error=OSError('refused')))
        self.assertNotIn('ALERT', self.tick(error=OSError('refused')))  # still down: no repeat

        self.assertIn('[cross-watch] RESOLVED', self.tick(healthy))
        self.assertEqual(self.tick(healthy), '')

    def test_stale_or_missing_watchdog_heartbeat_counts_as_down(self):
        self.tick({'heartbeats': {'watchdog': 500}})
        self.assertIn('ALERT hr-bg watchdog heartbeat is 500s old', self.tick({'heartbeats': {'watchdog': 500}}))

        self.setUp()
        self.tick({'heartbeats': {}})
        self.assertIn('ALERT hr-bg reports no watchdog heartbeat at all', self.tick({'heartbeats': {}}))

    def test_one_miss_between_successes_never_alerts(self):
        healthy = {'heartbeats': {'watchdog': 30}}
        for _ in range(3):
            self.tick(error=OSError('blip'))
            self.assertEqual(self.tick(healthy), '')
        self.assertFalse(m.cross_watch_alerting)


if __name__ == '__main__':
    unittest.main()
