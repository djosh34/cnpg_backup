"""Required operational evidence rejects missing measurements and false zeros."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from recovery_cases import Campaign

from campaign_fixture import resource_values
from campaign_process import CommandFailure

SAMPLE = '''rss_kib=30000
hwm_kib=40000
memory.current=80000000
memory.peak=100000000
memory.max=3221225472
oom=0
oom_kill=0
native_processes=0
blocks=2000000
available=1900000
free=1950000
block_size=4096
'''


class ResourceEvidence(unittest.TestCase):
    def test_kernel_process_cgroup_and_workspace_are_distinct(self):
        got = resource_values(SAMPLE)
        self.assertEqual(got['go_rss_bytes'], 30720000)
        self.assertEqual(got['go_hwm_bytes'], 40960000)
        self.assertEqual(got['cgroup_peak_bytes'], 100000000)
        self.assertEqual(got['workspace_used_bytes'], 204800000)
        self.assertEqual(got['workspace_capacity_bytes'], 8192000000)

    def test_actual_case_enforces_process_and_native_cgroup_independently(self):
        c = Campaign(None, None, SimpleNamespace(product_resources=Mock()))
        good = resource_values(SAMPLE)
        c.h.product_resources.return_value = good
        self.assertEqual(c.operational_sample('pod', idle=True), good)
        for field, value in [('go_rss_bytes', 128 << 20), ('go_hwm_bytes', 256 << 20),
                             ('cgroup_peak_bytes', (3 << 30) + 1), ('oom', 1),
                             ('native_processes', 2), ('workspace_capacity_bytes', 100 << 30)]:
            c.h.product_resources.return_value = dict(good, **{field: value})
            with self.subTest(field=field), self.assertRaises(AssertionError):
                c.operational_sample('pod', idle=True)

    def test_missing_unbounded_or_impossible_is_not_healthy_zero(self):
        for invalid in (SAMPLE.replace('rss_kib=30000\n', ''), SAMPLE.replace('3221225472', 'max'),
                        SAMPLE.replace('rss_kib=30000', 'rss_kib=0'),
                        SAMPLE.replace('free=1950000', 'free=2100000')):
            with self.subTest(invalid=invalid), self.assertRaises(CommandFailure):
                resource_values(invalid)


if __name__ == '__main__':
    unittest.main()
