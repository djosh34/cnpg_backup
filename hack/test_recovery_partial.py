"""Local distinguishing tests for the actual campaign's promotion byte oracle."""
import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from recovery_cases import Campaign


class PromotionArchiveTests(unittest.TestCase):
    def check(self, fault=None, compression='gzip'):
        size = 1 << 20
        old = '000000010000000000000001'
        partial, full = old + '.partial', '000000020000000000000001'
        raw = {partial: b'p' * size, full: b'f' * size}
        expected = hashlib.sha256(raw[partial]).hexdigest()
        local_full = hashlib.sha256(raw[full]).hexdigest()
        prefix = 'smoke/v1/new-repository/wal/'
        keys = [prefix + n[:8] + '/' + n for n in raw]
        state = {'name': 'fixture', 'repository_id': 'new-repository', 'plan': {
            'plan': {'target': {'kind': 'immediate'}, 'required_archive': [],
                     'source': {'system_identifier': '12345', 'wal_segment_bytes': size},
                     'chain': [{'timeline': 1, 'bundled_wal_end_lsn': '0/100100'}]},
            'bundled': {old: {'Size': size, 'SHA256': expected}}}}
        if fault == 'missing-partial':
            keys.remove(prefix + partial[:8] + '/' + partial)
        if fault == 'manufactured-full':
            keys.append(prefix + old[:8] + '/' + old)
        if fault == 'changed-partial':
            raw[partial] = b'x' * size
        if fault == 'changed-full':
            raw[full] = b'x' * size
        if fault == 'short-partial':
            raw[partial] = raw[partial][:-1]
        if fault == 'wrong-full-timeline':
            full = old
        if fault == 'changed-frontier':
            state['plan']['plan']['required_archive'] = [{'timeline': 1}]
        original_plan = copy.deepcopy(state['plan'])
        def download(*args):
            key = args[-1].split('/test-bucket/')[1]
            self.assertIn(key, keys)
            name = key.rsplit('/', 1)[1]
            body = gzip.compress(raw[name]) if compression == 'gzip' else raw[name]
            metadata = {'cnpg-format': 'wal-v1', 'cnpg-system-id': '12345',
                        'cnpg-stored-sha256': hashlib.sha256(body).hexdigest(),
                        'cnpg-compression': compression, 'cnpg-raw-bytes': str(len(raw[name])),
                        'cnpg-raw-sha256': hashlib.sha256(raw[name]).hexdigest()}
            if fault == 'corrupt-stored':
                body = body[:-1]
            if fault == 'wrong-system':
                metadata['cnpg-system-id'] = '54321'
            Path(args[args.index('--output') + 1]).write_bytes(body)
            Path(args[args.index('--dump-header') + 1]).write_text(
                'HTTP/1.1 200 OK\n' + '\n'.join('X-Amz-Meta-' + k + ': ' + v for k, v in metadata.items()))
        def kube(*args):
            if 'sha256sum' in args:
                return local_full + '  full\n'
            self.assertIn('test', args)
            self.assertTrue(args[-1].endswith(partial + '.done'))
            if fault == 'no-done':
                raise AssertionError('PostgreSQL has not acknowledged partial')
            return ''
        with tempfile.TemporaryDirectory() as tmp:
            campaign = Campaign(SimpleNamespace(), Mock())
            campaign.wal = SimpleNamespace(endpoint='https://localhost:19000', directory=Path(tmp))
            campaign.inventory = Mock(return_value=keys)
            campaign.event = Mock()
            with patch('recovery_cases.h.WORK', Path(tmp)), patch('recovery_cases.h.run', side_effect=download), \
                 patch('recovery_cases.h.kube', side_effect=kube):
                campaign.verify_promotion_archive(state, 'primary', full)
            self.assertEqual(state['plan'], original_plan, 'byte oracle changed selected source plan')
            event = campaign.event.call_args
            self.assertEqual(event.args[0], 'promotion-auxiliary-and-new-full-durable')
            self.assertEqual(len(event.kwargs['objects']), 2)
            self.assertEqual(event.kwargs['objects'][0]['raw_sha256'], expected)

    def test_correct_fresh_bytes_both_compressions(self):
        for compression in ('none', 'gzip'):
            with self.subTest(compression=compression):
                self.check(compression=compression)

    def test_oracle_rejects_missing_corrupt_substituted_or_unacknowledged_archive(self):
        for fault in ('missing-partial', 'manufactured-full', 'changed-partial', 'changed-full',
                      'short-partial', 'wrong-full-timeline', 'changed-frontier',
                      'corrupt-stored', 'wrong-system', 'no-done'):
            with self.subTest(fault=fault):
                with self.assertRaises(AssertionError):
                    self.check(fault)


if __name__ == '__main__':
    unittest.main()
